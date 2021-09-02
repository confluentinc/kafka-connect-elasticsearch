/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.elasticsearch;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.stream.Collectors.toMap;

/**
 * Tracks processed records to calculate safe offsets to commit.
 * <br>
 * Since ElasticsearchClient can potentially process multiple batches asynchronously for the same
 * partition, if we don't want to wait for all in-flight batches at the end of the put call
 * (or flush/preCommit) we need to keep track of what's the highest offset that is safe to commit.
 * For now, we do that at the individual record level because batching is handled by BulkProcessor,
 * and we don't have control over grouping/ordering.
 */
class OffsetTracker {

  private static final Logger log = LoggerFactory.getLogger(OffsetTracker.class);

  private final Map<TopicPartition, Map<Long, OffsetState>> offsetsByPartition = new HashMap<>();
  private final Map<TopicPartition, Long> maxOffsetByPartition = new HashMap<>();

  private final AtomicLong numEntries = new AtomicLong();

  static class OffsetState {

    private final long offset;
    private volatile boolean processed;

    OffsetState(long offset) {
      this.offset = offset;
    }

    /**
     * Marks the offset as processed (ready to report to preCommit)
     */
    public void markProcessed() {
      processed = true;
    }

    public boolean isProcessed() {
      return processed;
    }
  }

  /**
   * Partitions are no longer owned, we should release all related resources.
   */
  public synchronized void closePartitions(Collection<TopicPartition> topicPartitions) {
    topicPartitions.forEach(tp -> {
      Map<Long, OffsetState> offsets = offsetsByPartition.remove(tp);
      if (offsets != null) {
        numEntries.getAndAdd(offsets.size());
      }
      maxOffsetByPartition.remove(tp);
    });
  }

  /**
   * This method assumes that new records are added in offset order.
   * Older records can be re-added, and the same Offset object will be return if its
   * offset hasn't been reported yet.
   */
  public synchronized OffsetState addPendingRecord(SinkRecord sinkRecord) {
    log.trace("Adding pending record");
    TopicPartition tp = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
    Long partitionMax = maxOffsetByPartition.get(tp);
    if (partitionMax == null || sinkRecord.kafkaOffset() > partitionMax) {
      numEntries.incrementAndGet();
      return offsetsByPartition
              // Insertion order needs to be maintained
              .computeIfAbsent(tp, key -> new LinkedHashMap<>())
              .computeIfAbsent(sinkRecord.kafkaOffset(), OffsetState::new);
    } else {
      return new OffsetState(sinkRecord.kafkaOffset());
    }
  }

  /**
   * @return overall number of entries
   */
  public long numEntries() {
    return numEntries.get();
  }

  /**
   * Move offsets to the highest we can.
   */
  public synchronized void updateOffsets() {
    log.trace("Updating offsets");
    offsetsByPartition.forEach(((topicPartition, offsets) -> {
      Long max = maxOffsetByPartition.get(topicPartition);
      boolean newMaxFound = false;
      Iterator<OffsetState> iterator = offsets.values().iterator();
      while (iterator.hasNext()) {
        OffsetState offsetState = iterator.next();
        if (offsetState.isProcessed()) {
          iterator.remove();
          numEntries.decrementAndGet();
          if (max == null || offsetState.offset > max) {
            max = offsetState.offset;
            newMaxFound = true;
          }
        } else {
          break;
        }
      }
      if (newMaxFound) {
        maxOffsetByPartition.put(topicPartition, max);
      }
    }));
    log.trace("Updated offsets, num entries: {}", numEntries);
  }

  /**
   * @return offsets to commit
   */
  public synchronized Map<TopicPartition, OffsetAndMetadata> offsets() {
    return maxOffsetByPartition.entrySet().stream()
            .collect(toMap(
                Map.Entry::getKey,
                e -> new OffsetAndMetadata(e.getValue() + 1)));
  }

}
