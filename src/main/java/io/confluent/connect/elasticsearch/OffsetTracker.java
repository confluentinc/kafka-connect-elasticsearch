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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

class OffsetTracker {

  static class OffsetState {
    private final long offset;
    private final AtomicBoolean processed = new AtomicBoolean();

    OffsetState(long offset) {
      this.offset = offset;
    }

    public void markProcessed() {
      processed.set(true);
    }
  }

  // TODO limit size of queue
  private final Map<TopicPartition, Map<Long, OffsetState>> offsetsByPartition
          = new ConcurrentHashMap<>();

  private final Map<TopicPartition, Long> maxReportedByPartition
          = new ConcurrentHashMap<>();

  /**
   * This method assumes that new records are added in offset order.
   * Older records can be re-added, and the same Offset object will be return if its
   * offset hasn't been reported yet.
   */
  public OffsetState addPendingRecord(SinkRecord sinkRecord) {
    TopicPartition tp = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
    Long partitionMax = maxReportedByPartition.get(tp);
    if (partitionMax == null || sinkRecord.kafkaOffset() > partitionMax) {
      return offsetsByPartition
              .computeIfAbsent(tp, key -> new LinkedHashMap<>())
              .computeIfAbsent(sinkRecord.kafkaOffset(), OffsetState::new);
    }
    return new OffsetState(sinkRecord.kafkaOffset());
  }

  public Map<TopicPartition, OffsetAndMetadata> getAndResetOffsets() {
    Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();

    offsetsByPartition.forEach(((topicPartition, offsets) -> {
      Long max = maxReportedByPartition.get(topicPartition);
      boolean newMaxFound = false;
      Iterator<OffsetState> iterator = offsets.values().iterator();
      while (iterator.hasNext()) {
        OffsetState offsetState = iterator.next();
        if (offsetState.processed.get()) {
          iterator.remove();
          if (max == null || offsetState.offset > max) {
            max = offsetState.offset;
            newMaxFound = true;
          }
        } else {
          break;
        }
      }
      if (newMaxFound) {
        result.put(topicPartition, new OffsetAndMetadata(max + 1));
        maxReportedByPartition.put(topicPartition, max);
      }
    }));
    return result;
  }

}
