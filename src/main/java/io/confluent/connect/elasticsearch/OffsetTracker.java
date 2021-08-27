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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toMap;

class OffsetTracker {

  private static final Logger log = LoggerFactory.getLogger(OffsetTracker.class);

  private final Map<TopicPartition, Map<Long, OffsetState>> offsetsByPartition
          = new ConcurrentHashMap<>();

  private final Map<TopicPartition, Long> maxOffsetByPartition
          = new ConcurrentHashMap<>();

  private final AtomicLong numEntries = new AtomicLong();


  // no fairness, things can only be blocked from one thread, the one making the put call
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition notFull = lock.newCondition();
  private final long maxNumEntries;

  static class OffsetState {

    private static final Supplier<Boolean> TRUE = () -> true;
    private final long offset;
    private final AtomicReference<Supplier<Boolean>> processed = new AtomicReference<>();

    OffsetState(long offset) {
      this.offset = offset;
    }

    /**
     * Marks the offset as processed (ready to report to preCommit), if it has not been marked yet
     */
    public void markProcessed() {
      markProcessed(TRUE);
    }

    /**
     * Provides a supplier that will determine if the record was processed or not.
     * It does nothing is markProcessed was called before.
     * Intended for async operations that return a Future and don't provide a callback we can
     * hook to. (e.g. {@link ErrantRecordReporter#report(SinkRecord, Throwable)})
     */
    public void markProcessed(Supplier<Boolean> processedSupplier) {
      processed.compareAndSet(null, processedSupplier);
    }

    public boolean isProcessed() {
      Supplier<Boolean> booleanSupplier = processed.get();
      return booleanSupplier != null && Boolean.TRUE.equals(booleanSupplier.get());
    }
  }

  OffsetTracker(int maxNumEntries) {
    this.maxNumEntries = maxNumEntries;
  }

  /**
   * Partitions are no longer owned, we should release all related resources.
   */
  public void closePartitions(Collection<TopicPartition> topicPartitions) {
    lock.lock();
    try {
      topicPartitions.forEach(tp -> {
        Map<Long, OffsetState> offsets = offsetsByPartition.remove(tp);
        if (offsets != null) {
          numEntries.getAndAdd(offsets.size());
        }
        maxOffsetByPartition.remove(tp);
      });
    } finally {
      lock.unlock();
    }
  }

  /**
   * This method assumes that new records are added in offset order.
   * Older records can be re-added, and the same Offset object will be return if its
   * offset hasn't been reported yet.
   */
  public OffsetState addPendingRecord(SinkRecord sinkRecord) {
    lock.lock();
    try {
      applyBackpressureIfNecessary();

      numEntries.incrementAndGet();
      TopicPartition tp = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
      Long partitionMax = maxOffsetByPartition.get(tp);
      if (partitionMax == null || sinkRecord.kafkaOffset() > partitionMax) {
        return offsetsByPartition
                .computeIfAbsent(tp, key -> new LinkedHashMap<>())
                .computeIfAbsent(sinkRecord.kafkaOffset(), OffsetState::new);
      }
      return new OffsetState(sinkRecord.kafkaOffset());
    } finally {
      lock.unlock();
    }
  }

  private void applyBackpressureIfNecessary() {
    if (numEntries.get() >= maxNumEntries) {
      log.debug("Maximum number of offset tracking entries reached {}, blocking", maxNumEntries);
      try {
        notFull.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while waiting for offset tracking entries "
                + "to decrease", e);
      }
    }
  }

  public Map<TopicPartition, OffsetAndMetadata> getOffsets() {
    return maxOffsetByPartition.entrySet().stream()
            .collect(toMap(
                    Map.Entry::getKey,
                    e -> new OffsetAndMetadata(e.getValue() + 1)));
  }

  public void moveOffsets() {
    lock.lock();
    try {
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

      if (numEntries.get() < maxNumEntries) {
        notFull.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }

}
