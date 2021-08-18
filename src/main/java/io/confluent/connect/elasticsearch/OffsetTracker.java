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

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

class OffsetTracker {

  static class Offset {
    private final long offset;
    private final AtomicBoolean processed = new AtomicBoolean();

    Offset(long offset) {
      this.offset = offset;
    }

    public void markProcessed() {
      processed.set(true);
    }
  }

  // TODO limit size of queue
  private final Map<TopicPartition, Queue<Offset>> offsetsByPartition = new ConcurrentHashMap<>();
  private final Map<TopicPartition, Long> maxByPartition = new ConcurrentHashMap<>();

  public Offset addPendingRecord(SinkRecord sinkRecord) {
    TopicPartition tp = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
    Offset offset = new Offset(sinkRecord.kafkaOffset());
    Long partitionMax = maxByPartition.get(tp);
    if (partitionMax == null || sinkRecord.kafkaOffset() > partitionMax) {
      offsetsByPartition
              .computeIfAbsent(tp, key -> new ArrayDeque<>())
              .add(offset);
    }
    return offset;
  }

  public Map<TopicPartition, OffsetAndMetadata> getAndResetOffsets() {
    Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();

    offsetsByPartition.forEach(((topicPartition, offsets) -> {
      Long max = maxByPartition.get(topicPartition);
      boolean newMaxFound = false;
      Iterator<Offset> iterator = offsets.iterator();
      while (iterator.hasNext()) {
        Offset offset = iterator.next();
        if (offset.processed.get()) {
          iterator.remove();
          if (max == null || offset.offset > max) {
            max = offset.offset;
            newMaxFound = true;
          }
        } else {
          break;
        }
      }
      if (newMaxFound) {
        result.put(topicPartition, new OffsetAndMetadata(max + 1));
        maxByPartition.put(topicPartition, max);
      }
    }));
    return result;
  }

}
