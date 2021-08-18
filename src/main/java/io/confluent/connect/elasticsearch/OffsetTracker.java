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

  private final Map<TopicPartition, Queue<Offset>> offsetsByPartition = new ConcurrentHashMap<>();
  private final Map<TopicPartition, Long> maxByPartition = new ConcurrentHashMap<>();

  public Offset addPendingRecord(SinkRecord sinkRecord) {
    TopicPartition tp = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
    Offset offset = new Offset(sinkRecord.kafkaOffset());
    Long partitionMax = maxByPartition.get(tp);
    if (partitionMax == null || sinkRecord.kafkaOffset() > partitionMax) {
      offsetsByPartition
              .computeIfAbsent(tp, key -> new ArrayDeque<>()) // TODO concurrency
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
        result.put(topicPartition, new OffsetAndMetadata(max));
        maxByPartition.put(topicPartition, max);
      }
    }));
    return result;
  }

}
