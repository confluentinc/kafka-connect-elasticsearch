package io.confluent.connect.elasticsearch;

import io.confluent.connect.elasticsearch.OffsetTracker.Offset;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class OffsetTrackerTest {

  @Test
  public void testHappyPath() {
    OffsetTracker offsetTracker = new OffsetTracker();

    TopicPartition tp = new TopicPartition("t1", 0);

    SinkRecord record1 = sinkRecord(tp, 0);
    SinkRecord record2 = sinkRecord(tp, 1);
    SinkRecord record3 = sinkRecord(tp, 2);

    Offset offset1 = offsetTracker.addPendingRecord(record1);
    Offset offset2 = offsetTracker.addPendingRecord(record2);
    Offset offset3 = offsetTracker.addPendingRecord(record3);

    assertThat(offsetTracker.getAndResetOffsets()).isEmpty();

    offset2.markProcessed();
    assertThat(offsetTracker.getAndResetOffsets()).isEmpty();

    offset1.markProcessed();
    Map<TopicPartition, OffsetAndMetadata> offsetMap = offsetTracker.getAndResetOffsets();
    assertThat(offsetMap).hasSize(1);
    assertThat(offsetMap.get(tp).offset()).isEqualTo(1);

    offset3.markProcessed();
    offsetMap = offsetTracker.getAndResetOffsets();
    assertThat(offsetMap).hasSize(1);
    assertThat(offsetMap.get(tp).offset()).isEqualTo(2);

    assertThat(offsetTracker.getAndResetOffsets()).isEmpty();
  }

  /**
   * Verify that if we receive records that are below the already committed offset for partition
   * (e.g. after a RetriableException), the offset reporting is not affected.
   */
  @Test
  public void testBelowWatermark() {
    OffsetTracker offsetTracker = new OffsetTracker();

    TopicPartition tp = new TopicPartition("t1", 0);

    SinkRecord record1 = sinkRecord(tp, 0);
    SinkRecord record2 = sinkRecord(tp, 1);

    Offset offset1 = offsetTracker.addPendingRecord(record1);
    Offset offset2 = offsetTracker.addPendingRecord(record2);

    offset1.markProcessed();
    offset2.markProcessed();
    assertThat(offsetTracker.getAndResetOffsets().get(tp).offset()).isEqualTo(1);

    offset2 = offsetTracker.addPendingRecord(record2);
    assertThat(offsetTracker.getAndResetOffsets()).isEmpty();

    offset2.markProcessed();
    assertThat(offsetTracker.getAndResetOffsets()).isEmpty();
  }

  private SinkRecord sinkRecord(TopicPartition tp, long offset) {
    return sinkRecord(tp.topic(), tp.partition(), offset);
  }

  private SinkRecord sinkRecord(String topic, int partition, long offset) {
    return new SinkRecord(topic,
            partition,
            null,
            null,
            null,
            null,
            offset);
  }
}
