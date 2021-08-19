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

    OffsetTracker.OffsetState offsetState1 = offsetTracker.addPendingRecord(record1);
    OffsetTracker.OffsetState offsetState2 = offsetTracker.addPendingRecord(record2);
    OffsetTracker.OffsetState offsetState3 = offsetTracker.addPendingRecord(record3);

    assertThat(offsetTracker.getAndResetOffsets()).isEmpty();

    offsetState2.markProcessed();
    assertThat(offsetTracker.getAndResetOffsets()).isEmpty();

    offsetState1.markProcessed();
    Map<TopicPartition, OffsetAndMetadata> offsetMap = offsetTracker.getAndResetOffsets();
    assertThat(offsetMap).hasSize(1);
    assertThat(offsetMap.get(tp).offset()).isEqualTo(2);

    offsetState3.markProcessed();
    offsetMap = offsetTracker.getAndResetOffsets();
    assertThat(offsetMap).hasSize(1);
    assertThat(offsetMap.get(tp).offset()).isEqualTo(3);

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

    OffsetTracker.OffsetState offsetState1 = offsetTracker.addPendingRecord(record1);
    OffsetTracker.OffsetState offsetState2 = offsetTracker.addPendingRecord(record2);

    offsetState1.markProcessed();
    offsetState2.markProcessed();
    assertThat(offsetTracker.getAndResetOffsets().get(tp).offset()).isEqualTo(2);

    offsetState2 = offsetTracker.addPendingRecord(record2);
    assertThat(offsetTracker.getAndResetOffsets()).isEmpty();

    offsetState2.markProcessed();
    assertThat(offsetTracker.getAndResetOffsets()).isEmpty();
  }

  @Test
  public void testBatchRetry() {
    OffsetTracker offsetTracker = new OffsetTracker();

    TopicPartition tp = new TopicPartition("t1", 0);

    SinkRecord record1 = sinkRecord(tp, 0);
    SinkRecord record2 = sinkRecord(tp, 1);

    OffsetTracker.OffsetState offsetState1A = offsetTracker.addPendingRecord(record1);
    OffsetTracker.OffsetState offsetState2A = offsetTracker.addPendingRecord(record2);

    // first fails but second succeeds
    offsetState2A.markProcessed();
    assertThat(offsetTracker.getAndResetOffsets()).isEmpty();

    // now the batch is retried by the framework (e.g. RetriableException)
    OffsetTracker.OffsetState offsetState1B = offsetTracker.addPendingRecord(record1);
    OffsetTracker.OffsetState offsetState2B = offsetTracker.addPendingRecord(record2);

    offsetState2B.markProcessed();
    offsetState1B.markProcessed();
    assertThat(offsetTracker.getAndResetOffsets().get(tp).offset()).isEqualTo(2);
  }

  private SinkRecord sinkRecord(TopicPartition tp, long offset) {
    return sinkRecord(tp.topic(), tp.partition(), offset);
  }

  private SinkRecord sinkRecord(String topic, int partition, long offset) {
    return new SinkRecord(topic,
            partition,
            null,
            "testKey",
            null,
            "testValue" + offset,
            offset);
  }
}
