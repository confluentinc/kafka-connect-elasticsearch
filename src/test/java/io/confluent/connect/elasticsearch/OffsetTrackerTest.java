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

import io.confluent.connect.elasticsearch.OffsetTracker.OffsetState;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class OffsetTrackerTest {

  @Test
  public void testHappyPath() {
    OffsetTracker offsetTracker = new OffsetTracker(10);

    TopicPartition tp = new TopicPartition("t1", 0);

    SinkRecord record1 = sinkRecord(tp, 0);
    SinkRecord record2 = sinkRecord(tp, 1);
    SinkRecord record3 = sinkRecord(tp, 2);

    OffsetState offsetState1 = offsetTracker.addPendingRecord(record1);
    OffsetState offsetState2 = offsetTracker.addPendingRecord(record2);
    OffsetState offsetState3 = offsetTracker.addPendingRecord(record3);

    assertThat(offsetTracker.getOffsets()).isEmpty();

    offsetState2.markProcessed();
    assertThat(offsetTracker.getOffsets()).isEmpty();

    offsetState1.markProcessed();

    offsetTracker.moveOffsets();
    Map<TopicPartition, OffsetAndMetadata> offsetMap = offsetTracker.getOffsets();
    assertThat(offsetMap).hasSize(1);
    assertThat(offsetMap.get(tp).offset()).isEqualTo(2);

    offsetState3.markProcessed();
    offsetTracker.moveOffsets();
    offsetMap = offsetTracker.getOffsets();
    assertThat(offsetMap).hasSize(1);
    assertThat(offsetMap.get(tp).offset()).isEqualTo(3);

    offsetTracker.moveOffsets();
    assertThat(offsetMap.get(tp).offset()).isEqualTo(3);
  }

  /**
   * Verify that if we receive records that are below the already committed offset for partition
   * (e.g. after a RetriableException), the offset reporting is not affected.
   */
  @Test
  public void testBelowWatermark() {
    OffsetTracker offsetTracker = new OffsetTracker(10);

    TopicPartition tp = new TopicPartition("t1", 0);

    SinkRecord record1 = sinkRecord(tp, 0);
    SinkRecord record2 = sinkRecord(tp, 1);

    OffsetState offsetState1 = offsetTracker.addPendingRecord(record1);
    OffsetState offsetState2 = offsetTracker.addPendingRecord(record2);

    offsetState1.markProcessed();
    offsetState2.markProcessed();
    offsetTracker.moveOffsets();
    assertThat(offsetTracker.getOffsets().get(tp).offset()).isEqualTo(2);

    offsetState2 = offsetTracker.addPendingRecord(record2);
    offsetTracker.moveOffsets();
    assertThat(offsetTracker.getOffsets().get(tp).offset()).isEqualTo(2);

    offsetState2.markProcessed();
    offsetTracker.moveOffsets();
    assertThat(offsetTracker.getOffsets().get(tp).offset()).isEqualTo(2);
  }

  @Test
  public void testBatchRetry() {
    OffsetTracker offsetTracker = new OffsetTracker(10);

    TopicPartition tp = new TopicPartition("t1", 0);

    SinkRecord record1 = sinkRecord(tp, 0);
    SinkRecord record2 = sinkRecord(tp, 1);

    OffsetState offsetState1A = offsetTracker.addPendingRecord(record1);
    OffsetState offsetState2A = offsetTracker.addPendingRecord(record2);

    // first fails but second succeeds
    offsetState2A.markProcessed();
    offsetTracker.moveOffsets();
    assertThat(offsetTracker.getOffsets()).isEmpty();

    // now simulate the batch being retried by the framework (e.g. after a RetriableException)
    OffsetState offsetState1B = offsetTracker.addPendingRecord(record1);
    OffsetState offsetState2B = offsetTracker.addPendingRecord(record2);

    offsetState2B.markProcessed();
    offsetState1B.markProcessed();
    offsetTracker.moveOffsets();
    assertThat(offsetTracker.getOffsets().get(tp).offset()).isEqualTo(2);
  }

  @Test
  public void testRebalance() {
    OffsetTracker offsetTracker = new OffsetTracker(10);

    TopicPartition tp1 = new TopicPartition("t1", 0);
    TopicPartition tp2 = new TopicPartition("t2", 0);
    TopicPartition tp3 = new TopicPartition("t3", 0);

    SinkRecord record1 = sinkRecord(tp1, 0);
    SinkRecord record2 = sinkRecord(tp2, 0);

    offsetTracker.addPendingRecord(record1).markProcessed();
    offsetTracker.addPendingRecord(record2).markProcessed();

    offsetTracker.moveOffsets();
    assertThat(offsetTracker.getOffsets().size()).isEqualTo(2);

    offsetTracker.closePartitions(ImmutableList.of(tp1, tp3));
    assertThat(offsetTracker.getOffsets().keySet()).containsExactly(tp2);
  }

  @Test
  public void testBackPressure() {
    OffsetTracker offsetTracker = new OffsetTracker(2);

    TopicPartition tp1 = new TopicPartition("t1", 0);
    TopicPartition tp2 = new TopicPartition("t2", 0);

    offsetTracker.addPendingRecord(sinkRecord(tp1, 0));
    offsetTracker.addPendingRecord(sinkRecord(tp2, 0));

    // verify this call blocks
    CompletableFuture<Void> future = CompletableFuture.runAsync(() ->
            offsetTracker.addPendingRecord(sinkRecord(tp1, 1)));

    try {
      future.get(1, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // this is what we expect
    } catch (Exception e) {
      fail("Unexpected exception", e);
    } finally {
      future.cancel(true);
    }
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
