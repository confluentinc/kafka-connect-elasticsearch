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
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AsyncOffsetTrackerTest {

  @SuppressWarnings("unchecked")
  private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = mock(Map.class);
  private SinkTaskContext context = mock(SinkTaskContext.class);

  @Test
  public void testHappyPath() {
    AsyncOffsetTracker offsetTracker = new AsyncOffsetTracker(context);

    TopicPartition tp = new TopicPartition("t1", 0);
    when(context.assignment()).thenReturn(Collections.singleton(tp));

    SinkRecord record1 = sinkRecord(tp, 0);
    SinkRecord record2 = sinkRecord(tp, 1);
    SinkRecord record3 = sinkRecord(tp, 2);

    OffsetState offsetState1 = offsetTracker.addPendingRecord(record1);
    OffsetState offsetState2 = offsetTracker.addPendingRecord(record2);
    OffsetState offsetState3 = offsetTracker.addPendingRecord(record3);

    assertThat(offsetTracker.offsets(currentOffsets)).isEmpty();

    offsetState2.markProcessed();
    assertThat(offsetTracker.offsets(currentOffsets)).isEmpty();

    offsetState1.markProcessed();

    offsetTracker.updateOffsets();
    Map<TopicPartition, OffsetAndMetadata> offsetMap = offsetTracker.offsets(currentOffsets);
    assertThat(offsetMap).hasSize(1);
    assertThat(offsetMap.get(tp).offset()).isEqualTo(2);

    offsetState3.markProcessed();
    offsetTracker.updateOffsets();
    offsetMap = offsetTracker.offsets(currentOffsets);
    assertThat(offsetMap).hasSize(1);
    assertThat(offsetMap.get(tp).offset()).isEqualTo(3);

    offsetTracker.updateOffsets();
    assertThat(offsetMap.get(tp).offset()).isEqualTo(3);
  }

  /**
   * Verify that if we receive records that are below the already committed offset for partition
   * (e.g. after a RetriableException), the offset reporting is not affected.
   */
  @Test
  public void testBelowWatermark() {
    AsyncOffsetTracker offsetTracker = new AsyncOffsetTracker(context);

    TopicPartition tp = new TopicPartition("t1", 0);
    when(context.assignment()).thenReturn(Collections.singleton(tp));

    SinkRecord record1 = sinkRecord(tp, 0);
    SinkRecord record2 = sinkRecord(tp, 1);

    OffsetState offsetState1 = offsetTracker.addPendingRecord(record1);
    OffsetState offsetState2 = offsetTracker.addPendingRecord(record2);

    offsetState1.markProcessed();
    offsetState2.markProcessed();
    offsetTracker.updateOffsets();
    assertThat(offsetTracker.offsets(currentOffsets).get(tp).offset()).isEqualTo(2);

    offsetState2 = offsetTracker.addPendingRecord(record2);
    offsetTracker.updateOffsets();
    assertThat(offsetTracker.offsets(currentOffsets).get(tp).offset()).isEqualTo(2);

    offsetState2.markProcessed();
    offsetTracker.updateOffsets();
    assertThat(offsetTracker.offsets(currentOffsets).get(tp).offset()).isEqualTo(2);
  }

  @Test
  public void testBatchRetry() {
    AsyncOffsetTracker offsetTracker = new AsyncOffsetTracker(context);

    TopicPartition tp = new TopicPartition("t1", 0);
    when(context.assignment()).thenReturn(Collections.singleton(tp));

    SinkRecord record1 = sinkRecord(tp, 0);
    SinkRecord record2 = sinkRecord(tp, 1);

    OffsetState offsetState1A = offsetTracker.addPendingRecord(record1);
    OffsetState offsetState2A = offsetTracker.addPendingRecord(record2);

    // first fails but second succeeds
    offsetState2A.markProcessed();
    offsetTracker.updateOffsets();
    assertThat(offsetTracker.offsets(currentOffsets)).isEmpty();

    // now simulate the batch being retried by the framework (e.g. after a RetriableException)
    OffsetState offsetState1B = offsetTracker.addPendingRecord(record1);
    OffsetState offsetState2B = offsetTracker.addPendingRecord(record2);

    offsetState2B.markProcessed();
    offsetState1B.markProcessed();
    offsetTracker.updateOffsets();
    assertThat(offsetTracker.offsets(currentOffsets).get(tp).offset()).isEqualTo(2);
  }

  @Test
  public void testRebalance() {
    AsyncOffsetTracker offsetTracker = new AsyncOffsetTracker(context);

    TopicPartition tp1 = new TopicPartition("t1", 0);
    TopicPartition tp2 = new TopicPartition("t2", 0);
    TopicPartition tp3 = new TopicPartition("t3", 0);

    when(context.assignment()).thenReturn(new HashSet<>(Arrays.asList(tp1, tp2, tp3)));

    offsetTracker.addPendingRecord(sinkRecord(tp1, 0)).markProcessed();
    offsetTracker.addPendingRecord(sinkRecord(tp1, 1));
    offsetTracker.addPendingRecord(sinkRecord(tp2, 0)).markProcessed();
    assertThat(offsetTracker.numOffsetStateEntries()).isEqualTo(3);

    offsetTracker.updateOffsets();
    assertThat(offsetTracker.offsets(currentOffsets).size()).isEqualTo(2);
    assertThat(offsetTracker.numOffsetStateEntries()).isEqualTo(1);

    offsetTracker.closePartitions(ImmutableList.of(tp1, tp3));
    assertThat(offsetTracker.offsets(currentOffsets).keySet()).containsExactly(tp2);
    assertThat(offsetTracker.numOffsetStateEntries()).isEqualTo(0);
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
