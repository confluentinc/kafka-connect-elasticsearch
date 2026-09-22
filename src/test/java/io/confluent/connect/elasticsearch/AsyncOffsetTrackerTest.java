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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AsyncOffsetTrackerTest {

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
    Map<TopicPartition, OffsetAndMetadata> currentOffsets = frameworkOffsets(tp, 3);

    // Nothing is processed yet, so the commit is pinned at the first record.
    assertThat(offsetTracker.offsets(currentOffsets).get(tp).offset()).isEqualTo(0);

    offsetState2.markProcessed();
    assertThat(offsetTracker.offsets(currentOffsets).get(tp).offset()).isEqualTo(0);

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
    Map<TopicPartition, OffsetAndMetadata> currentOffsets = frameworkOffsets(tp, 2);

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
    Map<TopicPartition, OffsetAndMetadata> currentOffsets = frameworkOffsets(tp, 2);

    // first fails but second succeeds: the commit stays pinned at the failed record
    offsetState2A.markProcessed();
    offsetTracker.updateOffsets();
    assertThat(offsetTracker.offsets(currentOffsets).get(tp).offset()).isEqualTo(0);

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
    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    currentOffsets.put(tp1, new OffsetAndMetadata(2));
    currentOffsets.put(tp2, new OffsetAndMetadata(1));
    currentOffsets.put(tp3, new OffsetAndMetadata(0));
    Map<TopicPartition, OffsetAndMetadata> offsets = offsetTracker.offsets(currentOffsets);
    assertThat(offsets.get(tp1).offset()).isEqualTo(1);
    assertThat(offsets.get(tp2).offset()).isEqualTo(1);
    assertThat(offsets.get(tp3).offset()).isEqualTo(0);
    assertThat(offsetTracker.numOffsetStateEntries()).isEqualTo(1);

    // The runtime commits revoked partitions before close() and never asks about them again.
    offsetTracker.closePartitions(ImmutableList.of(tp1, tp3));
    assertThat(offsetTracker.offsets(frameworkOffsets(tp2, 1)).keySet()).containsExactly(tp2);
    assertThat(offsetTracker.numOffsetStateEntries()).isEqualTo(0);
  }

  // Records that never reach put() (Filter SMT, converter failure under errors.tolerance=all)
  // are still consumed by the framework, so a partition the tracker has never seen must commit
  // the framework's position instead of opting out and freezing the offset (CC-43920).
  @Test
  public void testUnseenPartitionCommitsFrameworkOffset() {
    AsyncOffsetTracker offsetTracker = new AsyncOffsetTracker(context);
    TopicPartition tp = new TopicPartition("t1", 0);
    when(context.assignment()).thenReturn(Collections.singleton(tp));

    Map<TopicPartition, OffsetAndMetadata> framework = frameworkOffsets(tp, 42);
    assertThat(offsetTracker.offsets(framework)).containsExactlyEntriesOf(framework);
  }

  // Once every received record is processed, trailing records that never reached put() must
  // not leave a permanent gap between the committed offset and the consumed position.
  @Test
  public void testFullyProcessedPartitionSweepsToFrameworkOffset() {
    AsyncOffsetTracker offsetTracker = new AsyncOffsetTracker(context);
    TopicPartition tp = new TopicPartition("t1", 0);
    when(context.assignment()).thenReturn(Collections.singleton(tp));

    offsetTracker.addPendingRecord(sinkRecord(tp, 0)).markProcessed();
    offsetTracker.addPendingRecord(sinkRecord(tp, 1)).markProcessed();
    offsetTracker.updateOffsets();

    assertThat(offsetTracker.offsets(frameworkOffsets(tp, 10)).get(tp).offset()).isEqualTo(10);
  }

  // A record still in flight pins the commit at its own offset even when later records are
  // already processed, so a restart redelivers it and nothing below it is redelivered.
  @Test
  public void testPendingRecordPinsCommitAtItsOffset() {
    AsyncOffsetTracker offsetTracker = new AsyncOffsetTracker(context);
    TopicPartition tp = new TopicPartition("t1", 0);
    when(context.assignment()).thenReturn(Collections.singleton(tp));

    offsetTracker.addPendingRecord(sinkRecord(tp, 0)).markProcessed();
    OffsetState pending = offsetTracker.addPendingRecord(sinkRecord(tp, 3));
    offsetTracker.addPendingRecord(sinkRecord(tp, 5)).markProcessed();
    offsetTracker.updateOffsets();

    assertThat(offsetTracker.offsets(frameworkOffsets(tp, 10)).get(tp).offset()).isEqualTo(3);

    pending.markProcessed();
    offsetTracker.updateOffsets();
    assertThat(offsetTracker.offsets(frameworkOffsets(tp, 10)).get(tp).offset()).isEqualTo(10);
  }

  // The framework rejects task offsets above the consumed position, so never exceed it.
  @Test
  public void testCommitNeverExceedsFrameworkOffset() {
    AsyncOffsetTracker offsetTracker = new AsyncOffsetTracker(context);
    TopicPartition tp = new TopicPartition("t1", 0);
    when(context.assignment()).thenReturn(Collections.singleton(tp));

    offsetTracker.addPendingRecord(sinkRecord(tp, 3));
    offsetTracker.updateOffsets();

    assertThat(offsetTracker.offsets(frameworkOffsets(tp, 2)).get(tp).offset()).isEqualTo(2);
  }

  // A revocation-time preCommit asks only about the revoked partitions; do not volunteer others.
  @Test
  public void testOnlyRequestedPartitionsAreReturned() {
    AsyncOffsetTracker offsetTracker = new AsyncOffsetTracker(context);
    TopicPartition tp1 = new TopicPartition("t1", 0);
    TopicPartition tp2 = new TopicPartition("t2", 0);
    when(context.assignment()).thenReturn(new HashSet<>(Arrays.asList(tp1, tp2)));

    offsetTracker.addPendingRecord(sinkRecord(tp1, 0)).markProcessed();
    offsetTracker.addPendingRecord(sinkRecord(tp2, 0)).markProcessed();
    offsetTracker.updateOffsets();

    assertThat(offsetTracker.offsets(frameworkOffsets(tp2, 1)).keySet()).containsExactly(tp2);
  }

  private Map<TopicPartition, OffsetAndMetadata> frameworkOffsets(TopicPartition tp, long offset) {
    return Collections.singletonMap(tp, new OffsetAndMetadata(offset));
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
