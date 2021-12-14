/*
 * Copyright 2021 Confluent Inc.
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

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Tracks processed records to calculate safe offsets to commit.
 *
 */
public interface OffsetTracker {

  /**
   * Method that return a total number of offset entries, that is in memory
   *
   * @return number of offset entries
   */
  default long numOffsetStateEntries() {
    return 0;
  }

  /**
   * Method that cleans up entries that are not needed anymore
   * (all the contiguous processed entries since the last reported offset)
   */
  default void updateOffsets() {
  }

  /**
   * Add a pending record
   * @param record record that has to be added
   * @return offset state, associated with this record
   */
  OffsetState addPendingRecord(SinkRecord record);

  /**
   * Method that returns offsets, that are safe to commit
   *
   * @param currentOffsets current offsets, that are provided by a task
   * @return offsets that are safe to commit
   */
  Map<TopicPartition, OffsetAndMetadata> offsets(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets
  );

  /**
   * Close partitions that are no longer assigned to the task
   * @param partitions partitions that have to be closed
   */
  default void closePartitions(Collection<TopicPartition> partitions) {
  }
}
