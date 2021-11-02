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
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

public interface OffsetTracker {

  long numOffsetStateEntries();

  void updateOffsets();

  OffsetState addPendingRecord(SinkRecord record, Set<TopicPartition> assignment);

  Map<TopicPartition, OffsetAndMetadata> offsets(
      ElasticsearchClient client,
      Map<TopicPartition, OffsetAndMetadata> currentOffsets
  );

  void closePartitions(Collection<TopicPartition> partitions);
}
