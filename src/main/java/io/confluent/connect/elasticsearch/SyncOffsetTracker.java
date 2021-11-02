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
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * It's a no-op offset tracker to use with <code>FLUSH_SYNCHRONOUSLY_CONFIG=true</code>
 *
 */
public class SyncOffsetTracker implements OffsetTracker {

  @Override
  public long numOffsetStateEntries() {
    return 0;
  }

  @Override
  public void updateOffsets() {
  }

  @Override
  public SyncOffsetState addPendingRecord(SinkRecord record, Set<TopicPartition> assignment) {
    return new SyncOffsetState();
  }


  /**
   * This is a blocking method,
   * that blocks until client doesn't have any in-flight requests
   *
   * @param client Elasticsearch client
   * @param currentOffsets current offsets from a task
   * @return offsets to commit
   */
  @Override
  public Map<TopicPartition, OffsetAndMetadata> offsets(
      ElasticsearchClient client,
      Map<TopicPartition, OffsetAndMetadata> currentOffsets
  ) {
    client.waitForInFlightRequests();
    return client.isFailed() ? Collections.emptyMap() : currentOffsets;
  }

  @Override
  public void closePartitions(Collection<TopicPartition> partitions) {
  }

  static class SyncOffsetState implements OffsetState {
    @Override
    public void markProcessed() {
    }

    @Override
    public boolean isProcessed() {
      return false;
    }

    @Override
    public long offset() {
      return -1;
    }
  }
}
