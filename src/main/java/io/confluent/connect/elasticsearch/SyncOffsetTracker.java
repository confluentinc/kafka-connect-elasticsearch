/**
 * Copyright 2021 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.elasticsearch;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

public class SyncOffsetTracker implements OffsetTracker {
  @Override
  public long numOffsetStateEntries() {
    return 0;
  }

  @Override
  public void updateOffsets() {
  }

  @Override
  public SyncOffsetState addPendingRecord(SinkRecord record) {
    return new SyncOffsetState();
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> offsets() {
    throw new UnsupportedOperationException();
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
