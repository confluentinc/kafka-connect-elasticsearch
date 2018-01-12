/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.elasticsearch;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.confluent.connect.elasticsearch.bulk.BulkProcessor;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;

import static io.confluent.connect.elasticsearch.DataConverter.BehaviorOnNullValues;

public class ElasticsearchWriter {
  private static final Logger log = LoggerFactory.getLogger(ElasticsearchWriter.class);

  private final JestClient client;
  private final String type;
  private final boolean ignoreKey;
  private final Set<String> ignoreKeyTopics;
  private final boolean ignoreSchema;
  private final Set<String> ignoreSchemaTopics;
  private final Map<String, String> topicToIndexMap;
  private final long flushTimeoutMs;
  private final BulkProcessor<IndexableRecord, ?> bulkProcessor;
  private final DataConverter converter;

  private final Set<String> existingMappings;

  ElasticsearchWriter(
      JestClient client,
      String type,
      boolean useCompactMapEntries,
      boolean ignoreKey,
      Set<String> ignoreKeyTopics,
      boolean ignoreSchema,
      Set<String> ignoreSchemaTopics,
      Map<String, String> topicToIndexMap,
      long flushTimeoutMs,
      int maxBufferedRecords,
      int maxInFlightRequests,
      int batchSize,
      long lingerMs,
      int maxRetries,
      long retryBackoffMs,
      BehaviorOnNullValues behaviorOnNullValues
  ) {
    this.client = client;
    this.type = type;
    this.ignoreKey = ignoreKey;
    this.ignoreKeyTopics = ignoreKeyTopics;
    this.ignoreSchema = ignoreSchema;
    this.ignoreSchemaTopics = ignoreSchemaTopics;
    this.topicToIndexMap = topicToIndexMap;
    this.flushTimeoutMs = flushTimeoutMs;
    this.converter = new DataConverter(useCompactMapEntries, behaviorOnNullValues);

    bulkProcessor = new BulkProcessor<>(
        new SystemTime(),
        new BulkIndexingClient(client),
        maxBufferedRecords,
        maxInFlightRequests,
        batchSize,
        lingerMs,
        maxRetries,
        retryBackoffMs
    );

    existingMappings = new HashSet<>();
  }

  public static class Builder {
    private final JestClient client;
    private String type;
    private boolean useCompactMapEntries = true;
    private boolean ignoreKey = false;
    private Set<String> ignoreKeyTopics = Collections.emptySet();
    private boolean ignoreSchema = false;
    private Set<String> ignoreSchemaTopics = Collections.emptySet();
    private Map<String, String> topicToIndexMap = new HashMap<>();
    private long flushTimeoutMs;
    private int maxBufferedRecords;
    private int maxInFlightRequests;
    private int batchSize;
    private long lingerMs;
    private int maxRetry;
    private long retryBackoffMs;
    private BehaviorOnNullValues behaviorOnNullValues = BehaviorOnNullValues.DEFAULT;

    public Builder(JestClient client) {
      this.client = client;
    }

    public Builder setType(String type) {
      this.type = type;
      return this;
    }

    public Builder setIgnoreKey(boolean ignoreKey, Set<String> ignoreKeyTopics) {
      this.ignoreKey = ignoreKey;
      this.ignoreKeyTopics = ignoreKeyTopics;
      return this;
    }

    public Builder setIgnoreSchema(boolean ignoreSchema, Set<String> ignoreSchemaTopics) {
      this.ignoreSchema = ignoreSchema;
      this.ignoreSchemaTopics = ignoreSchemaTopics;
      return this;
    }

    public Builder setCompactMapEntries(boolean useCompactMapEntries) {
      this.useCompactMapEntries = useCompactMapEntries;
      return this;
    }

    public Builder setTopicToIndexMap(Map<String, String> topicToIndexMap) {
      this.topicToIndexMap = topicToIndexMap;
      return this;
    }

    public Builder setFlushTimoutMs(long flushTimeoutMs) {
      this.flushTimeoutMs = flushTimeoutMs;
      return this;
    }

    public Builder setMaxBufferedRecords(int maxBufferedRecords) {
      this.maxBufferedRecords = maxBufferedRecords;
      return this;
    }

    public Builder setMaxInFlightRequests(int maxInFlightRequests) {
      this.maxInFlightRequests = maxInFlightRequests;
      return this;
    }

    public Builder setBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder setLingerMs(long lingerMs) {
      this.lingerMs = lingerMs;
      return this;
    }

    public Builder setMaxRetry(int maxRetry) {
      this.maxRetry = maxRetry;
      return this;
    }

    public Builder setRetryBackoffMs(long retryBackoffMs) {
      this.retryBackoffMs = retryBackoffMs;
      return this;
    }

    /**
     * Change the behavior that the resulting {@link ElasticsearchWriter} will have when it
     * encounters records with null values.
     * @param behaviorOnNullValues Cannot be null. If in doubt,
     *                             {@link BehaviorOnNullValues#DEFAULT} can be used.
     */
    public Builder setBehaviorOnNullValues(BehaviorOnNullValues behaviorOnNullValues) {
      this.behaviorOnNullValues =
          Objects.requireNonNull(behaviorOnNullValues, "behaviorOnNullValues cannot be null");
      return this;
    }

    public ElasticsearchWriter build() {
      return new ElasticsearchWriter(
          client,
          type,
          useCompactMapEntries,
          ignoreKey,
          ignoreKeyTopics,
          ignoreSchema,
          ignoreSchemaTopics,
          topicToIndexMap,
          flushTimeoutMs,
          maxBufferedRecords,
          maxInFlightRequests,
          batchSize,
          lingerMs,
          maxRetry,
          retryBackoffMs,
          behaviorOnNullValues
      );
    }
  }

  public void write(Collection<SinkRecord> records) {
    for (SinkRecord sinkRecord : records) {
      final String indexOverride = topicToIndexMap.get(sinkRecord.topic());
      final String index = indexOverride != null ? indexOverride : sinkRecord.topic();
      final boolean ignoreKey = ignoreKeyTopics.contains(sinkRecord.topic()) || this.ignoreKey;
      final boolean ignoreSchema = ignoreSchemaTopics.contains(sinkRecord.topic()) || this.ignoreSchema;

      if (!ignoreSchema && !existingMappings.contains(index)) {
        try {
          if (Mapping.getMapping(client, index, type) == null) {
            Mapping.createMapping(client, index, type, sinkRecord.valueSchema());
          }
        } catch (IOException e) {
          // FIXME: concurrent tasks could attempt to create the mapping and one of the requests may fail
          throw new ConnectException("Failed to initialize mapping for index: " + index, e);
        }
        existingMappings.add(index);
      }

      final IndexableRecord indexableRecord = converter.convertRecord(
          sinkRecord,
          index,
          type,
          ignoreKey,
          ignoreSchema
      );

      // In the event that the sink record's value was null and the data converter has been told to
      // ignore null values, the returned record will be null.
      // TODO: If necessary, move the check for null-valued records to the top of the for-loop to
      //       avoid potential performance penalties. Leaving the check here since it relegates all
      //       three kinds of null-record-handling behavior to the DataConverter class.
      if (indexableRecord != null) {
        bulkProcessor.add(indexableRecord, flushTimeoutMs);
      } else {
        log.debug(
            "Ignoring sink record with key of {} and null value for topic/partition/offset "
                + "{}/{}/{}",
            sinkRecord.key(),
            sinkRecord.topic(),
            sinkRecord.kafkaPartition(),
            sinkRecord.kafkaOffset()
        );
      }
    }
  }

  public void flush() {
    bulkProcessor.flush(flushTimeoutMs);
  }

  public void start() {
    bulkProcessor.start();
  }

  public void stop() {
    try {
      bulkProcessor.flush(flushTimeoutMs);
    } catch (Exception e) {
      log.warn("Failed to flush during stop", e);
    }
    bulkProcessor.stop();
    bulkProcessor.awaitStop(flushTimeoutMs);
  }

  private boolean indexExists(String index) {
    Action action = new IndicesExists.Builder(index).build();
    try {
      JestResult result = client.execute(action);
      return result.isSucceeded();
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  public void createIndicesForTopics(Set<String> assignedTopics) {
    for (String index : indicesForTopics(assignedTopics)) {
      if (!indexExists(index)) {
        CreateIndex createIndex = new CreateIndex.Builder(index).build();
        try {
          JestResult result = client.execute(createIndex);
          if (!result.isSucceeded()) {
            String msg = result.getErrorMessage() != null ? ": " + result.getErrorMessage() : "";
            throw new ConnectException("Could not create index '" + index + "'" + msg);
          }
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }
    }
  }

  private Set<String> indicesForTopics(Set<String> assignedTopics) {
    final Set<String> indices = new HashSet<>();
    for (String topic : assignedTopics) {
      final String index = topicToIndexMap.get(topic);
      if (index != null) {
        indices.add(index);
      } else {
        indices.add(topic);
      }
    }
    return indices;
  }

}
