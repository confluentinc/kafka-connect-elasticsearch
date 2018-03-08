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

import io.confluent.connect.elasticsearch.bulk.BulkProcessor;
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

import static io.confluent.connect.elasticsearch.DataConverter.BehaviorOnNullValues;
import static io.confluent.connect.elasticsearch.bulk.BulkProcessor.BehaviorOnMalformedDoc;

public class ElasticsearchWriter {
  private static final Logger log = LoggerFactory.getLogger(ElasticsearchWriter.class);

  private final ElasticsearchClient client;
  private final String type;
  private final boolean ignoreKey;
  private final Set<String> ignoreKeyTopics;
  private final boolean ignoreSchema;
  private final Set<String> ignoreSchemaTopics;
  @Deprecated
  private final Map<String, String> topicToIndexMap;
  private final long flushTimeoutMs;
  private final BulkProcessor<IndexableRecord, ?> bulkProcessor;
  private final boolean dropInvalidMessage;
  private final BehaviorOnNullValues behaviorOnNullValues;
  private final DataConverter converter;

  private final Set<String> existingMappings;
  private final BehaviorOnMalformedDoc behaviorOnMalformedDoc;

  ElasticsearchWriter(
      ElasticsearchClient client,
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
      boolean dropInvalidMessage,
      BehaviorOnNullValues behaviorOnNullValues,
      BehaviorOnMalformedDoc behaviorOnMalformedDoc
  ) {
    this.client = client;
    this.type = type;
    this.ignoreKey = ignoreKey;
    this.ignoreKeyTopics = ignoreKeyTopics;
    this.ignoreSchema = ignoreSchema;
    this.ignoreSchemaTopics = ignoreSchemaTopics;
    this.topicToIndexMap = topicToIndexMap;
    this.flushTimeoutMs = flushTimeoutMs;
    this.dropInvalidMessage = dropInvalidMessage;
    this.behaviorOnNullValues = behaviorOnNullValues;
    this.converter = new DataConverter(useCompactMapEntries, behaviorOnNullValues);
    this.behaviorOnMalformedDoc = behaviorOnMalformedDoc;

    bulkProcessor = new BulkProcessor<>(
        new SystemTime(),
        new BulkIndexingClient(client),
        maxBufferedRecords,
        maxInFlightRequests,
        batchSize,
        lingerMs,
        maxRetries,
        retryBackoffMs,
        behaviorOnMalformedDoc
    );

    existingMappings = new HashSet<>();
  }

  public static class Builder {
    private final ElasticsearchClient client;
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
    private boolean dropInvalidMessage;
    private BehaviorOnNullValues behaviorOnNullValues = BehaviorOnNullValues.DEFAULT;
    private BehaviorOnMalformedDoc behaviorOnMalformedDoc;

    public Builder(ElasticsearchClient client) {
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

    public Builder setDropInvalidMessage(boolean dropInvalidMessage) {
      this.dropInvalidMessage = dropInvalidMessage;
      return this;
    }

    /**
     * Change the behavior that the resulting {@link ElasticsearchWriter} will have when it
     * encounters records with null values.
     * @param behaviorOnNullValues Cannot be null. If in doubt, {@link BehaviorOnNullValues#DEFAULT}
     *                             can be used.
     */
    public Builder setBehaviorOnNullValues(BehaviorOnNullValues behaviorOnNullValues) {
      this.behaviorOnNullValues =
          Objects.requireNonNull(behaviorOnNullValues, "behaviorOnNullValues cannot be null");
      return this;
    }

    public Builder setBehaviorOnMalformedDoc(BehaviorOnMalformedDoc behaviorOnMalformedDoc) {
      this.behaviorOnMalformedDoc = behaviorOnMalformedDoc;
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
          dropInvalidMessage,
          behaviorOnNullValues,
          behaviorOnMalformedDoc
      );
    }
  }

  public void write(Collection<SinkRecord> records) {
    for (SinkRecord sinkRecord : records) {
      // Preemptively skip records with null values if they're going to be ignored anyways
      if (ignoreRecord(sinkRecord)) {
        log.debug(
            "Ignoring sink record with key {} and null value for topic/partition/offset {}/{}/{}",
            sinkRecord.key(),
            sinkRecord.topic(),
            sinkRecord.kafkaPartition(),
            sinkRecord.kafkaOffset());
        continue;
      }

      final String indexOverride = topicToIndexMap.get(sinkRecord.topic());
      final String index = indexOverride != null ? indexOverride : sinkRecord.topic();
      final boolean ignoreKey = ignoreKeyTopics.contains(sinkRecord.topic()) || this.ignoreKey;
      final boolean ignoreSchema =
          ignoreSchemaTopics.contains(sinkRecord.topic()) || this.ignoreSchema;

      if (!ignoreSchema && !existingMappings.contains(index)) {
        try {
          if (Mapping.getMapping(client, index, type) == null) {
            Mapping.createMapping(client, index, type, sinkRecord.valueSchema());
          }
        } catch (IOException e) {
          // FIXME: concurrent tasks could attempt to create the mapping and one of the requests may
          // fail
          throw new ConnectException("Failed to initialize mapping for index: " + index, e);
        }
        existingMappings.add(index);
      }

      tryWriteRecord(sinkRecord, index, ignoreKey, ignoreSchema);
    }
  }

  private boolean ignoreRecord(SinkRecord record) {
    return record.value() == null && behaviorOnNullValues == BehaviorOnNullValues.IGNORE;
  }

  private void tryWriteRecord(
      SinkRecord sinkRecord,
      String index,
      boolean ignoreKey,
      boolean ignoreSchema) {

    try {
      IndexableRecord record = converter.convertRecord(
          sinkRecord,
          index,
          type,
          ignoreKey,
          ignoreSchema);
      if (record != null) {
        bulkProcessor.add(record, flushTimeoutMs);
      }
    } catch (ConnectException convertException) {
      if (dropInvalidMessage) {
        log.error(
            "Can't convert record from topic {} with partition {} and offset {}. "
                + "Error message: {}",
            sinkRecord.topic(),
            sinkRecord.kafkaPartition(),
            sinkRecord.kafkaOffset(),
            convertException.getMessage()
        );
      } else {
        throw convertException;
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

  public void createIndicesForTopics(Set<String> assignedTopics) {
    Objects.requireNonNull(assignedTopics);
    client.createIndices(indicesForTopics(assignedTopics));
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
