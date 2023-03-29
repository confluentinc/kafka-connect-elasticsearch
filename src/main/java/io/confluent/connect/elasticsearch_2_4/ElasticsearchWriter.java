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

package io.confluent.connect.elasticsearch_2_4;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.connect.elasticsearch_2_4.bulk.BulkProcessor;
import io.confluent.connect.elasticsearch_2_4.cluster.mapping.ClusterMapper;
import io.confluent.connect.elasticsearch_2_4.index.mapping.IndexMapper;
import io.confluent.connect.elasticsearch_2_4.jest.JestElasticsearchClient;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.confluent.connect.elasticsearch_2_4.DataConverter.BehaviorOnNullValues;
import static io.confluent.connect.elasticsearch_2_4.bulk.BulkProcessor.BehaviorOnMalformedDoc;

public class ElasticsearchWriter {
  private static final Logger log = LoggerFactory.getLogger(ElasticsearchWriter.class);

  private final Map<String, ElasticsearchClient> clients;
  private final String type;
  private final boolean ignoreKey;
  private final Set<String> ignoreKeyTopics;
  private final boolean ignoreSchema;
  private final Set<String> ignoreSchemaTopics;
  @Deprecated
  private final Map<String, String> topicToIndexMap;
  private final long flushTimeoutMs;
  private final Map<String, BulkProcessor<IndexableRecord, ?>> bulkProcessors;
  private final boolean dropInvalidMessage;
  private final BehaviorOnNullValues behaviorOnNullValues;
  private final DataConverter converter;

  private final Set<String> existingMappings;

  private final IndexMapper indexMapper;

  private final ClusterMapper clusterMapper;

  private final Map<String, String> configurations;
  private final int maxBufferedRecords;
  private final int maxInFlightRequests;
  private final int batchSize;
  private final long lingerMs;
  private final int maxRetries;
  private final long retryBackoffMs;
  private final BehaviorOnMalformedDoc behaviorOnMalformedDoc;
  private final ErrantRecordReporter reporter;

  ElasticsearchWriter(
      Map<String, String> configurations,
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
      BehaviorOnMalformedDoc behaviorOnMalformedDoc,
      ErrantRecordReporter reporter,
      IndexMapper indexMapper,
      ClusterMapper clusterMapper) {
    this.configurations = configurations;
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
    this.indexMapper = indexMapper;
    this.clusterMapper = clusterMapper;
    this.maxBufferedRecords = maxBufferedRecords;
    this.maxInFlightRequests = maxInFlightRequests;
    this.batchSize = batchSize;
    this.lingerMs = lingerMs;
    this.maxRetries = maxRetries;
    this.retryBackoffMs = retryBackoffMs;
    this.behaviorOnMalformedDoc = behaviorOnMalformedDoc;
    this.reporter = reporter;
    this.clients = new HashMap<>();
    this.bulkProcessors = new HashMap<>();

    existingMappings = new HashSet<>();
  }

  public static class Builder {
    private Map<String, String> configurations;
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
    private ErrantRecordReporter reporter;
    private IndexMapper indexMapper;
    private ClusterMapper clusterMapper;

    public Builder(Map<String, String> configurations) {
      this.configurations = configurations;
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

    public Builder setErrantRecordReporter(ErrantRecordReporter reporter) {
      this.reporter = reporter;
      return this;
    }

    public Builder setIndexMapper(IndexMapper indexMapper) {
      this.indexMapper = indexMapper;
      return this;
    }

    public Builder setClusterMapper(ClusterMapper clusterMapper) {
      this.clusterMapper = clusterMapper;
      return this;
    }


    public ElasticsearchWriter build() {
      return new ElasticsearchWriter(
          configurations,
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
          behaviorOnMalformedDoc,
          reporter,
          indexMapper,
          clusterMapper
      );
    }
  }

  public void write(Collection<SinkRecord> records) {
    for (SinkRecord sinkRecord : records) {
      // Preemptively skip records with null values if they're going to be ignored anyways
      if (ignoreRecord(sinkRecord)) {
        log.trace(
            "Ignoring sink record with null value for topic/partition/offset {}/{}/{}",
            sinkRecord.topic(),
            sinkRecord.kafkaPartition(),
            sinkRecord.kafkaOffset()
        );
        continue;
      }
      log.trace("Writing record to Elasticsearch: topic/partition/offset {}/{}/{}",
          sinkRecord.topic(),
          sinkRecord.kafkaPartition(),
          sinkRecord.kafkaOffset()
      );
      String index = "";
      try {
        index = getIndexName(sinkRecord.topic(), sinkRecord);
      } catch (Exception e) {
        throw new ConnectException(e.getMessage());
      }
      String clusterKey = null;
      try {
        clusterKey = getClusterKey(sinkRecord.topic(), sinkRecord);
      } catch (Exception e) {
        throw new ConnectException(e.getMessage());
      }
      final boolean ignoreKey = ignoreKeyTopics.contains(sinkRecord.topic()) || this.ignoreKey;
      final boolean ignoreSchema =
          ignoreSchemaTopics.contains(sinkRecord.topic()) || this.ignoreSchema;

      getClient(clusterKey).createIndices(Collections.singleton(index));

      if (!ignoreSchema && !existingMappings.contains(index)) {
        try {
          if (Mapping.getMapping(getClient(clusterKey), index, type) == null) {
            Mapping.createMapping(getClient(clusterKey), index, type, sinkRecord.valueSchema());
          }
        } catch (IOException e) {
          // FIXME: concurrent tasks could attempt to create the mapping and one of the requests may
          // fail
          throw new ConnectException("Failed to initialize mapping for index: " + index, e);
        }
        log.debug("Locally caching mapping for index '{}'", index);
        existingMappings.add(index);
      }

      tryWriteRecord(sinkRecord, index, clusterKey, ignoreKey, ignoreSchema);
    }
  }

  private String getClusterKey(String topic, SinkRecord sinkRecord) throws Exception {
    return String.join(",", getClusterName(topic, sinkRecord));
  }

  private ElasticsearchClient getClient(String clusterKey) {
    if (!clients.containsKey(clusterKey)) {

      ElasticsearchClient client =
              new JestElasticsearchClient(
                      configurations,
                      new HashSet<>(Arrays.asList(clusterKey.split(",")))
              );
      clients.put(clusterKey, client);
    }
    return clients.get(clusterKey);
  }

  private BulkProcessor<IndexableRecord, ?> getBulkProcessor(String clusterKey) {
    if (!bulkProcessors.containsKey(clusterKey)) {
      BulkProcessor<IndexableRecord, ?> bp = new BulkProcessor<>(
          new SystemTime(),
          new BulkIndexingClient(getClient(clusterKey)),
          maxBufferedRecords,
          maxInFlightRequests,
          batchSize,
          lingerMs,
          maxRetries,
          retryBackoffMs,
          behaviorOnMalformedDoc,
          reporter
      );
      bp.start();
      bulkProcessors.put(clusterKey, bp);
    }
    return bulkProcessors.get(clusterKey);
  }

  private boolean ignoreRecord(SinkRecord record) {
    return record.value() == null && behaviorOnNullValues == BehaviorOnNullValues.IGNORE;
  }

  private void tryWriteRecord(
          SinkRecord sinkRecord,
          String index,
          String clusterKey,
          boolean ignoreKey,
          boolean ignoreSchema) {
    IndexableRecord record = null;
    try {
      record = converter.convertRecord(
              sinkRecord,
              index,
              type,
              ignoreKey,
              ignoreSchema);
    } catch (ConnectException convertException) {
      if (dropInvalidMessage) {
        log.error(
            "Can't convert record from topic/partition/offset {}/{}/{}. "
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
    if (record != null) {
      log.trace(
              "Adding record from topic/partition/offset {}/{}/{} to bulk processor",
              sinkRecord.topic(),
              sinkRecord.kafkaPartition(),
              sinkRecord.kafkaOffset()
      );
      getBulkProcessor(clusterKey).add(record, sinkRecord, flushTimeoutMs);
    }
  }



  /**
   * Return the expected index name for a given topic, using the configured mapping or the topic
   * name. Elasticsearch accepts only lowercase index names
   * (<a href="https://github.com/elastic/elasticsearch/issues/29420">ref</a>_.
   */
  private String getIndexName(String topic, SinkRecord sinkRecord) throws Exception {
    JsonNode valueJson = null;
    if (sinkRecord.value() != null) {
      valueJson = converter.getValueAsJson(sinkRecord);
    }
    final String indexOverride = indexMapper.getIndex(topic, valueJson);
    String index = indexOverride != null ? indexOverride : topic.toLowerCase();
    log.trace("calculated index is '{}'", index);
    return index;
  }

  /**
   * Return the expected cluster name for a given topic, using the configured mapping or the topic
   * name. Elasticsearch accepts only lowercase index names
   * (<a href="https://github.com/elastic/elasticsearch/issues/29420">ref</a>_.
   */
  private Set<String> getClusterName(String topic, SinkRecord sinkRecord) throws Exception {
    JsonNode valueJson = null;
    if (sinkRecord.value() != null) {
      valueJson = converter.getValueAsJson(sinkRecord);
    }
    final Set<String> cluster = clusterMapper.getCluster(topic, valueJson);;
    log.trace("calculated cluster is '{}'", cluster);
    return cluster;
  }

  public void flush() {
    bulkProcessors.values().forEach(bp -> bp.flush(flushTimeoutMs));
  }

  public void stop() {
    try {
      log.debug(
          "Flushing records, waiting up to {}ms ('{}')",
          flushTimeoutMs,
          ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG
      );
      bulkProcessors.values().forEach(bp -> bp.flush(flushTimeoutMs));
    } catch (Exception e) {
      log.warn("Failed to flush during stop", e);
    }
    log.debug("Stopping Elastisearch writer");
    bulkProcessors.values().forEach(BulkProcessor::stop);
    log.debug(
        "Waiting for bulk processor to stop, up to {}ms ('{}')",
        flushTimeoutMs,
        ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG
    );
    bulkProcessors.values().forEach(bp -> bp.awaitStop(flushTimeoutMs));

    clients.values().forEach(ElasticsearchClient::close);

    log.debug("Stopped Elastisearch writer");
  }
}
