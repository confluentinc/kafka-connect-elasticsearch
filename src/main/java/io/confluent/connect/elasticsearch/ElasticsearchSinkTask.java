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

import io.confluent.connect.elasticsearch.bulk.BulkProcessor;
import io.confluent.connect.elasticsearch.jest.JestElasticsearchClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ElasticsearchSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);
  private ElasticsearchWriter writer;
  private ElasticsearchClient client;
  private Boolean createIndicesAtStartTime;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    start(props, null);
  }

  @SuppressWarnings("deprecation")
  // public for testing
  public void start(Map<String, String> props, ElasticsearchClient client) {
    try {
      log.info("Starting ElasticsearchSinkTask");

      ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
      String type = config.getString(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG);
      boolean ignoreKey =
          config.getBoolean(ElasticsearchSinkConnectorConfig.KEY_IGNORE_CONFIG);
      boolean ignoreVersion =
              config.getBoolean(ElasticsearchSinkConnectorConfig.VERSION_IGNORE_CONFIG);
      boolean ignoreSchema =
          config.getBoolean(ElasticsearchSinkConnectorConfig.SCHEMA_IGNORE_CONFIG);
      boolean useCompactMapEntries =
          config.getBoolean(ElasticsearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG);


      Map<String, String> topicToIndexMap =
          parseMapConfig(config.getList(ElasticsearchSinkConnectorConfig.TOPIC_INDEX_MAP_CONFIG));
      Set<String> topicIgnoreKey =
          new HashSet<>(config.getList(ElasticsearchSinkConnectorConfig.TOPIC_KEY_IGNORE_CONFIG));
      Set<String> topicIgnoreSchema = new HashSet<>(
          config.getList(ElasticsearchSinkConnectorConfig.TOPIC_SCHEMA_IGNORE_CONFIG)
      );

      long flushTimeoutMs =
          config.getLong(ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG);
      int maxBufferedRecords =
          config.getInt(ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG);
      int batchSize =
          config.getInt(ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG);
      long lingerMs =
          config.getLong(ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG);
      int maxInFlightRequests =
          config.getInt(ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG);
      long retryBackoffMs =
          config.getLong(ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG);
      int maxRetry =
          config.getInt(ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG);
      boolean dropInvalidMessage =
          config.getBoolean(ElasticsearchSinkConnectorConfig.DROP_INVALID_MESSAGE_CONFIG);
      boolean createIndicesAtStartTime =
          config.getBoolean(ElasticsearchSinkConnectorConfig.AUTO_CREATE_INDICES_AT_START_CONFIG);

      DataConverter.BehaviorOnNullValues behaviorOnNullValues =
          DataConverter.BehaviorOnNullValues.forValue(
              config.getString(ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG)
          );

      BulkProcessor.BehaviorOnMalformedDoc behaviorOnMalformedDoc =
          BulkProcessor.BehaviorOnMalformedDoc.forValue(
              config.getString(ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG)
          );

      // Calculate the maximum possible backoff time ...
      long maxRetryBackoffMs =
          RetryUtil.computeRetryWaitTimeInMillis(maxRetry, retryBackoffMs);
      if (maxRetryBackoffMs > RetryUtil.MAX_RETRY_TIME_MS) {
        log.warn("This connector uses exponential backoff with jitter for retries, "
                + "and using '{}={}' and '{}={}' results in an impractical but possible maximum "
                + "backoff time greater than {} hours.",
            ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG, maxRetry,
            ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs,
            TimeUnit.MILLISECONDS.toHours(maxRetryBackoffMs));
      }

      if (client != null) {
        this.client = client;
      } else {
        this.client = new JestElasticsearchClient(props);
      }

      ElasticsearchWriter.Builder builder = new ElasticsearchWriter.Builder(this.client)
          .setType(type)
          .setIgnoreKey(ignoreKey, topicIgnoreKey)
          .setIgnoreVersion(ignoreVersion)
          .setIgnoreSchema(ignoreSchema, topicIgnoreSchema)
          .setCompactMapEntries(useCompactMapEntries)
          .setTopicToIndexMap(topicToIndexMap)
          .setFlushTimoutMs(flushTimeoutMs)
          .setMaxBufferedRecords(maxBufferedRecords)
          .setMaxInFlightRequests(maxInFlightRequests)
          .setBatchSize(batchSize)
          .setLingerMs(lingerMs)
          .setRetryBackoffMs(retryBackoffMs)
          .setMaxRetry(maxRetry)
          .setDropInvalidMessage(dropInvalidMessage)
          .setBehaviorOnNullValues(behaviorOnNullValues)
          .setBehaviorOnMalformedDoc(behaviorOnMalformedDoc);

      this.createIndicesAtStartTime = createIndicesAtStartTime;

      writer = builder.build();
      writer.start();
      log.info(
          "Started ElasticsearchSinkTask, will {} records with null values ('{}')",
          behaviorOnNullValues,
          ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG
      );
    } catch (ConfigException e) {
      throw new ConnectException(
          "Couldn't start ElasticsearchSinkTask due to configuration error:",
          e
      );
    }
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    log.debug("Opening the task for topic partitions: {}", partitions);
    if (createIndicesAtStartTime) {
      Set<String> topics = new HashSet<>();
      for (TopicPartition tp : partitions) {
        topics.add(tp.topic());
      }
      writer.createIndicesForTopics(topics);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    log.debug("Putting {} records to Elasticsearch", records.size());
    writer.write(records);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    log.debug("Flushing data to Elasticsearch with the following offsets: {}", offsets);
    writer.flush();
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    log.debug("Closing the task for topic partitions: {}", partitions);
  }

  @Override
  public void stop() throws ConnectException {
    log.info("Stopping ElasticsearchSinkTask");
    if (writer != null) {
      writer.stop();
    }
    if (client != null) {
      client.close();
    }
  }

  private Map<String, String> parseMapConfig(List<String> values) {
    Map<String, String> map = new HashMap<>();
    for (String value : values) {
      String[] parts = value.split(":");
      String topic = parts[0];
      String type = parts[1];
      map.put(topic, type);
    }
    return map;
  }

}
