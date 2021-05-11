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

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnNullValues;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.DocWriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ElasticsearchSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);

  private DataConverter converter;
  private ElasticsearchClient client;
  private ElasticsearchSinkConnectorConfig config;
  private ErrantRecordReporter reporter;
  private Set<String> existingMappings;
  private Set<String> indexCache;

  @Override
  public void start(Map<String, String> props) {
    start(props, null);
  }

  // visible for testing
  protected void start(Map<String, String> props, ElasticsearchClient client) {
    log.info("Starting ElasticsearchSinkTask.");

    this.config = new ElasticsearchSinkConnectorConfig(props);
    this.converter = new DataConverter(config);
    this.existingMappings = new HashSet<>();
    this.indexCache = new HashSet<>();

    this.reporter = null;
    try {
      if (context.errantRecordReporter() == null) {
        log.info("Errant record reporter not configured.");
      }

      // may be null if DLQ not enabled
      reporter = context.errantRecordReporter();
    } catch (NoClassDefFoundError | NoSuchMethodError e) {
      // Will occur in Connect runtimes earlier than 2.6
      log.warn("AK versions prior to 2.6 do not support the errant record reporter.");
    }

    this.client = client != null ? client : new ElasticsearchClient(config, reporter);

    log.info("Started ElasticsearchSinkTask.");
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    log.debug("Putting {} records to Elasticsearch.", records.size());
    for (SinkRecord record : records) {
      if (shouldSkipRecord(record)) {
        logTrace("Ignoring {} with null value.", record);
        reportBadRecord(record, new ConnectException("Cannot write null valued record."));
        continue;
      }

      logTrace("Writing {} to Elasticsearch.", record);

      ensureIndexExists(createIndexName(record.topic()));
      checkMapping(record);
      tryWriteRecord(record);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    log.debug("Flushing data to Elasticsearch with the following offsets: {}", offsets);
    try {
      client.flush();
    } catch (IllegalStateException e) {
      log.debug("Tried to flush data to Elasticsearch, but BulkProcessor is already closed.");
    }
  }

  @Override
  public void stop() {
    log.debug("Stopping Elasticsearch client.");
    client.close();
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  private void checkMapping(SinkRecord record) {
    String index = createIndexName(record.topic());
    if (!config.shouldIgnoreSchema(record.topic()) && !existingMappings.contains(index)) {
      if (!client.hasMapping(index)) {
        client.createMapping(index, record.valueSchema());
      }
      log.debug("Caching mapping for index '{}' locally.", index);
      existingMappings.add(index);
    }
  }

  /**
   * Returns the converted index name from a given topic name. Elasticsearch accepts:
   * <ul>
   *   <li>all lowercase</li>
   *   <li>less than 256 bytes</li>
   *   <li>does not start with - or _</li>
   *   <li>is not . or ..</li>
   * </ul>
   * (<a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params">ref</a>_.)
   */
  private String convertTopicToIndexName(String topic) {
    String index = topic.toLowerCase();
    if (index.length() > 255) {
      index = index.substring(0, 255);
    }

    if (index.startsWith("-") || index.startsWith("_")) {
      index = index.substring(1);
    }

    if (index.equals(".") || index.equals("..")) {
      index = index.replace(".", "dot");
      log.warn("Elasticsearch cannot have indices named {}. Index will be named {}.", topic, index);
    }

    if (!topic.equals(index)) {
      log.trace("Topic '{}' was translated to index '{}'.", topic, index);
    }

    return index;
  }

  /**
   * Returns the converted index name from a given topic name in the form {type}-{dataset}-{topic}.
   * For the <code>topic</code>, Elasticsearch accepts:
   * <ul>
   *   <li>all lowercase</li>
   *   <li>no longer than 100 bytes</li>
   * </ul>
   * (<a href="https://github.com/elastic/ecs/blob/master/rfcs/text/0009-data_stream-fields.md#restrictions-on-values">ref</a>_.)
   */
  private String convertTopicToDataStreamName(String topic) {
    topic = topic.toLowerCase();
    if (topic.length() > 100) {
      topic = topic.substring(0, 100);
    }
    String dataStream = String.format(
        "%s-%s-%s",
        config.dataStreamType().name().toLowerCase(),
        config.dataStreamDataset(),
        topic
    );
    return dataStream;
  }

  /**
   * Returns the converted index name from a given topic name. If writing to a data stream,
   * returns the index name in the form {type}-{dataset}-{topic}. For both cases, Elasticsearch
   * accepts:
   * <ul>
   *   <li>all lowercase</li>
   *   <li>less than 256 bytes</li>
   *   <li>does not start with - or _</li>
   *   <li>is not . or ..</li>
   * </ul>
   * (<a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params">ref</a>_.)
   */
  private String createIndexName(String topic) {
    return config.isDataStream()
        ? convertTopicToDataStreamName(topic)
        : convertTopicToIndexName(topic);
  }

  private void ensureIndexExists(String index) {
    if (!indexCache.contains(index)) {
      log.info("Creating index {}.", index);
      client.createIndexOrDataStream(index);
      indexCache.add(index);
    }
  }

  private void logTrace(String formatMsg, SinkRecord record) {
    if (log.isTraceEnabled()) {
      log.trace(formatMsg, recordString(record));
    }
  }

  private void reportBadRecord(SinkRecord record, Throwable error) {
    if (reporter != null) {
      reporter.report(record, error);
    }
  }

  private boolean shouldSkipRecord(SinkRecord record) {
    return record.value() == null && config.behaviorOnNullValues() == BehaviorOnNullValues.IGNORE;
  }

  private void tryWriteRecord(SinkRecord sinkRecord) {
    DocWriteRequest<?> record = null;
    try {
      record = converter.convertRecord(sinkRecord, createIndexName(sinkRecord.topic()));
    } catch (DataException convertException) {
      reportBadRecord(sinkRecord, convertException);

      if (config.dropInvalidMessage()) {
        log.error("Can't convert {}.", recordString(sinkRecord), convertException);
      } else {
        throw convertException;
      }
    }

    if (record != null) {
      log.trace("Adding {} to bulk processor.", recordString(sinkRecord));
      client.index(sinkRecord, record);
    }
  }

  private static String recordString(SinkRecord record) {
    return String.format(
        "record from topic=%s partition=%s offset=%s",
        record.topic(),
        record.kafkaPartition(),
        record.kafkaOffset()
    );
  }
}
