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

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnNullValues;

public class ElasticsearchSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);

  private DataConverter converter;
  private ElasticsearchClient client;
  private ElasticsearchSinkConnectorConfig config;
  private ErrantRecordReporter reporter;
  private Set<String> existingMappings;
  private Set<String> indexCache;
  private OffsetTracker offsetTracker;

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
    this.offsetTracker = new OffsetTracker();

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

    log.info("Started ElasticsearchSinkTask. Connecting to ES server version: {}",
        getServerVersion());
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    log.debug("Putting {} records to Elasticsearch.", records.size());
    for (SinkRecord record : records) {
      OffsetTracker.Offset offset = offsetTracker.addPendingRecord(record);

      if (shouldSkipRecord(record)) {
        logTrace("Ignoring {} with null value.", record);
        offset.markProcessed();
        reportBadRecord(record, new ConnectException("Cannot write null valued record."));
        continue;
      }

      logTrace("Writing {} to Elasticsearch.", record);

      ensureIndexExists(convertTopicToIndexName(record.topic()));
      checkMapping(record);
      tryWriteRecord(record, offset);
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition,
          OffsetAndMetadata> currentOffsets) {
    return offsetTracker.getAndResetOffsets();
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
    String index = convertTopicToIndexName(record.topic());
    if (!config.shouldIgnoreSchema(record.topic()) && !existingMappings.contains(index)) {
      if (!client.hasMapping(index)) {
        client.createMapping(index, record.valueSchema());
      }
      log.debug("Caching mapping for index '{}' locally.", index);
      existingMappings.add(index);
    }
  }

  private String getServerVersion() {
    ConfigCallbackHandler configCallbackHandler = new ConfigCallbackHandler(config);
    RestHighLevelClient highLevelClient = new RestHighLevelClient(
        RestClient
            .builder(
                config.connectionUrls()
                    .stream()
                    .map(HttpHost::create)
                    .collect(Collectors.toList())
                    .toArray(new HttpHost[config.connectionUrls().size()])
            )
            .setHttpClientConfigCallback(configCallbackHandler)
    );
    MainResponse response;
    String esVersionNumber = "Unknown";
    try {
      response = highLevelClient.info(RequestOptions.DEFAULT);
      esVersionNumber = response.getVersion().getNumber();
    } catch (Exception e) {
      // Same error messages as from validating the connection for IOException.
      // Insufficient privileges to validate the version number if caught
      // ElasticsearchStatusException.
      log.warn("Failed to get ES server version", e);
    } finally {
      try {
        highLevelClient.close();
      } catch (Exception e) {
        log.warn("Failed to close high level client", e);
      }
    }
    return esVersionNumber;
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

  private void ensureIndexExists(String index) {
    if (!indexCache.contains(index)) {
      log.info("Creating index {}.", index);
      client.createIndex(index);
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

  private void tryWriteRecord(SinkRecord sinkRecord, OffsetTracker.Offset offset) {
    DocWriteRequest<?> record = null;
    try {
      record = converter.convertRecord(sinkRecord, convertTopicToIndexName(sinkRecord.topic()));
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
      client.index(sinkRecord, record, offset);
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
