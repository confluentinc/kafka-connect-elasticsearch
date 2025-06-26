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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.elasticsearch.action.DocWriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnNullValues;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class ElasticsearchSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);

  private DataConverter converter;
  private ElasticsearchClient client;
  private ElasticsearchSinkTaskConfig config;
  private ErrantRecordReporter reporter;
  private Set<String> existingMappings;
  private Set<String> indexCache;
  private Map<String, String> topicToResourceMap;
  private OffsetTracker offsetTracker;
  private PartitionPauser partitionPauser;

  @Override
  public void start(Map<String, String> props) {
    start(props, null);
  }

  // visible for testing
  protected void start(Map<String, String> props, ElasticsearchClient client) {
    log.info("Starting ElasticsearchSinkTask.");

    this.config = new ElasticsearchSinkTaskConfig(props);
    this.converter = new DataConverter(config);
    this.existingMappings = new HashSet<>();
    this.indexCache = new HashSet<>();

    // Initialize topic to resource mapping cache
    if (config.isExternalResourceUsageEnabled()) {
      try {
        this.topicToResourceMap = config.getTopicToExternalResourceMap();
      } catch (ConfigException e) {
        throw new ConnectException("Failed to parse topic-to-resource mappings: "
                + e.getMessage(), e);
      }
    }

    int offsetHighWaterMark = config.maxBufferedRecords() * 10;
    int offsetLowWaterMark = config.maxBufferedRecords() * 5;
    this.partitionPauser = new PartitionPauser(context,
        () -> offsetTracker.numOffsetStateEntries() > offsetHighWaterMark,
        () -> offsetTracker.numOffsetStateEntries() <= offsetLowWaterMark);
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
    Runnable afterBulkCallback = () -> offsetTracker.updateOffsets();
    this.client = client != null ? client
        : new ElasticsearchClient(config, reporter, afterBulkCallback,
            config.getTaskId(), config.getConnectorName());

    if (!config.flushSynchronously()) {
      this.offsetTracker = new AsyncOffsetTracker(context);
    } else {
      this.offsetTracker = new SyncOffsetTracker(this.client);
    }

    log.info("Started ElasticsearchSinkTask. Connecting to ES server version: {}",
        this.client.version());
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    log.debug("Putting {} records to Elasticsearch.", records.size());
    client.throwIfFailed();
    partitionPauser.maybeResumePartitions();

    for (SinkRecord record : records) {
      OffsetState offsetState = offsetTracker.addPendingRecord(record);
      if (shouldSkipRecord(record)) {
        logTrace("Ignoring {} with null value.", record);
        offsetState.markProcessed();
        reportBadRecord(record, new ConnectException("Cannot write null valued record."));
        continue;
      }
      logTrace("Writing {} to Elasticsearch.", record);

      tryWriteRecord(record, offsetState);
    }
    partitionPauser.maybePausePartitions();
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    try {
      // This will just trigger an asynchronous execution of any buffered records
      client.flush();
    } catch (IllegalStateException e) {
      log.debug("Tried to flush data to Elasticsearch, but BulkProcessor is already closed.", e);
    }
    Map<TopicPartition, OffsetAndMetadata> offsets = offsetTracker.offsets(currentOffsets);
    log.debug("preCommitting offsets {}", offsets);
    return offsets;
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

  private void checkMapping(String resourceName, SinkRecord record) {
    if (!config.shouldIgnoreSchema(record.topic()) && !existingMappings.contains(resourceName)) {
      if (!client.hasMapping(resourceName)) {
        client.createMapping(resourceName, record.valueSchema());
      }
      log.debug("Caching mapping for resource '{}' locally.", resourceName);
      existingMappings.add(resourceName);
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
   * Returns the converted datastream name from a given topic name in the form:
   * {type}-{dataset}-{namespace}
   * For the <code>namespace</code> (that can contain topic), Elasticsearch accepts:
   * <ul>
   *   <li>all lowercase</li>
   *   <li>no longer than 100 bytes</li>
   * </ul>
   * (<a href="https://github.com/elastic/ecs/blob/master/rfcs/text/0009-data_stream-fields.md#restrictions-on-values">ref</a>_.)
   */
  private String convertTopicToDataStreamName(String topic) {
    String namespace = config.dataStreamNamespace();
    namespace = namespace.replace("${topic}", topic.toLowerCase());
    if (namespace.length() > 100) {
      namespace = namespace.substring(0, 100);
    }
    String dataStream = String.format(
        "%s-%s-%s",
        config.dataStreamType().toLowerCase(),
        config.dataStreamDataset(),
        namespace
    );
    return dataStream;
  }

  /**
   * Returns the converted index name from a given topic name. If writing to a data stream,
   * returns the index name in the form {type}-{dataset}-{namespace}. For both cases, Elasticsearch
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
      // No need to wait for the futures (synchronously or async), the framework will wait for
      // all these futures before calling preCommit
      reporter.report(record, error);
    }
  }

  private boolean shouldSkipRecord(SinkRecord record) {
    return record.value() == null && config.behaviorOnNullValues() == BehaviorOnNullValues.IGNORE;
  }

  private void tryWriteRecord(SinkRecord sinkRecord, OffsetState offsetState) {
    String resourceName;
    if (!config.isExternalResourceUsageEnabled()) {
      resourceName = createIndexName(sinkRecord.topic());
      ensureIndexExists(resourceName);
    } else {
      if (topicToResourceMap.containsKey(sinkRecord.topic())) {
        resourceName = topicToResourceMap.get(sinkRecord.topic());
      } else {
        throw new ConnectException(String.format(
                "Topic '%s' is not mapped to any resource. "
                + "All topics must be mapped when using topic-to-resource mapping configuration. "
                + "Please check the '%s' configuration to ensure all topics are properly mapped.",
                sinkRecord.topic(),
                ElasticsearchSinkConnectorConfig.TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG
        ));
      }
    }
    
    checkMapping(resourceName, sinkRecord);

    DocWriteRequest<?> docWriteRequest = null;
    try {
      docWriteRequest = converter.convertRecord(sinkRecord, resourceName);
    } catch (DataException convertException) {
      reportBadRecord(sinkRecord, convertException);

      if (config.dropInvalidMessage()) {
        log.error("Can't convert {}.", recordString(sinkRecord), convertException);
        offsetState.markProcessed();
      } else {
        throw convertException;
      }
    }

    if (docWriteRequest != null) {
      logTrace("Adding {} to bulk processor.", sinkRecord);
      client.index(sinkRecord, docWriteRequest, offsetState);
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

  @Override
  public void close(Collection<TopicPartition> partitions) {
    offsetTracker.closePartitions(partitions);
  }

  // Visible for testing
  static class PartitionPauser {

    // Kafka consumer poll timeout to set when partitions are paused, to avoid waiting for a long
    // time (default poll timeout) to resume it.
    private static final long PAUSE_POLL_TIMEOUT_MS = 100;

    private final SinkTaskContext context;
    private final BooleanSupplier pauseCondition;
    private final BooleanSupplier resumeCondition;
    private boolean partitionsPaused;

    public PartitionPauser(SinkTaskContext context,
                           BooleanSupplier pauseCondition,
                           BooleanSupplier resumeCondition) {
      this.context = context;
      this.pauseCondition = pauseCondition;
      this.resumeCondition = resumeCondition;
    }

    /**
     * Resume partitions if they are paused and resume condition is met.
     * Has to be run in the task thread.
     */
    void maybeResumePartitions() {
      if (partitionsPaused) {
        if (resumeCondition.getAsBoolean()) {
          log.debug("Resuming all partitions");
          context.resume(context.assignment().toArray(new TopicPartition[0]));
          partitionsPaused = false;
        } else {
          context.timeout(PAUSE_POLL_TIMEOUT_MS);
        }
      }
    }

    /**
     * Pause partitions if they are not paused and pause condition is met.
     * Has to be run in the task thread.
     */
    void maybePausePartitions() {
      if (!partitionsPaused && pauseCondition.getAsBoolean()) {
        log.debug("Pausing all partitions");
        context.pause(context.assignment().toArray(new TopicPartition[0]));
        context.timeout(PAUSE_POLL_TIMEOUT_MS);
        partitionsPaused = true;
      }
    }
  }
}
