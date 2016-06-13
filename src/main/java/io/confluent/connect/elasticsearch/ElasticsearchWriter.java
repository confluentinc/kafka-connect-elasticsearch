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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.MapperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * The ElasticsearchWriter handles connections to Elasticsearch, sending data and flush.
 * Transport client is used to send requests to Elasticsearch cluster. Requests are batched
 * when sending to Elasticsearch. To ensure delivery guarantee and order, we retry in case of
 * failures for a batch.
 *
 * Currently, we only send out requests to Elasticsearch when flush is called, which is not
 * desirable from the latency point of view.
 *
 * TODO: Replace the Elasticsearch BulkProcessor with our own processor to handle batching and retry.
 *
 * TODO: Use REST instead of transport client.
 *
 * TODO: Use offset as external version to fence requests with lower version.
 */
public class ElasticsearchWriter {
  private static final Logger log = LoggerFactory.getLogger(ElasticsearchWriter.class);

  private final Converter converter;

  private final Client client;
  private final BulkProcessor bulkProcessor;
  private static final int CONCURRENT_REQUESTS = 1;
  private final Semaphore semaphore = new Semaphore(CONCURRENT_REQUESTS);
  private final Queue<SinkRecord> buffer;
  private BulkRequest currentBulkRequest;

  private final String type;
  private final boolean ignoreKey;
  private final boolean ignoreSchema;
  private final Map<String, TopicConfig> topicConfigs;
  private final long flushTimeoutMs;
  private final long maxBufferedRecords;
  private final long batchSize;

  private final SinkTaskContext context;
  private final Set<TopicPartition> assignment;
  private static final Class<? extends Throwable> NON_RETRIABLE_EXCEPTION_CLASS = MapperException.class;
  private boolean canRetry;
  private final Set<String> mappings;

  /**
   * ElasticsearchWriter constructor
   * @param client The client to connect to Elasticsearch.
   * @param type The type to use when writing to Elasticsearch.
   * @param ignoreKey Whether to ignore key during indexing.
   * @param ignoreSchema Whether to ignore schema during indexing.
   * @param topicConfigs The map of per topic configs.
   * @param flushTimeoutMs The flush timeout.
   * @param maxBufferedRecords The max number of buffered records.
   * @param batchSize Approximately the max number of records each writer will buffer.
   * @param context The SinkTaskContext.
   * @param mock Whether to use mock Elasticsearch client.
   */
  ElasticsearchWriter(
      Client client,
      String type,
      boolean ignoreKey,
      boolean ignoreSchema,
      Map<String, TopicConfig> topicConfigs,
      long flushTimeoutMs,
      long maxBufferedRecords,
      long batchSize,
      SinkTaskContext context,
      Converter converter,
      boolean mock) {

    this.client = client;
    this.type = type;
    this.ignoreKey = ignoreKey;
    this.ignoreSchema = ignoreSchema;

    if (topicConfigs == null) {
      this.topicConfigs = new HashMap<>();
    } else {
      this.topicConfigs = topicConfigs;
    }

    this.flushTimeoutMs = flushTimeoutMs;
    this.maxBufferedRecords  = maxBufferedRecords;
    this.batchSize = batchSize;

    this.context = context;
    this.assignment = context.assignment();
    this.canRetry = true;

    this.currentBulkRequest = new BulkRequest();

    // create index if needed.
    if (!mock) {
      createIndices(topicConfigs);
    }

    // create the buffer
    buffer = new LinkedList<>();

    // Config the JsonConverter
    this.converter = converter;

    // Create the bulkProcessor
    bulkProcessor = BulkProcessor.builder(
        client,
        new BulkProcessor.Listener() {

          @Override
          public void beforeBulk(long executionId, BulkRequest request) {
            log.debug("Before executing the request: {}", request);
            try {
              semaphore.acquire();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }

          @Override
          public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (!response.hasFailures()) {
              log.debug("Finished request with no failures.");
              currentBulkRequest = new BulkRequest();
            } else {
              log.error("Failure:" + response.buildFailureMessage());
              for (BulkItemResponse bulkItemResponse : response.getItems()) {
                if (bulkItemResponse.isFailed()) {
                  Throwable cause = bulkItemResponse.getFailure().getCause();
                  Throwable rootCause = ExceptionsHelper.unwrapCause(cause);
                  if (NON_RETRIABLE_EXCEPTION_CLASS.isAssignableFrom(rootCause.getClass())) {
                    canRetry = false;
                    break;
                  }
                }
              }
            }
            semaphore.release();
          }

          @Override
          public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            log.error("Failure:", failure);
            if (NON_RETRIABLE_EXCEPTION_CLASS.isAssignableFrom(failure.getClass())) {
              canRetry = false;
            }
            semaphore.release();
          }
        })
        .setBulkActions(-1)
        .setBulkSize(new ByteSizeValue(-1))
        .setFlushInterval(null)
        .setConcurrentRequests(CONCURRENT_REQUESTS)
        .setBackoffPolicy(BackoffPolicy.noBackoff())
        .build();

    //Create mapping cache
    mappings = new HashSet<>();
  }

  public static class Builder {

    private final Client client;
    private String type;
    private boolean ignoreKey = false;
    private boolean ignoreSchema = false;
    private Map<String, TopicConfig> topicConfigs = new HashMap<>();
    private long flushTimeoutMs;
    private long maxBufferedRecords;
    private long batchSize;
    private SinkTaskContext context;
    private Converter converter = ElasticsearchSinkTask.getConverter();
    private boolean mock;

    /**
     * Constructor of ElasticsearchWriter Builder.
     * @param client The client to connect to Elasticsearch.
     */
    public Builder(Client client) {
      this.client = client;
    }

    /**
     * Set the index.
     * @param type The type to use for each index.
     * @return an instance of ElasticsearchWriter Builder.
     */
    public Builder setType(String type) {
      this.type = type;
      return this;
    }

    /**
     * Set whether to ignore key during indexing.
     * @param ignoreKey Whether to ignore key.
     * @return an instance of ElasticsearchWriter Builder.
     */

    public Builder setIgnoreKey(boolean ignoreKey) {
      this.ignoreKey = ignoreKey;
      return this;
    }

    /**
     * Set whether to ignore schema during indexing.
     * @param ignoreSchema Whether to ignore key.
     * @return an instance of ElasticsearchWriter Builder.
     */
    public Builder setIgnoreSchema(boolean ignoreSchema) {
      this.ignoreSchema = ignoreSchema;
      return this;
    }

    /**
     * Set per topic configurations.
     * @param topicConfigs The map of per topic configuration.
     * @return an instance of ElasticsearchWriter Builder.
     */
    public Builder setTopicConfigs(Map<String, TopicConfig> topicConfigs) {
      this.topicConfigs = topicConfigs;
      return this;
    }

    /**
     * Set the flush timeout.
     * @param flushTimeoutMs The flush timeout in milliseconds.
     * @return an instance of ElasticsearchWriter Builder.
     */
    public Builder setFlushTimoutMs(long flushTimeoutMs) {
      this.flushTimeoutMs = flushTimeoutMs;
      return this;
    }

    /**
     * Set the max number of records to buffer for each writer.
     * @param maxBufferedRecords The max number of buffered record.s
     * @return an instance of ElasticsearchWriter Builder.
     */
    public Builder setMaxBufferedRecords(long maxBufferedRecords) {
      this.maxBufferedRecords = maxBufferedRecords;
      return this;
    }

    /**
     * Set the number of requests to process as a batch when writing
     * to Elasticsearch.
     * @param batchSize the size of each batch.
     * @return an instance of ElasticsearchWriter Builder.
     */
    public Builder setBatchSize(long batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Set the SinkTaskContext
     * @param context The SinkTaskContext.
     * @return an instance of ElasticsearchWriter Builder.
     */
    public Builder setContext(SinkTaskContext context) {
      this.context = context;
      return this;
    }

    /**
     * Set the converter.
     * @param converter The converter to use.
     * @return an instance of ElasticsearchWriter Builder.
     */
    public Builder setConverter(Converter converter) {
      this.converter = converter;
      return this;
    }

    /**
     * Set whether to use the mock client to connect to Elasticsearch.
     * @param mock Whether to use mock client.
     * @return an instance of ElasticsearchWriter Builder.
     */
    public Builder setMock(boolean mock) {
      this.mock = mock;
      return this;
    }

    /**
     * Build the ElasticsearchWriter.
     * @return an instance of ElasticsearchWriter.
     */
    public ElasticsearchWriter build() {
      return new ElasticsearchWriter(
          client, type, ignoreKey, ignoreSchema, topicConfigs, flushTimeoutMs, maxBufferedRecords, batchSize, context, converter, mock);
    }
  }

  public void write(Collection<SinkRecord> records) {

    for (SinkRecord record: records) {
      buffer.add(record);
    }

    if (buffer.size() > maxBufferedRecords) {
      for (TopicPartition tp: assignment) {
        context.pause(tp);
      }
    }

    if (!currentBulkRequest.requests().isEmpty()) {
      log.debug("We need to retry {}", currentBulkRequest);
      return;
    }

    Iterator<SinkRecord> iter = buffer.iterator();
    int size = 0;
    while (iter.hasNext() && size < batchSize) {
      size++;
      SinkRecord record = iter.next();
      iter.remove();
      IndexRequest request = DataConverter.convertRecord(record, type, client, converter, ignoreKey, ignoreSchema, topicConfigs, mappings);
      currentBulkRequest.add(request);
    }
  }

  // TODO: fix the logic here. Currently we did not properly handle the case that the data is in the
  // buffer but not yet flushed.
  public void flush() {
    for (ActionRequest request: currentBulkRequest.requests()) {
      bulkProcessor.add(request);
    }
    bulkProcessor.flush();
    try {
      if (semaphore.tryAcquire(CONCURRENT_REQUESTS, flushTimeoutMs, TimeUnit.MILLISECONDS)) {
        log.info("Bulk request finished.");
        if (buffer.size() < maxBufferedRecords) {
          for (TopicPartition tp: assignment) {
            context.resume(tp);
          }
        }
        semaphore.release();
        if (!canRetry) {
          throw new ConnectException("Cannot continue execution.");
        }
      } else {
        // TODO: we want to cancel the current bulk request before submitting the next one
        // Currently, the bulkProcessor thread may be blocked when retry
        log.error("Not able to finish flushing before timeout:" + flushTimeoutMs);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void close() {
    try {
      bulkProcessor.awaitClose(flushTimeoutMs, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private boolean indexExists(String index) {
    return client.admin().indices().prepareExists(index).execute().actionGet().isExists();
  }


  private void createIndices(Map<String, TopicConfig> topicConfigs) {
    Set<TopicPartition> assignment = context.assignment();
    Set<String> topics = new HashSet<>();
    for (TopicPartition tp: assignment) {
      String topic = tp.topic();
      if (!topicConfigs.containsKey(topic)) {
        topics.add(topic);
      }
    }

    Set<String> indices = new HashSet<>(topics);
    for (String topic: topicConfigs.keySet()) {
      indices.add(topicConfigs.get(topic).getIndex());
    }

    for (String index: indices) {
      if (!indexExists(index)) {
        CreateIndexResponse createIndexResponse = client.admin().indices()
            .prepareCreate(index)
            .execute()
            .actionGet();

        if (!createIndexResponse.isAcknowledged()) {
          throw new ConnectException("Could not create index:" + index);
        }
      }
    }
  }

  // public for testing
  public BulkRequest getCurrentBulkRequest() {
    return currentBulkRequest;
  }
}
