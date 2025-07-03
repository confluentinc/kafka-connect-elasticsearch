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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;

import org.apache.http.HttpHost;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.client.indices.CreateDataStreamRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.VersionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnMalformedDoc;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static java.util.stream.Collectors.toList;

/**
 * Based on Elasticsearch's BulkProcessor, which is responsible for building batches based on size
 * and linger time (not grouped by partitions) and limiting the concurrency (max number of
 * in-flight requests).
 *
 * <p>Batch processing is asynchronous. BulkProcessor delegates the bulk calls to a separate thread
 * pool. Retries are handled synchronously in each batch thread.
 *
 * <p>If all the retries fail, the exception is reported via an atomic reference to an error,
 * which is checked and thrown from a subsequent call to the task's put method and that results
 * in failure of the task.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class ElasticsearchClient {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchClient.class);

  private static final long WAIT_TIME_MS = 10;
  private static final long CLOSE_WAIT_TIME_MS = 5_000;
  private static final String RESOURCE_ALREADY_EXISTS_EXCEPTION =
      "resource_already_exists_exception";
  private static final String VERSION_CONFLICT_EXCEPTION = "version_conflict_engine_exception";
  private static final Set<String> MALFORMED_DOC_ERRORS = new HashSet<>(
      Arrays.asList(
          "strict_dynamic_mapping_exception",
          "mapper_parsing_exception",
          "illegal_argument_exception",
          "action_request_validation_exception",
          "document_parsing_exception"
      )
  );
  private static final String UNKNOWN_VERSION_TAG = "Unknown";
  protected final AtomicInteger numBufferedRecords;
  private final AtomicReference<ConnectException> error;
  protected final BulkProcessor bulkProcessor;
  private final ConcurrentMap<DocWriteRequest<?>, SinkRecordAndOffset> requestToSinkRecord;
  private final ConcurrentMap<Long, List<SinkRecordAndOffset>> inFlightRequests;
  private final ElasticsearchSinkConnectorConfig config;
  private final ErrantRecordReporter reporter;
  private final RestHighLevelClient client;
  private final ExecutorService bulkExecutorService;
  private final Time clock;
  private final Lock inFlightRequestLock = new ReentrantLock();
  private final Condition inFlightRequestsUpdated = inFlightRequestLock.newCondition();
  private final String esVersion;

  @SuppressWarnings("deprecation")
  public ElasticsearchClient(
      ElasticsearchSinkConnectorConfig config,
      ErrantRecordReporter reporter,
      Runnable afterBulkCallback,
      int taskId,
      String connectorName
  ) {
    this.bulkExecutorService = Executors.newFixedThreadPool(config.maxInFlightRequests(),
      new ThreadFactory() {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        @Override
        public Thread newThread(Runnable r) {
          Thread thread = Executors.defaultThreadFactory().newThread(r);
          thread.setName(connectorName + "-" + taskId + "-elasticsearch-bulk-executor-"
                  + threadNumber.getAndIncrement());
          return thread;
        }
      });
    this.numBufferedRecords = new AtomicInteger(0);
    this.error = new AtomicReference<>();
    this.requestToSinkRecord = new ConcurrentHashMap<>();
    this.inFlightRequests = reporter != null ? new ConcurrentHashMap<>() : null;
    this.config = config;
    this.reporter = reporter;
    this.clock = Time.SYSTEM;

    ConfigCallbackHandler configCallbackHandler = new ConfigCallbackHandler(config);
    RestClient client = RestClient
        .builder(
            config.connectionUrls()
                .stream()
                .map(HttpHost::create)
                .collect(toList())
                .toArray(new HttpHost[config.connectionUrls().size()])
        ).setHttpClientConfigCallback(configCallbackHandler).build();

    esVersion = getServerVersion(client);

    RestHighLevelClientBuilder clientBuilder = new RestHighLevelClientBuilder(client);

    if (shouldSetCompatibilityToES8()) {
      log.info("Staring client in ES 8 compatibility mode");
      clientBuilder.setApiCompatibilityMode(true);
    }

    this.client = clientBuilder.build();

    this.bulkProcessor = BulkProcessor
        .builder(buildConsumer(), buildListener(afterBulkCallback))
        .setBulkActions(config.batchSize())
        .setBulkSize(config.bulkSize())
        .setConcurrentRequests(config.maxInFlightRequests() - 1) // 0 = no concurrent requests
        .setFlushInterval(TimeValue.timeValueMillis(config.lingerMs()))
        // Disabling bulk processor retries, because they only cover a small subset of errors
        // (see https://github.com/elastic/elasticsearch/issues/71159)
        // We are doing retries in the async thread instead.
        .setBackoffPolicy(BackoffPolicy.noBackoff())
        .build();
  }

  /**
   * Elastic High level Rest Client 7.17 has a compatibility mode to support ES 8. Checks the
   * version number of ES to determine if we should be running in compatibility mode while using
   * HLRC 7.17 to talk to ES.
   */
  private boolean shouldSetCompatibilityToES8() {
    return !version().equals(UNKNOWN_VERSION_TAG)
        && Integer.parseInt(version().split("\\.")[0]) >= 8;
  }

  private String getServerVersion(RestClient client) {
    RestHighLevelClient highLevelClient = new RestHighLevelClientBuilder(client).build();
    MainResponse response;
    String esVersionNumber = UNKNOWN_VERSION_TAG;
    try {
      response = highLevelClient.info(RequestOptions.DEFAULT);
      esVersionNumber = response.getVersion().getNumber();
    } catch (Exception e) {
      // Same error messages as from validating the connection for IOException.
      // Insufficient privileges to validate the version number if caught
      // ElasticsearchStatusException.
      log.warn("Failed to get ES server version", e);
    }
    return esVersionNumber;
  }

  private BiConsumer<BulkRequest, ActionListener<BulkResponse>> buildConsumer() {
    return (req, lis) ->
      // Executes a synchronous bulk request in a background thread, with synchronous retries.
      // We don't use bulkAsync because we can't retry from its callback (see
      // https://github.com/confluentinc/kafka-connect-elasticsearch/pull/575)
      // BulkProcessor is the one guaranteeing that no more than maxInFlightRequests batches
      // are started at the same time (a new consumer is not called until all others are finished),
      // which means we don't need to limit the executor pending task queue.

      // Result is ignored because everything is reported via the corresponding ActionListener.
      bulkExecutorService.submit(() -> {
        try {
          BulkResponse bulkResponse = callWithRetries(
              "execute bulk request",
              () -> client.bulk(req, RequestOptions.DEFAULT)
          );
          lis.onResponse(bulkResponse);
        } catch (Exception ex) {
          lis.onFailure(ex);
        } catch (Throwable ex) {
          lis.onFailure(new ConnectException("Bulk request failed", ex));
        }
      });
  }

  /**
   * Returns the underlying Elasticsearch client.
   *
   * @return the underlying RestHighLevelClient
   */
  public RestHighLevelClient client() {
    return client;
  }

  /**
   * Closes the ElasticsearchClient.
   *
   * @throws ConnectException if all the records fail to flush before the timeout.
   */
  public void close() {
    try {
      if (!bulkProcessor.awaitClose(config.flushTimeoutMs(), TimeUnit.MILLISECONDS)) {
        throw new ConnectException(
            "Failed to process outstanding requests in time while closing the ElasticsearchClient."
        );
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException(
          "Interrupted while processing all in-flight requests on ElasticsearchClient close.", e
      );
    } finally {
      closeResources();
    }
  }

  /**
   * Creates an index or data stream. Will not recreate the index or data stream if
   * it already exists. Will create a data stream instead of an index if the data stream
   * configurations are set.
   *
   * @param name the name of the index or data stream to create
   * @return true if the index or data stream was created, false if it already exists
   */
  public boolean createIndexOrDataStream(String name) {
    if (indexExists(name)) {
      return false;
    }
    return config.isDataStream() ? createDataStream(name) : createIndex(name);
  }

  /**
   * Creates a mapping for the given index and schema.
   *
   * @param resourceName the resource to create the mapping for
   * @param schema the schema to map
   */
  public void createMapping(String resourceName, Schema schema) {
    PutMappingRequest request = new PutMappingRequest(resourceName)
            .source(Mapping.buildMapping(schema));
    callWithRetries(
        String.format("create mapping for resource %s with schema %s", resourceName, schema),
        () -> client.indices().putMapping(request, RequestOptions.DEFAULT)
    );
  }

  public String version() {
    return esVersion;
  }

  /**
   * Triggers a flush of any buffered records.
   */
  public void flush() {
    bulkProcessor.flush();
  }

  public void waitForInFlightRequests() {
    inFlightRequestLock.lock();
    try {
      while (numBufferedRecords.get() > 0) {
        inFlightRequestsUpdated.await();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException(e);
    } finally {
      inFlightRequestLock.unlock();
    }
  }

  /**
   * Checks whether the index already has a mapping or not.
   * @param resourceName the resource to check
   * @return true if a mapping exists, false if it does not
   */
  public boolean hasMapping(String resourceName) {
    MappingMetadata mapping = mapping(resourceName);
    return mapping != null && mapping.sourceAsMap() != null && !mapping.sourceAsMap().isEmpty();
  }

  /**
   * Buffers a record to index. Will ensure that there are no concurrent requests for the same
   * document id when either the DLQ is configured or
   * {@link ElasticsearchSinkConnectorConfig#IGNORE_KEY_CONFIG} is set to <code>false</code> because
   * they require the use of a map keyed by document id.
   *
   * <p>This call is usually asynchronous, but can block in any of the following scenarios:
   * <ul>
   *   <li>A new batch is finished (e.g. max batch size has been reached) and
   *    the overall number of threads (max in flight requests) are in use.</li>
   *   <li>The maximum number of buffered records have been reached</li>
   * </ul>
   *
   * @param record the record to index
   * @param request the associated request to send
   * @param offsetState record's offset state
   * @throws ConnectException if one of the requests failed
   */
  public void index(SinkRecord record, DocWriteRequest<?> request, OffsetState offsetState) {
    throwIfFailed();

    // TODO should we just pause partitions instead of blocking and failing the connector?
    verifyNumBufferedRecords();

    requestToSinkRecord.put(request, new SinkRecordAndOffset(record, offsetState));
    numBufferedRecords.incrementAndGet();
    bulkProcessor.add(request);
  }

  public void throwIfFailed() {
    if (isFailed()) {
      try {
        close();
      } catch (ConnectException e) {
        // if close fails, want to still throw the original exception
        log.warn("Couldn't close elasticsearch client", e);
      }
      throw error.get();
    }
  }

  /**
   * Wait for internal buffer to be less than max.buffered.records configuration
    */
  private void verifyNumBufferedRecords() {
    long maxWaitTime = clock.milliseconds() + config.flushTimeoutMs();
    while (numBufferedRecords.get() >= config.maxBufferedRecords()) {
      clock.sleep(WAIT_TIME_MS);
      if (clock.milliseconds() > maxWaitTime) {
        throw new ConnectException(
            String.format("Could not make space in the internal buffer fast enough. "
                            + "Consider increasing %s or %s.",
                    FLUSH_TIMEOUT_MS_CONFIG,
                    MAX_BUFFERED_RECORDS_CONFIG
            )
        );
      }
    }
  }

  private static class SinkRecordAndOffset {

    private final SinkRecord sinkRecord;
    private final OffsetState offsetState;

    public SinkRecordAndOffset(SinkRecord sinkRecord, OffsetState offsetState) {
      this.sinkRecord = sinkRecord;
      this.offsetState = offsetState;
    }
  }

  /**
   * Checks whether the index exists.
   *
   * @param index the index to check
   * @return true if it exists, false if it does not
   */
  public boolean indexExists(String index) {
    GetIndexRequest request = new GetIndexRequest(index);
    return callWithRetries(
        "check if index " + index + " exists",
        () -> client.indices().exists(request, RequestOptions.DEFAULT)
    );
  }

  /**
   * Creates a listener with callback functions to handle completed requests for the BulkProcessor.
   *
   * @return the listener
   */
  private BulkProcessor.Listener buildListener(Runnable afterBulkCallback) {
    return new Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {
        if (inFlightRequests != null) {
          List<SinkRecordAndOffset> sinkRecords = request.requests().stream()
                  .map(requestToSinkRecord::get)
                  .collect(toList());

          inFlightRequests.put(executionId, sinkRecords);
        }
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        List<DocWriteRequest<?>> requests = request.requests();

        int idx = 0;
        for (BulkItemResponse bulkItemResponse : response) {
          DocWriteRequest<?> req = idx < requests.size() ? requests.get(idx) : null;
          boolean failed = handleResponse(bulkItemResponse, req, executionId);
          if (!failed && req != null) {
            requestToSinkRecord.get(req).offsetState.markProcessed();
          }
          idx++;
        }

        afterBulkCallback.run();

        bulkFinished(executionId, request);
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        log.warn("Bulk request {} failed", executionId, failure);
        error.compareAndSet(null, new ConnectException("Bulk request failed", failure));
        bulkFinished(executionId, request);
      }

      private void bulkFinished(long executionId, BulkRequest request) {
        request.requests().forEach(requestToSinkRecord::remove);
        removeFromInFlightRequests(executionId);
        inFlightRequestLock.lock();
        try {
          numBufferedRecords.addAndGet(-request.requests().size());
          inFlightRequestsUpdated.signalAll();
        } finally {
          inFlightRequestLock.unlock();
        }
      }
    };
  }

  /**
   * Calls the specified function with retries and backoffs until the retries are exhausted or the
   * function succeeds.
   *
   * @param description description of the attempted action in present tense
   * @param function the function to call and retry
   * @param <T> the return type of the function
   * @return the return value of the called function
   */
  private <T> T callWithRetries(String description, Callable<T> function) {
    return RetryUtil.callWithRetries(
        description,
        function,
        config.maxRetries() + 1,
        config.retryBackoffMs()
    );
  }

  /**
   * Closes all the connection and thread resources of the client.
   */
  private void closeResources() {
    bulkExecutorService.shutdown();
    try {
      if (!bulkExecutorService.awaitTermination(CLOSE_WAIT_TIME_MS, TimeUnit.MILLISECONDS)) {
        bulkExecutorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      bulkExecutorService.shutdownNow();
      Thread.currentThread().interrupt();
      log.warn("Interrupted while awaiting for executor service shutdown.", e);
    }

    try {
      client.close();
    } catch (IOException e) {
      log.warn("Failed to close Elasticsearch client.", e);
    }
  }

  /**
   * Creates a data stream. Will not recreate the data stream if it already exists.
   *
   * @param dataStream the data stream to create given in the form {type}-{dataset}-{namespace}
   * @return true if the data stream was created, false if it already exists
   */
  private boolean createDataStream(String dataStream) {
    CreateDataStreamRequest request = new CreateDataStreamRequest(dataStream);
    return callWithRetries(
        "create data stream " + dataStream,
        () -> {
          try {
            client.indices().createDataStream(request, RequestOptions.DEFAULT);
          } catch (ElasticsearchStatusException | IOException e) {
            if (!e.getMessage().contains(RESOURCE_ALREADY_EXISTS_EXCEPTION)) {
              throw e;
            }
            return false;
          }
          return true;
        }
    );
  }

  /**
   * Creates an index. Will not recreate the index if it already exists.
   *
   * @param index the index to create
   * @return true if the index was created, false if it already exists
   */
  private boolean createIndex(String index) {
    CreateIndexRequest request = new CreateIndexRequest(index);
    return callWithRetries(
        "create index " + index,
        () -> {
          try {
            client.indices().create(request, RequestOptions.DEFAULT);
          } catch (ElasticsearchStatusException | IOException e) {
            if (!e.getMessage().contains(RESOURCE_ALREADY_EXISTS_EXCEPTION)) {
              throw e;
            }
            return false;
          }
          return true;
        }
    );
  }

  /**
   * Processes a response from a {@link org.elasticsearch.action.bulk.BulkItemRequest}.
   * Successful responses are ignored. Failed responses are reported to the DLQ and handled
   * according to configuration (ignore or fail). Version conflicts are ignored.
   *
   * @param response    the response to process
   * @param request     the request which generated the response
   * @param executionId the execution id of the request
   * @return true if the record was not successfully processed, and we should not commit its offset
   */
  protected boolean handleResponse(BulkItemResponse response,
                                   DocWriteRequest<?> request,
                                   long executionId) {
    if (response.isFailed()) {
      for (String error : MALFORMED_DOC_ERRORS) {
        if (response.getFailureMessage().contains(error)) {
          reportBadRecordAndError(response, executionId);
          return handleMalformedDocResponse();
        }
      }
      if (response.getFailureMessage().contains(VERSION_CONFLICT_EXCEPTION)) {
        // Now check if this version conflict is caused by external version number
        // which was set by us (set explicitly to the topic's offset), in which case
        // the version conflict is due to a repeated or out-of-order message offset
        // and thus can be ignored, since the newer value (higher offset) should
        // remain the key's value in any case.
        if (request == null || request.versionType() != VersionType.EXTERNAL) {
          log.warn("{} version conflict for operation {} on document '{}' version {}"
                          + " in index '{}'.",
                  request != null ? request.versionType() : "UNKNOWN",
                  response.getOpType(),
                  response.getId(),
                  response.getVersion(),
                  response.getIndex()
          );

          log.trace("{} version conflict for operation {} on document '{}' version {}"
                          + " in index '{}'",
                  request != null ? request.versionType() : "UNKNOWN",
                  response.getOpType(),
                  response.getId(),
                  response.getVersion(),
                  response.getIndex()
          );
          // Maybe this was a race condition?  Put it in the DLQ in case someone
          // wishes to investigate.
          reportBadRecordAndError(response, executionId);
        } else {
          // This is an out-of-order or (more likely) repeated topic offset.  Allow the
          // higher offset's value for this key to remain.
          //
          // Note: For external version conflicts, response.getVersion() will be returned as -1,
          // but we have the actual version number for this record because we set it in
          // the request.
          log.debug("Ignoring EXTERNAL version conflict for operation {} on"
                          + " document '{}' version {} in index '{}'.",
                  response.getOpType(),
                  response.getId(),
                  request.version(),
                  response.getIndex()
          );
        }
        return false;
      }
      reportBadRecordAndError(response, executionId);
      error.compareAndSet(
          null,
          new ConnectException("Indexing record failed. "
                  + "Please check DLQ topic for errors.")
      );
      return true;
    }
    return false;
  }

  /**
   * Handle a failed response as a result of a malformed document. Depending on the configuration,
   * ignore or fail.
   *
   * @return true if the record was not successfully processed, and we should not commit its offset
   */
  private boolean handleMalformedDocResponse() {
    String errorMsg = "Encountered an illegal document error."
            + " Ignoring and will not index record. "
            + "Please check DLQ topic for errors.";
    switch (config.behaviorOnMalformedDoc()) {
      case IGNORE:
        log.debug(errorMsg);
        return false;
      case WARN:
        log.warn(errorMsg);
        return false;
      case FAIL:
      default:
        log.error(String.format("Encountered an illegal document error. "
              + "Please check DLQ topic for errors."
              + " To ignore future records like this,"
              + " change the configuration '%s' to '%s'.",
              ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG,
              BehaviorOnMalformedDoc.IGNORE)
        );
        error.compareAndSet(
            null,
            new ConnectException(
                    "Indexing record failed. Please check DLQ topic for errors.")
        );
        return true;
    }
  }

  /**
   * Whether there is a failed response.
   *
   * @return true if a response has failed, false if none have failed
   */
  public boolean isFailed() {
    return error.get() != null;
  }

  /**
   * Gets the mapping for an index.
   *
   * @param index the index to fetch the mapping for
   * @return the MappingMetadata for the index
   */
  private MappingMetadata mapping(String index) {
    GetMappingsRequest request = new GetMappingsRequest().indices(index);
    GetMappingsResponse response = callWithRetries(
        "get mapping for index " + index,
        () -> client.indices().getMapping(request, RequestOptions.DEFAULT)
    );
    return response.mappings().get(index);
  }

  /**
   * Removes the mapping for bulk request id to records being written.
   *
   * @param executionId the execution id of the bulk request
   */
  private void removeFromInFlightRequests(long executionId) {
    if (inFlightRequests != null) {
      inFlightRequests.remove(executionId);
    }
  }

  /**
   * Reports a bad record and errors to the DLQ.
   *
   * @param response    the failed response from ES
   * @param executionId the execution id of the request associated with the response
   */
  private synchronized void reportBadRecordAndError(BulkItemResponse response, long executionId) {

    // RCCA-7507 : Don't push to DLQ if we receive Internal version conflict on data streams
    if (response.getFailureMessage().contains(VERSION_CONFLICT_EXCEPTION)
            && config.isDataStream()) {
      log.debug("Skipping DLQ insertion for DataStream type.");
      return;
    }
    if (reporter != null) {
      List<SinkRecordAndOffset> sinkRecords =
          inFlightRequests.getOrDefault(executionId, new ArrayList<>());
      SinkRecordAndOffset original = sinkRecords.size() > response.getItemId()
          ? sinkRecords.get(response.getItemId())
          : null;
      if (original != null) {
        reporter.report(
            original.sinkRecord,
            new ReportingException("Indexing failed: " + response.getFailureMessage())
        );
      }
    }
  }

  /**
   * Exception that swallows the stack trace used for reporting errors from Elasticsearch
   * (mapper_parser_exception, illegal_argument_exception, and action_request_validation_exception)
   * resulting from bad records using the AK 2.6 reporter DLQ interface.
   */
  @SuppressWarnings("serial")
  public static class ReportingException extends RuntimeException {

    public ReportingException(String message) {
      super(message);
    }

    /**
     * This method is overridden to swallow the stack trace.
     *
     * @return Throwable
     */
    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }
}
