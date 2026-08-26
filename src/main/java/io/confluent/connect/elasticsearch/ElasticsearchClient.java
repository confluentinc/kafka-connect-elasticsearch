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
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.http.HttpHost;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.json.JsonpUtils;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnMalformedDoc;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BULK_SIZE_BYTES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static java.util.stream.Collectors.toList;

/**
 * Based on the Elasticsearch Java API Client's BulkIngester, which is responsible for building
 * batches based on size and linger time (not grouped by partitions) and limiting the concurrency
 * (max number of in-flight requests).
 *
 * <p>Batch processing is asynchronous. The BulkIngester executes bulk requests through a
 * {@link RetryingElasticsearchAsyncClient}, which retries whole-request failures (transport
 * errors and non-2xx responses, including a whole-response 429) with a jittered, capped
 * exponential backoff while the ingester's in-flight slot stays held — preserving record
 * order during retries at max.in.flight.requests=1 (see that class for the full design).
 * Item-level failures (including per-item 429s) are not retried here; they are terminal and
 * handled by the listener.
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
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicReference<ConnectException> error;
  protected final BulkIngester<BulkOpContext> bulkIngester;
  private final ElasticsearchSinkConnectorConfig config;
  private final ErrantRecordReporter reporter;
  private final co.elastic.clients.elasticsearch.ElasticsearchClient client;
  private final RetryingElasticsearchAsyncClient retryingClient;
  private final JacksonJsonpMapper jsonpMapper;
  private final RestClientTransport transport;
  private final ScheduledExecutorService bulkRetryExecutor;
  private final ScheduledExecutorService bulkIngesterScheduler;
  private final ExecutorService bulkDispatcherExecutor;
  private final Time clock;
  private final Lock inFlightRequestLock = new ReentrantLock();
  private final Condition inFlightRequestsUpdated = inFlightRequestLock.newCondition();
  private final String esVersion;

  public ElasticsearchClient(
      ElasticsearchSinkConnectorConfig config,
      ErrantRecordReporter reporter,
      Runnable afterBulkCallback,
      int taskId,
      String connectorName
  ) {
    String threadPrefix = connectorName + "-" + taskId + "-elasticsearch-";
    ScheduledThreadPoolExecutor retryExecutor = new ScheduledThreadPoolExecutor(
        1, namedDaemonThreadFactory(threadPrefix + "bulk-retry-"));
    retryExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    retryExecutor.setRemoveOnCancelPolicy(true);
    this.bulkRetryExecutor = retryExecutor;

    this.bulkIngesterScheduler = Executors.newScheduledThreadPool(
        config.maxInFlightRequests() + 1,
        namedDaemonThreadFactory(threadPrefix + "bulk-ingester-"));
    this.bulkDispatcherExecutor = Executors.newFixedThreadPool(
        config.maxInFlightRequests(),
        namedDaemonThreadFactory(threadPrefix + "bulk-dispatcher-"));
    this.numBufferedRecords = new AtomicInteger(0);
    this.error = new AtomicReference<>();
    this.config = config;
    this.reporter = reporter;
    this.clock = Time.SYSTEM;
    long lingerMs = config.lingerMs();
    if (lingerMs == 0) {
      log.warn("{}=0 is treated as 1 ms (flush immediately); the Elasticsearch BulkIngester"
              + " does not support a zero flush interval.", LINGER_MS_CONFIG);
      lingerMs = 1;
    }
    long bulkSize = config.bulkSize();
    if (bulkSize == 0) {
      log.warn("{}=0 is treated as 1 byte (flush every record); the Elasticsearch BulkIngester"
              + " does not support a zero bulk size.", BULK_SIZE_BYTES_CONFIG);
      bulkSize = 1;
    }
    final long flushIntervalMs = lingerMs;
    final long maxBulkSizeBytes = bulkSize;

    ConfigCallbackHandler configCallbackHandler = new ConfigCallbackHandler(config);
    JacksonJsonpMapper mapper = new JacksonJsonpMapper();
    RestClient restClient = null;
    RestClientTransport clientTransport = null;
    co.elastic.clients.elasticsearch.ElasticsearchClient syncClient;
    String serverVersion;
    RetryingElasticsearchAsyncClient asyncClient;
    BulkIngester<BulkOpContext> ingester;
    try {
      restClient = RestClient
          .builder(
              config.connectionUrls()
                  .stream()
                  .map(HttpHost::create)
                  .collect(toList())
                  .toArray(new HttpHost[config.connectionUrls().size()])
          ).setHttpClientConfigCallback(configCallbackHandler).build();
      clientTransport = new RestClientTransport(restClient, mapper);
      syncClient = new co.elastic.clients.elasticsearch.ElasticsearchClient(clientTransport);
      serverVersion = getServerVersion(syncClient);
      asyncClient = new RetryingElasticsearchAsyncClient(
          clientTransport,
          config.maxRetries(),
          config.retryBackoffMs(),
          bulkRetryExecutor,
          bulkDispatcherExecutor);
      final RetryingElasticsearchAsyncClient ingesterClient = asyncClient;
      ingester = BulkIngester.of(builder -> builder
          .client(ingesterClient)
          .maxOperations(config.batchSize())
          .maxSize(maxBulkSizeBytes)
          .maxConcurrentRequests(config.maxInFlightRequests())
          .flushInterval(flushIntervalMs, TimeUnit.MILLISECONDS)
          .scheduler(bulkIngesterScheduler)
          .listener(buildListener(afterBulkCallback)));
    } catch (RuntimeException | Error e) {
      // RestClient.builder().build() starts the HTTP client's non-daemon I/O reactor
      // threads immediately; a task restart loop must not leak them (or the executors)
      // when construction fails past that point — stop() is never called on a task
      // whose constructor threw.
      closeQuietly(clientTransport, restClient);
      bulkRetryExecutor.shutdownNow();
      bulkDispatcherExecutor.shutdownNow();
      bulkIngesterScheduler.shutdownNow();
      throw e;
    }
    this.jsonpMapper = mapper;
    this.transport = clientTransport;
    this.client = syncClient;
    this.esVersion = serverVersion;
    this.retryingClient = asyncClient;
    this.bulkIngester = ingester;
  }

  /**
   * Best-effort close of a partially constructed transport. Closing the transport also
   * closes the RestClient beneath it; a bare RestClient is closed directly.
   */
  private static void closeQuietly(RestClientTransport transport, RestClient restClient) {
    try {
      if (transport != null) {
        transport.close();
      } else if (restClient != null) {
        restClient.close();
      }
    } catch (Exception e) {
      log.warn("Failed to close the Elasticsearch transport after failed construction.", e);
    }
  }

  // Package-private and overridable so a test can force a failure after the transport
  // (and its live I/O reactor threads) is built, exercising the constructor's cleanup.
  String getServerVersion(
      co.elastic.clients.elasticsearch.ElasticsearchClient esClient) {
    String esVersionNumber = UNKNOWN_VERSION_TAG;
    try {
      esVersionNumber = esClient.info().version().number();
    } catch (Exception e) {
      // Same error messages as from validating the connection for IOException.
      // Insufficient privileges to validate the version number if caught
      // ElasticsearchException.
      log.warn("Failed to get ES server version", e);
    }
    return esVersionNumber;
  }

  /**
   * Closes the ElasticsearchClient.
   *
   * @throws ConnectException if all the records fail to flush before the timeout.
   */
  public void close() {
    // close() is reachable twice on one task (throwIfFailed() from put, then the
    // framework's stop()), always on the task thread. The second call must be a no-op:
    // re-running bulkIngester.close() after closeResources() terminated its scheduler
    // would park forever (its close waits on listener tasks that scheduler must run).
    if (!closed.compareAndSet(false, true)) {
      log.debug("The ElasticsearchClient is already closed.");
      return;
    }
    try {
      // Flush and drain even when a batch has already failed: sibling batches may still
      // be in flight carrying good records, and master's awaitClose gave them the
      // flush-timeout window to finish and mark their offsets. Aborting their live HTTP
      // exchanges instead would redeliver records Elasticsearch already indexed.
      try {
        bulkIngesterScheduler.submit(() -> {
          try {
            bulkIngester.flush();
          } catch (Exception e) {
            log.debug("Tried to flush data to Elasticsearch on close, but failed.", e);
          }
        });
      } catch (RejectedExecutionException e) {
        log.debug("Could not schedule a flush because the scheduler is already closed.", e);
      }
      if (!awaitBufferDrain(config.flushTimeoutMs())) {
        throw new ConnectException(
            "Failed to process outstanding requests in time while closing the ElasticsearchClient."
        );
      }
      if (isFailed()) {
        throw error.get();
      }
    } finally {
      closeResources();
    }
  }

  /**
   * Waits until all buffered records have been processed or the timeout expires.
   *
   * @param timeoutMs how long to wait for the buffer to drain
   * @return true if the buffer was drained, false if the timeout expired first
   */
  private boolean awaitBufferDrain(long timeoutMs) {
    long maxWaitTime = clock.milliseconds() + timeoutMs;
    while (numBufferedRecords.get() > 0) {
      if (Thread.currentThread().isInterrupted()) {
        // clock.sleep swallows the InterruptedException and re-sets the flag, so there is
        // no caught exception to chain; attach a fresh one to keep master's cause contract.
        throw new ConnectException(
            "Interrupted while processing all in-flight requests on ElasticsearchClient close.",
            new InterruptedException()
        );
      }
      if (clock.milliseconds() > maxWaitTime) {
        return false;
      }
      clock.sleep(WAIT_TIME_MS);
    }
    return true;
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
    String mappingJson = Mapping.buildMappingJson(schema);
    callWithRetries(
        String.format("create mapping for resource %s with schema %s", resourceName, schema),
        () -> client.indices().putMapping(m -> m
            .index(resourceName)
            .withJson(new StringReader(mappingJson)))
    );
  }

  public String version() {
    return esVersion;
  }

  /**
   * Triggers a flush of any buffered records.
   */
  public void flush() {
    bulkIngester.flush();
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
    TypeMapping mapping = mapping(resourceName);
    return mapping != null && !"{}".equals(JsonpUtils.toJsonString(mapping, jsonpMapper));
  }

  /**
   * Buffers a record to index.
   *
   * <p>This call is usually asynchronous, but can block in any of the following scenarios:
   * <ul>
   *   <li>A new batch is finished (e.g. max batch size has been reached) and
   *    the overall number of concurrent requests (max in flight requests) are in use.</li>
   *   <li>The maximum number of buffered records have been reached</li>
   * </ul>
   *
   * @param record the record to index
   * @param operation the associated bulk operation to send
   * @param offsetState record's offset state
   * @throws ConnectException if one of the requests failed
   */
  public void index(SinkRecord record, BulkOperation operation, OffsetState offsetState) {
    throwIfFailed();

    // TODO should we just pause partitions instead of blocking and failing the connector?
    verifyNumBufferedRecords();

    numBufferedRecords.incrementAndGet();
    bulkIngester.add(operation, new BulkOpContext(record, offsetState, operation));
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

  /**
   * The per-operation context handed to the BulkIngester. The listener receives these contexts
   * aligned one-to-one with the bulk response items.
   */
  static class BulkOpContext {

    final SinkRecord sinkRecord;
    final OffsetState offsetState;
    final BulkOperation operation;

    BulkOpContext(SinkRecord sinkRecord, OffsetState offsetState, BulkOperation operation) {
      this.sinkRecord = sinkRecord;
      this.offsetState = offsetState;
      this.operation = operation;
    }
  }

  /**
   * Checks whether the index exists.
   *
   * @param index the index to check
   * @return true if it exists, false if it does not
   */
  public boolean indexExists(String index) {
    return callWithRetries(
        "check if index " + index + " exists",
        () -> client.indices().exists(e -> e.index(index)).value()
    );
  }

  /**
   * Creates a listener with callback functions to handle completed requests for the BulkIngester.
   *
   * @return the listener
   */
  private BulkListener<BulkOpContext> buildListener(Runnable afterBulkCallback) {
    return new BulkListener<BulkOpContext>() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request,
                             List<BulkOpContext> contexts) {
        log.trace("Executing bulk request {} with {} operations", executionId, contexts.size());
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
                            List<BulkOpContext> contexts, BulkResponse response) {
        List<BulkResponseItem> items = response.items();
        for (int i = 0; i < items.size() && i < contexts.size(); i++) {
          BulkOpContext context = contexts.get(i);
          boolean failed = handleResponse(items.get(i), context);
          if (!failed) {
            context.offsetState.markProcessed();
          }
        }

        afterBulkCallback.run();

        bulkFinished(contexts);
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
                            List<BulkOpContext> contexts, Throwable failure) {
        log.warn("Bulk request {} failed", executionId, failure);
        error.compareAndSet(null, new ConnectException("Bulk request failed", failure));
        bulkFinished(contexts);
      }

      private void bulkFinished(List<BulkOpContext> contexts) {
        inFlightRequestLock.lock();
        try {
          numBufferedRecords.addAndGet(-contexts.size());
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
    if (numBufferedRecords.get() == 0) {
      try {
        bulkIngester.close();
      } catch (Exception e) {
        log.warn("Failed to close bulk ingester.", e);
      }
    } else {
      log.warn("Skipping bulk ingester close because {} records are still buffered; closing the "
              + "underlying transport will abort them.", numBufferedRecords.get());
    }

    try {
      transport.close();
    } catch (IOException e) {
      log.warn("Failed to close Elasticsearch client.", e);
    }

    // Stop the retry ladder before anything else: shutdown() discards a retry queued
    // mid-backoff without running it, which would leave its bulk future incomplete
    // forever — the ingester's in-flight slot held and the listener's buffer accounting
    // never run, hanging any later waitForInFlightRequests(). Fail those futures
    // explicitly while the ingester scheduler is still accepting the listener callbacks
    // that do that accounting.
    bulkRetryExecutor.shutdown();
    retryingClient.failAllPending(
        new ConnectException("Bulk request aborted: the Elasticsearch client is closing"));
    bulkDispatcherExecutor.shutdown();
    bulkIngesterScheduler.shutdown();
    try {
      awaitTerminationWithin(
          Arrays.asList(bulkRetryExecutor, bulkDispatcherExecutor, bulkIngesterScheduler),
          CLOSE_WAIT_TIME_MS);
    } catch (InterruptedException e) {
      bulkRetryExecutor.shutdownNow();
      bulkDispatcherExecutor.shutdownNow();
      bulkIngesterScheduler.shutdownNow();
      Thread.currentThread().interrupt();
      log.warn("Interrupted while awaiting for executor service shutdown.", e);
    }
  }

  /**
   * Awaits termination of all the given executors against one shared deadline, so the
   * caller waits at most {@code totalTimeoutMs} in total rather than that per executor
   * (the pools wind down concurrently). Any executor not terminated by the deadline is
   * forced down with {@code shutdownNow()}. Callers must have already called
   * {@code shutdown()} on each.
   *
   * @param executors the executors to await, in the order to check them
   * @param totalTimeoutMs the total budget shared across all of them
   * @throws InterruptedException if the calling thread is interrupted while waiting
   */
  static void awaitTerminationWithin(List<ExecutorService> executors, long totalTimeoutMs)
      throws InterruptedException {
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(totalTimeoutMs);
    for (ExecutorService executor : executors) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (!executor.awaitTermination(Math.max(0, remainingNanos), TimeUnit.NANOSECONDS)) {
        executor.shutdownNow();
      }
    }
  }

  /**
   * Creates a data stream. Will not recreate the data stream if it already exists.
   *
   * @param dataStream the data stream to create given in the form {type}-{dataset}-{namespace}
   * @return true if the data stream was created, false if it already exists
   */
  private boolean createDataStream(String dataStream) {
    return callWithRetries(
        "create data stream " + dataStream,
        () -> {
          try {
            client.indices().createDataStream(c -> c.name(dataStream));
          } catch (ElasticsearchException e) {
            if (!isResourceAlreadyExists(e)) {
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
    return callWithRetries(
        "create index " + index,
        () -> {
          try {
            client.indices().create(c -> c.index(index));
          } catch (ElasticsearchException e) {
            if (!isResourceAlreadyExists(e)) {
              throw e;
            }
            return false;
          }
          return true;
        }
    );
  }

  private static boolean isResourceAlreadyExists(ElasticsearchException e) {
    ErrorCause cause = e.error();
    return (cause != null && RESOURCE_ALREADY_EXISTS_EXCEPTION.equals(cause.type()))
        || (e.getMessage() != null && e.getMessage().contains(RESOURCE_ALREADY_EXISTS_EXCEPTION));
  }

  private static VersionType operationVersionType(BulkOperation operation) {
    if (operation.isIndex()) {
      return operation.index().versionType();
    }
    if (operation.isDelete()) {
      return operation.delete().versionType();
    }
    return null;
  }

  private static Long operationVersion(BulkOperation operation) {
    if (operation.isIndex()) {
      return operation.index().version();
    }
    if (operation.isDelete()) {
      return operation.delete().version();
    }
    return null;
  }

  /**
   * Processes an item of a bulk response.
   * Successful responses are ignored. Failed responses are reported to the DLQ and handled
   * according to configuration (ignore or fail). Version conflicts are ignored.
   *
   * @param item    the bulk response item to process
   * @param context the context of the operation which generated the response
   * @return true if the record was not successfully processed, and we should not commit its offset
   */
  protected boolean handleResponse(BulkResponseItem item, BulkOpContext context) {
    if (item.error() == null) {
      return false;
    }
    String errorType = item.error().type();
    if (MALFORMED_DOC_ERRORS.contains(errorType)) {
      reportBadRecordAndError(item, context);
      return handleMalformedDocResponse();
    }
    if (VERSION_CONFLICT_EXCEPTION.equals(errorType)) {
      // Now check if this version conflict is caused by external version number
      // which was set by us (set explicitly to the topic's offset), in which case
      // the version conflict is due to a repeated or out-of-order message offset
      // and thus can be ignored, since the newer value (higher offset) should
      // remain the key's value in any case.
      VersionType versionType = operationVersionType(context.operation);
      if (versionType != VersionType.External) {
        log.warn("{} version conflict for operation {} version {}"
                        + " in index '{}'.",
                versionType != null ? versionType : "UNKNOWN",
                item.operationType(),
                item.version(),
                item.index()
        );

        log.trace("{} version conflict for operation {} on document '{}' version {}"
                        + " in index '{}'",
                versionType != null ? versionType : "UNKNOWN",
                item.operationType(),
                item.id(),
                item.version(),
                item.index()
        );
        // Maybe this was a race condition?  Put it in the DLQ in case someone
        // wishes to investigate.
        reportBadRecordAndError(item, context);
      } else {
        // This is an out-of-order or (more likely) repeated topic offset.  Allow the
        // higher offset's value for this key to remain.
        //
        // Note: For external version conflicts, the response does not carry the version,
        // but we have the actual version number for this record because we set it in
        // the operation.
        log.debug("Ignoring EXTERNAL version conflict for operation {}"
                        + " version {} in index '{}'.",
                item.operationType(),
                operationVersion(context.operation),
                item.index()
        );
      }
      return false;
    }
    reportBadRecordAndError(item, context);
    error.compareAndSet(
        null,
        new ConnectException("Indexing record failed. "
                + "Please check DLQ topic for errors.")
    );
    return true;
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
   * @return the mapping for the index, or null if the index has no mapping
   */
  private TypeMapping mapping(String index) {
    return callWithRetries(
        "get mapping for index " + index,
        () -> {
          co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord record =
              client.indices().getMapping(g -> g.index(index)).result().get(index);
          return record == null ? null : record.mappings();
        }
    );
  }

  /**
   * Reports a bad record and errors to the DLQ.
   *
   * @param item    the failed bulk response item from ES
   * @param context the context of the operation associated with the response
   */
  private synchronized void reportBadRecordAndError(BulkResponseItem item, BulkOpContext context) {

    // RCCA-7507 : Don't push to DLQ if we receive Internal version conflict on data streams
    ErrorCause cause = item.error();
    if (cause != null && VERSION_CONFLICT_EXCEPTION.equals(cause.type()) && config.isDataStream()) {
      log.debug("Skipping DLQ insertion for DataStream type.");
      return;
    }
    if (reporter != null) {
      reporter.report(
          context.sinkRecord,
          new ReportingException("Indexing failed: " + describeError(item.error()))
      );
    }
  }

  /**
   * Renders an error and its caused_by chain as {@code [type] reason; nested: [type] reason...}.
   * The nested causes usually carry the actionable detail (e.g. which value failed to parse),
   * and both type and reason are optional in the response, so nulls must not leak into the
   * message.
   *
   * @param error the top-level error cause returned by Elasticsearch, may be null
   * @return a human-readable description of the whole error chain
   */
  private static String describeError(ErrorCause error) {
    if (error == null) {
      return "unknown error";
    }
    StringBuilder message = new StringBuilder();
    for (ErrorCause current = error; current != null; current = current.causedBy()) {
      if (message.length() > 0) {
        message.append("; nested: ");
      }
      message.append('[')
          .append(current.type() == null ? "unknown" : current.type())
          .append("] ")
          .append(current.reason() == null ? "unknown reason" : current.reason());
    }
    return message.toString();
  }

  /**
   * A factory for daemon threads named {@code namePrefix1}, {@code namePrefix2}, ... so
   * every connector-owned pool is attributable to its connector and task in thread dumps.
   *
   * @param namePrefix the thread-name prefix, including a trailing separator
   * @return the thread factory
   */
  private static ThreadFactory namedDaemonThreadFactory(String namePrefix) {
    return new ThreadFactory() {
      private final AtomicInteger threadNumber = new AtomicInteger(1);
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = Executors.defaultThreadFactory().newThread(r);
        thread.setName(namePrefix + threadNumber.getAndIncrement());
        thread.setDaemon(true);
        return thread;
      }
    };
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
