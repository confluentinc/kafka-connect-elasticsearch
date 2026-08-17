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

import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
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

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnMalformedDoc;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static java.util.stream.Collectors.toList;

/**
 * Based on Elasticsearch's BulkIngester, which is responsible for building batches based on size
 * and linger time (not grouped by partitions) and limiting the concurrency (max number of
 * in-flight requests).
 *
 * <p>Batch processing is asynchronous. BulkIngester manages its own concurrency; retries of
 * transport-level failures (timeouts, connection errors) are handled by {@link RetryingTransport}
 * using the same backoff algorithm as {@link RetryUtil}.
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
  protected final BulkIngester<SinkRecordAndOffset> bulkIngester;
  private final ConcurrentMap<Long, List<SinkRecordAndOffset>> inFlightRequests;
  private final ElasticsearchSinkConnectorConfig config;
  private final ErrantRecordReporter reporter;
  private final co.elastic.clients.elasticsearch.ElasticsearchClient client;
  private final RestClient restClient;
  private final ScheduledExecutorService bulkScheduler;
  private final ScheduledExecutorService retryScheduler;
  private final String threadNamePrefix;
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
    this.numBufferedRecords = new AtomicInteger(0);
    this.error = new AtomicReference<>();
    this.inFlightRequests = reporter != null ? new ConcurrentHashMap<>() : null;
    this.config = config;
    this.reporter = reporter;
    this.clock = Time.SYSTEM;

    ConfigCallbackHandler configCallbackHandler = new ConfigCallbackHandler(config);
    this.restClient = RestClient
        .builder(
            config.connectionUrls()
                .stream()
                .map(HttpHost::create)
                .collect(toList())
                .toArray(new HttpHost[config.connectionUrls().size()])
        ).setHttpClientConfigCallback(configCallbackHandler).build();

    this.threadNamePrefix = connectorName + "-" + taskId + "-";

    // BulkIngester runs BOTH its flushInterval timer and every BulkListener callback on the
    // scheduler passed to its builder: the constructor calls scheduler.scheduleWithFixedDelay for
    // the timer, and listenerAfterBulkSuccess/listenerAfterBulkException both call
    // scheduler.submit(...) for the callbacks. A single thread would therefore serialize all
    // afterBulk processing (handleResponse over up to batch.size items, DLQ reporting and offset
    // marking) against each other and against the timer. Sized maxInFlightRequests + 1 to preserve
    // the pre-migration behaviour, where callbacks ran on the maxInFlightRequests-sized
    // bulkExecutorService and the flush timer had BulkProcessor's own separate scheduler. The
    // callback path was already concurrent before this migration, which is why
    // reportBadRecordAndError is synchronized and bulkFinished takes inFlightRequestLock.
    this.bulkScheduler = Executors.newScheduledThreadPool(config.maxInFlightRequests() + 1,
        daemonThreadFactory(threadNamePrefix + "elasticsearch-bulk-scheduler-"));
    this.retryScheduler = Executors.newScheduledThreadPool(1,
        daemonThreadFactory(threadNamePrefix + "elasticsearch-retry-scheduler-"));
    RestClientTransport rawTransport =
        new RestClientTransport(restClient, new JacksonJsonpMapper());
    RetryingTransport transport = new RetryingTransport(
        rawTransport, retryScheduler, config.maxRetries(), config.retryBackoffMs());
    this.client = new co.elastic.clients.elasticsearch.ElasticsearchClient(transport);

    this.esVersion = getServerVersion();

    this.bulkIngester = BulkIngester.<SinkRecordAndOffset>of(b -> b
        .client(this.client)
        .maxOperations(config.batchSize())
        .maxSize(config.bulkSize())
        // Preserves the pre-migration concurrency exactly. BulkProcessor was configured with
        // setConcurrentRequests(maxInFlightRequests - 1), which produced a Semaphore of that many
        // permits, so max.in.flight.requests=N has always allowed N-1 concurrent bulk requests.
        // ElasticsearchConnectorNetworkIT documents this in a TODO and records that correcting the
        // off-by-one "would be a breaking change", so it is deliberately not corrected here.
        //
        // Math.max(1, ...) is required, not defensive: max.in.flight.requests permits 1
        // (between(1, 1000)), and the two APIs differ exactly at zero. Old
        // setConcurrentRequests(0) meant Semaphore(1) plus a latch, i.e. one synchronous request;
        // new maxConcurrentRequests(0) gates on requestsInFlightCount < 0, which is never true, so
        // no request would ever be admitted.
        .maxConcurrentRequests(Math.max(1, config.maxInFlightRequests() - 1))
        .flushInterval(config.lingerMs(), TimeUnit.MILLISECONDS)
        .scheduler(this.bulkScheduler)
        .listener(buildListener(afterBulkCallback))
    );
  }

  private static ThreadFactory daemonThreadFactory(String namePrefix) {
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

  private String getServerVersion() {
    try {
      return client.info().version().number();
    } catch (Exception e) {
      // Same error messages as from validating the connection for IOException.
      // Insufficient privileges to validate the version number if caught
      // ElasticsearchException.
      log.warn("Failed to get ES server version", e);
      return UNKNOWN_VERSION_TAG;
    }
  }

  /**
   * Returns the underlying Elasticsearch client.
   *
   * @return the underlying ElasticsearchClient
   */
  public co.elastic.clients.elasticsearch.ElasticsearchClient client() {
    return client;
  }

  /**
   * Closes the ElasticsearchClient.
   *
   * @throws ConnectException if all the records fail to flush before the timeout.
   */
  public void close() {
    // BulkIngester.close() has no timeout of its own -- it blocks in
    // FnCondition.whenReady(closedAndFlushed) until operations are drained, requestsInFlightCount
    // is 0 and both listenerInProgressCount and retriesInProgressCount reach 0. Run it on its own
    // thread so we can bound the wait by flush.timeout.ms, matching the pre-migration
    // BulkProcessor.awaitClose(timeout, unit) contract.
    //
    // Known limitation on timeout: that wait uses Condition.awaitUninterruptibly(), so the
    // abandoned close thread cannot be interrupted out of it, and listenerInProgressCount is
    // incremented before each listener task is submitted to bulkScheduler. If closeResources()
    // below reaches bulkScheduler.shutdownNow() while listener tasks are still queued, those tasks
    // are discarded, the counter never returns to 0 and the abandoned thread parks permanently.
    // It is a daemon thread, so JVM exit is unaffected, but a worker that repeatedly times out on
    // close will accumulate one parked thread per occurrence. Sizing bulkScheduler at
    // maxInFlightRequests + 1 (see constructor) makes a listener backlog far less likely; removing
    // the failure mode entirely needs a redesign of the close path and is tracked separately.
    ExecutorService closeExecutor = Executors.newSingleThreadExecutor(
        daemonThreadFactory(threadNamePrefix + "elasticsearch-bulk-ingester-close-"));
    try {
      Future<?> future = closeExecutor.submit((Runnable) bulkIngester::close);
      future.get(config.flushTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new ConnectException(
          "Failed to process outstanding requests in time while closing the ElasticsearchClient."
      );
    } catch (ExecutionException e) {
      throw new ConnectException("Failed to close ElasticsearchClient.", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException(
          "Interrupted while processing all in-flight requests on ElasticsearchClient close.", e
      );
    } finally {
      closeExecutor.shutdownNow();
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
    try {
      String mappingJson = new ObjectMapper().writeValueAsString(Mapping.buildMapping(schema));
      callWithRetries(
          String.format("create mapping for resource %s with schema %s", resourceName, schema),
          () -> client.indices().putMapping(
              r -> r.index(resourceName).withJson(new StringReader(mappingJson)))
      );
    } catch (JsonProcessingException e) {
      throw new ConnectException("Failed to serialize mapping for resource " + resourceName, e);
    }
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
    GetMappingResponse response = callWithRetries(
        "get mapping for index " + resourceName,
        () -> client.indices().getMapping(r -> r.index(resourceName))
    );
    IndexMappingRecord record = response.result().get(resourceName);
    return record != null && record.mappings() != null
        && !record.mappings().properties().isEmpty();
  }

  /**
   * Buffers a record to index.
   *
   * <p>This call is usually asynchronous, but can block in any of the following scenarios:
   * <ul>
   *   <li>A new batch is finished (e.g. max batch size has been reached) and
   *    the overall number of threads (max in flight requests) are in use.</li>
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
    bulkIngester.add(operation, new SinkRecordAndOffset(record, offsetState, operation));
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
   * Context attached to each buffered operation and handed back by {@link BulkListener}.
   *
   * <p>Package-private, not private: it appears in the signature of the {@code protected}
   * {@link #handleResponse}, so making it private would leave that method impossible for any
   * subclass to override -- including the tests that exercise the version-conflict branches.
   */
  static class SinkRecordAndOffset {

    final SinkRecord sinkRecord;
    final OffsetState offsetState;
    final BulkOperation operation;

    public SinkRecordAndOffset(
        SinkRecord sinkRecord, OffsetState offsetState, BulkOperation operation) {
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
        () -> client.indices().exists(r -> r.index(index)).value()
    );
  }

  /**
   * Creates a listener with callback functions to handle completed requests for the BulkIngester.
   *
   * @return the listener
   */
  private BulkListener<SinkRecordAndOffset> buildListener(Runnable afterBulkCallback) {
    return new BulkListener<SinkRecordAndOffset>() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request,
                              List<SinkRecordAndOffset> contexts) {
        if (inFlightRequests != null) {
          inFlightRequests.put(executionId, contexts);
        }
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
                             List<SinkRecordAndOffset> contexts, BulkResponse response) {
        List<BulkResponseItem> items = response.items();

        int idx = 0;
        for (BulkResponseItem item : items) {
          SinkRecordAndOffset ctx = idx < contexts.size() ? contexts.get(idx) : null;
          boolean failed = handleResponse(item, ctx, executionId);
          if (!failed && ctx != null) {
            ctx.offsetState.markProcessed();
          }
          idx++;
        }

        afterBulkCallback.run();

        bulkFinished(executionId, contexts.size());
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
                             List<SinkRecordAndOffset> contexts, Throwable failure) {
        log.warn("Bulk request {} failed", executionId, failure);
        error.compareAndSet(null, new ConnectException("Bulk request failed", failure));
        bulkFinished(executionId, contexts.size());
      }

      private void bulkFinished(long executionId, int count) {
        removeFromInFlightRequests(executionId);
        inFlightRequestLock.lock();
        try {
          numBufferedRecords.addAndGet(-count);
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
   * Returns true iff the bulk operation was submitted with VersionType.External.
   *
   * <p>Reads versionType directly off the operation rather than re-deriving it from config and
   * topic, because DataConverter.convertRecord sets External only on index/create/delete
   * operations, never on update (UPSERT). Re-deriving from config would misclassify an UPSERT
   * version conflict as an intentional offset-collision and silently drop it instead of routing
   * it to the DLQ.
   */
  private static boolean isExternallyVersioned(BulkOperation operation) {
    if (operation.isIndex()) {
      return VersionType.External.equals(operation.index().versionType());
    }
    if (operation.isCreate()) {
      return VersionType.External.equals(operation.create().versionType());
    }
    if (operation.isDelete()) {
      return VersionType.External.equals(operation.delete().versionType());
    }
    return false;
  }

  /**
   * Closes all the connection and thread resources of the client.
   */
  private void closeResources() {
    bulkScheduler.shutdown();
    retryScheduler.shutdown();
    try {
      if (!bulkScheduler.awaitTermination(CLOSE_WAIT_TIME_MS, TimeUnit.MILLISECONDS)) {
        bulkScheduler.shutdownNow();
      }
      if (!retryScheduler.awaitTermination(CLOSE_WAIT_TIME_MS, TimeUnit.MILLISECONDS)) {
        retryScheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      bulkScheduler.shutdownNow();
      retryScheduler.shutdownNow();
      Thread.currentThread().interrupt();
      log.warn("Interrupted while awaiting for executor service shutdown.", e);
    }

    try {
      restClient.close();
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
    return callWithRetries(
        "create data stream " + dataStream,
        () -> {
          try {
            client.indices().createDataStream(r -> r.name(dataStream));
          } catch (ElasticsearchException | IOException e) {
            if (e.getMessage() == null
                || !e.getMessage().contains(RESOURCE_ALREADY_EXISTS_EXCEPTION)) {
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
            client.indices().create(r -> r.index(index));
          } catch (ElasticsearchException | IOException e) {
            if (e.getMessage() == null
                || !e.getMessage().contains(RESOURCE_ALREADY_EXISTS_EXCEPTION)) {
              throw e;
            }
            return false;
          }
          return true;
        }
    );
  }

  /**
   * Processes a response from a bulk item request.
   * Successful responses are ignored. Failed responses are reported to the DLQ and handled
   * according to configuration (ignore or fail). Version conflicts are ignored.
   *
   * @param item        the response item to process
   * @param ctx         the context carrying the original record and offset state, or null
   * @param executionId the execution id of the request
   * @return true if the record was not successfully processed, and we should not commit its offset
   */
  protected boolean handleResponse(BulkResponseItem item, SinkRecordAndOffset ctx,
                                    long executionId) {
    if (item.error() != null) {
      String errorType = item.error().type();
      if (MALFORMED_DOC_ERRORS.contains(errorType)) {
        reportBadRecordAndError(item, ctx);
        return handleMalformedDocResponse();
      }
      if (VERSION_CONFLICT_EXCEPTION.equals(errorType)) {
        // Now check if this version conflict is caused by external version number
        // which was set by us (set explicitly to the topic's offset), in which case
        // the version conflict is due to a repeated or out-of-order message offset
        // and thus can be ignored, since the newer value (higher offset) should
        // remain the key's value in any case.
        boolean isExternalVersioned = ctx != null && isExternallyVersioned(ctx.operation);
        if (!isExternalVersioned) {
          log.warn("Version conflict for operation {} on document '{}' in index '{}'.",
                  item.operationType(),
                  item.id(),
                  item.index()
          );

          log.trace("Version conflict for operation {} on document '{}' in index '{}': {}",
                  item.operationType(),
                  item.id(),
                  item.index(),
                  item.error().reason()
          );
          // Maybe this was a race condition?  Put it in the DLQ in case someone
          // wishes to investigate.
          reportBadRecordAndError(item, ctx);
        } else {
          // This is an out-of-order or (more likely) repeated topic offset.  Allow the
          // higher offset's value for this key to remain.
          log.debug("Ignoring EXTERNAL version conflict for operation {} on document '{}'"
                          + " in index '{}'.",
                  item.operationType(),
                  item.id(),
                  item.index()
          );
        }
        return false;
      }
      reportBadRecordAndError(item, ctx);
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
   * @param item        the failed response item from ES
   * @param ctx         the context carrying the original record, or null
   */
  private synchronized void reportBadRecordAndError(BulkResponseItem item,
                                                     SinkRecordAndOffset ctx) {

    // RCCA-7507 : Don't push to DLQ if we receive Internal version conflict on data streams
    if (VERSION_CONFLICT_EXCEPTION.equals(item.error().type()) && config.isDataStream()) {
      log.debug("Skipping DLQ insertion for DataStream type.");
      return;
    }
    if (reporter != null && ctx != null) {
      reporter.report(
          ctx.sinkRecord,
          new ReportingException("Indexing failed: "
              + item.error().type() + ": " + item.error().reason())
      );
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
