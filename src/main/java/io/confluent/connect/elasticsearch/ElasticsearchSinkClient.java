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

import co.elastic.clients.elasticsearch.ElasticsearchClient;
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
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.JsonpUtils;
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
import java.util.function.BooleanSupplier;

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
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
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
public class ElasticsearchSinkClient {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkClient.class);

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
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  protected final AtomicInteger numBufferedRecords;
  private final AtomicReference<ConnectException> error;
  protected final BulkIngester<SinkRecordAndOffset> bulkIngester;
  private final ElasticsearchSinkConnectorConfig config;
  private final ErrantRecordReporter reporter;
  private final ElasticsearchClient client;
  private final JsonpMapper jsonpMapper;
  private final RestClient restClient;
  private final RetryingTransport transport;
  private final ScheduledExecutorService bulkScheduler;
  private final ScheduledExecutorService retryScheduler;
  private final String threadNamePrefix;
  private final Time clock;
  private final Lock inFlightRequestLock = new ReentrantLock();
  private final Condition inFlightRequestsUpdated = inFlightRequestLock.newCondition();
  private final String esVersion;
  private final int maxConcurrentRequests;

  public ElasticsearchSinkClient(
      ElasticsearchSinkConnectorConfig config,
      ErrantRecordReporter reporter,
      Runnable afterBulkCallback,
      int taskId,
      String connectorName
  ) {
    this.threadNamePrefix = connectorName + "-" + taskId + "-";

    // bulkScheduler runs BulkIngester's flush timer AND every afterBulk callback; sized N+1 so the
    // timer isn't starved behind N concurrent callbacks.
    this.bulkScheduler = Executors.newScheduledThreadPool(config.maxInFlightRequests() + 1,
        daemonThreadFactory(threadNamePrefix + "elasticsearch-bulk-scheduler-"));
    this.retryScheduler = Executors.newScheduledThreadPool(1,
        daemonThreadFactory(threadNamePrefix + "elasticsearch-retry-scheduler-"));

    this.numBufferedRecords = new AtomicInteger(0);
    this.error = new AtomicReference<>();
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

    this.jsonpMapper = new JacksonJsonpMapper();
    RestClientTransport rawTransport =
        new RestClientTransport(restClient, jsonpMapper);
    this.transport = new RetryingTransport(
        rawTransport, retryScheduler, config.maxRetries(), config.retryBackoffMs());
    this.client = new ElasticsearchClient(transport);

    this.esVersion = getServerVersion();

    if (config.lingerMs() == 0) {
      // BulkIngester schedules its flush timer with scheduleWithFixedDelay, which rejects a
      // period <= 0. linger.ms=0 is a valid, previously-working config, so clamp it to 1 ms
      // (flush immediately) rather than crash the task at start.
      log.warn("{}=0 is treated as 1 ms (flush immediately); the Elasticsearch BulkIngester "
          + "does not support a zero flush interval.",
          ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG);
    }

    this.maxConcurrentRequests = Math.max(1, config.maxInFlightRequests() - 1);
    this.bulkIngester = BulkIngester.<SinkRecordAndOffset>of(b -> b
        .client(this.client)
        .maxOperations(config.batchSize())
        .maxSize(config.bulkSize())
        .maxConcurrentRequests(maxConcurrentRequests)
        .flushInterval(Math.max(1L, config.lingerMs()), TimeUnit.MILLISECONDS)
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
  public ElasticsearchClient client() {
    return client;
  }

  /**
   * Closes the ElasticsearchSinkClient.
   *
   * @throws ConnectException if all the records fail to flush before the timeout.
   */
  public void close() {
    // BulkIngester.close() has no timeout; run it on a daemon thread so future.get can bound the
    // wait by flush.timeout.ms, matching the old BulkProcessor.awaitClose(timeout) contract.
    ExecutorService closeExecutor = Executors.newSingleThreadExecutor(
        daemonThreadFactory(threadNamePrefix + "elasticsearch-bulk-ingester-close-"));
    try {
      Future<?> future = closeExecutor.submit((Runnable) bulkIngester::close);
      future.get(config.flushTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new ConnectException(
          "Failed to process outstanding requests in time while closing "
              + "the ElasticsearchSinkClient."
      );
    } catch (ExecutionException e) {
      throw new ConnectException("Failed to close ElasticsearchSinkClient.", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException(
          "Interrupted while processing all in-flight requests on ElasticsearchSinkClient close.", e
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
      String mappingJson = OBJECT_MAPPER.writeValueAsString(Mapping.buildMapping(schema));
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
    // BulkIngester.flush() parks uninterruptibly when there are buffered operations and every
    // request slot is busy (with an empty buffer it returns immediately).
    verifyFreeBulkSlot(() -> bulkIngester.pendingOperations() > 0
        && bulkIngester.pendingRequests() >= maxConcurrentRequests);
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
    // A mapping counts as present if the mappings object has ANY content (properties, dynamic,
    // dynamic_templates, _meta, runtime, ...), not just declared properties — matching the
    // pre-migration sourceAsMap()-non-empty semantics. Serializing through the client's own
    // mapper avoids enumerating TypeMapping fields, which grow across client versions.
    return record != null && record.mappings() != null
        && !"{}".equals(JsonpUtils.toJsonString(record.mappings(), jsonpMapper));
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
    verifyFreeBulkSlot(this::addWouldBlock);

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
   * Waits (bounded by flush.timeout.ms, responsive to interruption) while {@code wouldBlock}
   * holds, so the calling thread never reaches BulkIngester's internal wait — that wait is
   * uninterruptible (FnCondition.awaitUninterruptibly) and swallows the worker interrupt
   * Connect uses to cancel a stuck task. The old BulkProcessor blocked on an interruptible
   * semaphore here, so a stuck task could always be cancelled.
   */
  private void verifyFreeBulkSlot(BooleanSupplier wouldBlock) {
    long maxWaitTime = clock.milliseconds() + config.flushTimeoutMs();
    while (wouldBlock.getAsBoolean()) {
      if (Thread.currentThread().isInterrupted()) {
        throw new ConnectException("Interrupted while waiting for a free bulk request slot.");
      }
      clock.sleep(WAIT_TIME_MS);
      if (clock.milliseconds() > maxWaitTime) {
        throw new ConnectException(
            String.format("All %d bulk request slot(s) stayed busy longer than %d ms; "
                            + "Elasticsearch is not keeping up with the write load. "
                            + "Consider increasing %s or %s.",
                    maxConcurrentRequests,
                    config.flushTimeoutMs(),
                    FLUSH_TIMEOUT_MS_CONFIG,
                    MAX_IN_FLIGHT_REQUESTS_CONFIG
            )
        );
      }
    }
  }

  /**
   * True in exactly the state where BulkIngester.add() would park uninterruptibly: every
   * request slot is busy and this operation would fill the batch (by count, or by bytes when
   * bulk.size.bytes is set), making add() flush and wait for a slot. One residual window
   * remains: an operation whose own size pushes the batch past bulk.size.bytes cannot be
   * predicted without serializing it, and the flush-timer thread can take the last free slot
   * between this check and add().
   */
  private boolean addWouldBlock() {
    if (bulkIngester.pendingRequests() < maxConcurrentRequests) {
      return false;
    }
    boolean fillsBatchCount = bulkIngester.pendingOperations() + 1 >= config.batchSize();
    boolean fillsBatchBytes = config.bulkSize() > 0
        && bulkIngester.pendingOperationsSize() >= config.bulkSize();
    return fillsBatchCount || fillsBatchBytes;
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
        // no-op: afterBulk receives the contexts directly, so nothing needs tracking here
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
                             List<SinkRecordAndOffset> contexts, BulkResponse response) {
        List<BulkResponseItem> items = response.items();

        int idx = 0;
        for (BulkResponseItem item : items) {
          SinkRecordAndOffset ctx = idx < contexts.size() ? contexts.get(idx) : null;
          boolean failed = handleResponse(item, ctx);
          if (!failed && ctx != null) {
            ctx.offsetState.markProcessed();
          }
          idx++;
        }

        afterBulkCallback.run();

        bulkFinished(contexts.size());
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
                             List<SinkRecordAndOffset> contexts, Throwable failure) {
        log.warn("Bulk request {} failed", executionId, failure);
        error.compareAndSet(null, new ConnectException("Bulk request failed", failure));
        bulkFinished(contexts.size());
      }

      private void bulkFinished(int count) {
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
    // Fail in-flight requests BEFORE shutting down the schedulers. BulkIngester's completion
    // handling submits afterBulk to bulkScheduler before releasing the request slot, so the
    // scheduler must still be alive; and once retryScheduler.shutdownNow() discards a queued
    // retry, that request's future could never complete on its own — leaving the daemon thread
    // inside bulkIngester.close() parked forever, leaking it plus the buffered records.
    transport.failPendingRequests(
        new IOException("Request abandoned: the Elasticsearch client is closing."));

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
          } catch (ElasticsearchException e) {
            // benign create-vs-create race: someone else created it between our existence
            // check and this call; matched on the structured error type, not the message
            if (!RESOURCE_ALREADY_EXISTS_EXCEPTION.equals(e.error().type())) {
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
          } catch (ElasticsearchException e) {
            // benign create-vs-create race: someone else created it between our existence
            // check and this call; matched on the structured error type, not the message
            if (!RESOURCE_ALREADY_EXISTS_EXCEPTION.equals(e.error().type())) {
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
   * @param item the response item to process
   * @param ctx  the context carrying the original record and offset state, or null
   * @return true if the record was not successfully processed, and we should not commit its offset
   */
  protected boolean handleResponse(BulkResponseItem item, SinkRecordAndOffset ctx) {
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
