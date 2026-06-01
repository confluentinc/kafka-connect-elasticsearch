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
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.Endpoint;
import co.elastic.clients.transport.TransportOptions;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
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
 * <p>Batch processing is asynchronous. BulkIngester manages its own concurrency. The context
 * object (SinkRecordAndOffset) is carried through the listener, eliminating the need for
 * additional request-to-record maps.
 *
 * <p>If all the retries fail, the exception is reported via an atomic reference to an error,
 * which is checked and thrown from a subsequent call to the task's put method and that results
 * in failure of the task.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class ElasticsearchClient {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchClient.class);

  private static final long WAIT_TIME_MS = 10;
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
  private final co.elastic.clients.elasticsearch.ElasticsearchClient client;
  private final RestClient restClient;
  private final ExecutorService callbackDispatcher;
  private final ScheduledExecutorService bulkScheduler;
  private final ScheduledExecutorService retryScheduler;
  private final ElasticsearchSinkConnectorConfig config;
  private final ErrantRecordReporter reporter;
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
                .toArray(new HttpHost[0])
        ).setHttpClientConfigCallback(configCallbackHandler).build();

    AtomicInteger dispatcherCounter = new AtomicInteger(0);
    String dispatcherPrefix = connectorName + "-" + taskId + "-es-bulk-callback-";
    this.callbackDispatcher = Executors.newCachedThreadPool(r -> {
      Thread t = new Thread(r, dispatcherPrefix + dispatcherCounter.getAndIncrement());
      t.setDaemon(true);
      return t;
    });

    String threadPrefix = connectorName + "-" + taskId + "-elasticsearch-bulk-executor-";
    AtomicInteger threadCounter = new AtomicInteger(0);
    this.bulkScheduler = Executors.newScheduledThreadPool(
        1,
        r -> {
          Thread t = new Thread(r, threadPrefix + threadCounter.getAndIncrement());
          t.setDaemon(true);
          return t;
        }
    );

    // retryScheduler is intentionally separate from bulkScheduler. BulkIngester submits
    // afterBulk callbacks to bulkScheduler and its single thread can block inside
    // FnCondition.awaitUninterruptibly() waiting for a concurrency slot. If retry tasks
    // shared that same thread they would never run, permanently deadlocking the flush.
    String retryPrefix = connectorName + "-" + taskId + "-es-retry-";
    AtomicInteger retryCounter = new AtomicInteger(0);
    this.retryScheduler = Executors.newScheduledThreadPool(
        1,
        r -> {
          Thread t = new Thread(r, retryPrefix + retryCounter.getAndIncrement());
          t.setDaemon(true);
          return t;
        }
    );

    // DispatchingTransport breaks a lock-ordering deadlock between BulkIngester's FnCondition lock
    // and the HLRC NIO pool lock: by dispatching completion callbacks to a separate executor,
    // the IO thread never directly acquires the FnCondition lock.
    // It also retries transport-level failures (e.g. SocketTimeoutException), equivalent to what
    // the original callWithRetries() provided in buildConsumer().
    RestClientTransport rawTransport = new RestClientTransport(
        restClient, new JacksonJsonpMapper());
    DispatchingTransport transport = new DispatchingTransport(
        rawTransport, callbackDispatcher, retryScheduler,
        config.maxRetries(), config.retryBackoffMs());
    this.client = new co.elastic.clients.elasticsearch.ElasticsearchClient(transport);

    this.esVersion = getServerVersion();

    this.bulkIngester = BulkIngester.<SinkRecordAndOffset>of(b -> {
      b.client(this.client)
          .maxOperations(config.batchSize())
          .maxConcurrentRequests(config.maxInFlightRequests())
          .flushInterval(config.lingerMs(), TimeUnit.MILLISECONDS)
          .scheduler(this.bulkScheduler)
          .listener(buildListener(afterBulkCallback));
      if (config.bulkSize() > 0) {
        b.maxSize(config.bulkSize());
      }
      return b;
    });
  }

  /**
   * Package-private constructor for tests. Accepts a pre-built BulkIngester so tests can inject
   * mocks (e.g. a BulkIngester whose close() blocks indefinitely) without a real ES server or
   * real socket layer. Does not call info().
   */
  ElasticsearchClient(
      ElasticsearchSinkConnectorConfig config,
      ErrantRecordReporter reporter,
      BulkIngester<SinkRecordAndOffset> bulkIngester,
      RestClient restClient
  ) {
    this.numBufferedRecords = new AtomicInteger(0);
    this.error = new AtomicReference<>();
    this.config = config;
    this.reporter = reporter;
    this.clock = Time.SYSTEM;
    this.restClient = restClient;
    this.callbackDispatcher = Executors.newCachedThreadPool();
    this.bulkScheduler = Executors.newScheduledThreadPool(1);
    this.retryScheduler = Executors.newScheduledThreadPool(1);
    this.bulkIngester = bulkIngester;
    this.client = null;
    this.esVersion = UNKNOWN_VERSION_TAG;
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
   */
  public co.elastic.clients.elasticsearch.ElasticsearchClient client() {
    return client;
  }

  /**
   * Closes the ElasticsearchClient, flushing outstanding requests and waiting up to
   * {@code flush.timeout.ms} for them to complete.
   *
   * <p>Note: on timeout the abandoned close thread continues running until
   * {@code closeResources()} tears down the underlying transport, at which point it
   * fails fast — matching pre-migration behavior.
   *
   * @throws ConnectException if the flush timeout is exceeded, if BulkIngester.close() throws,
   *     or if the calling thread is interrupted.
   */
  public void close() {
    // BulkIngester.close() has no timeout — it blocks until all in-flight requests and
    // listener tasks settle. Run it on a daemon thread so we can bound the wait by
    // flush.timeout.ms and avoid hanging the Connect worker thread indefinitely.
    ExecutorService closeExecutor = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "elasticsearch-bulk-ingester-close");
      t.setDaemon(true);
      return t;
    });
    try {
      Runnable closeTask = bulkIngester::close;
      Future<?> future = closeExecutor.submit(closeTask);
      future.get(config.flushTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new ConnectException(
          "Failed to process outstanding requests in time while closing the ElasticsearchClient.");
    } catch (ExecutionException e) {
      throw new ConnectException("Failed to close ElasticsearchClient.", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while closing ElasticsearchClient.", e);
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
      throw new ConnectException("Failed to serialize mapping", e);
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
    co.elastic.clients.elasticsearch.indices.GetMappingResponse resp = callWithRetries(
        "get mapping for index " + resourceName,
        () -> client.indices().getMapping(r -> r.index(resourceName))
    );
    IndexMappingRecord record = resp.result().get(resourceName);
    return record != null && record.mappings() != null
        && !record.mappings().properties().isEmpty();
  }

  /**
   * Buffers a record to index.
   *
   * <p>This call is usually asynchronous, but can block when the maximum number of
   * buffered records has been reached.
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

  static class SinkRecordAndOffset {

    final SinkRecord sinkRecord;
    final OffsetState offsetState;
    final BulkOperation operation;

    SinkRecordAndOffset(SinkRecord sinkRecord, OffsetState offsetState, BulkOperation operation) {
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
   * Creates a listener with callback functions to handle completed requests for BulkIngester.
   *
   * @return the listener
   */
  BulkListener<SinkRecordAndOffset> buildListener(Runnable afterBulkCallback) {
    return new BulkListener<SinkRecordAndOffset>() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request,
                             List<SinkRecordAndOffset> contexts) {
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
                            List<SinkRecordAndOffset> contexts, BulkResponse response) {
        // bulkFinished must run even if handleResponse or the callback throws, so that
        // numBufferedRecords is always decremented and waitForInFlightRequests() never stalls.
        try {
          int idx = 0;
          for (BulkResponseItem item : response.items()) {
            SinkRecordAndOffset ctx = idx < contexts.size() ? contexts.get(idx) : null;
            boolean failed = handleResponse(item, ctx, executionId);
            if (!failed && ctx != null) {
              ctx.offsetState.markProcessed();
            }
            idx++;
          }
          afterBulkCallback.run();
        } catch (RuntimeException e) {
          error.compareAndSet(null, new ConnectException("Error in bulk response listener.", e));
        } finally {
          bulkFinished(contexts.size());
        }
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
                            List<SinkRecordAndOffset> contexts, Throwable failure) {
        // Whole-request failure fires after DispatchingTransport exhausts its retries.
        // BulkIngester.backoffPolicy plays no role here: it only retries per-document
        // HTTP 429 items in an otherwise-successful response body. It never retries a
        // transport-level failure. Failing the task is intentional: transport failures
        // are ambiguous infrastructure errors, not document-quality issues, so DLQ
        // semantics ("this record is bad") do not apply. This matches the pre-migration
        // HLRC BulkProcessor.Listener.afterBulk(Throwable) exactly.
        // bulkFinished always runs so numBufferedRecords is decremented and
        // waitForInFlightRequests() cannot stall. Failed records never get markProcessed()
        // so their offsets do not advance and the batch can be replayed after restart.
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
   * Reads versionType directly off the operation rather than re-deriving it from config and topic,
   * because DataConverter.convertRecord sets External only on index/delete ops, never on update
   * (UPSERT). Re-deriving from config would misclassify an UPSERT version conflict as an
   * intentional offset-collision and silently drop it instead of routing it to the DLQ.
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
   * Closes all the connection resources of the client.
   */
  private void closeResources() {
    // shutdown() not shutdownNow(): bulkIngester.close() already drained all pending flushes,
    // so only future periodic triggers remain — no need to interrupt any running task.
    bulkScheduler.shutdown();
    retryScheduler.shutdown();
    callbackDispatcher.shutdown();
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
          } catch (co.elastic.clients.elasticsearch._types.ElasticsearchException | IOException e) {
            if (e.getMessage() != null
                && e.getMessage().contains(RESOURCE_ALREADY_EXISTS_EXCEPTION)) {
              return false;
            }
            throw e instanceof RuntimeException
                ? (RuntimeException) e
                : new ConnectException(e.getMessage(), e);
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
          } catch (co.elastic.clients.elasticsearch._types.ElasticsearchException | IOException e) {
            if (e.getMessage() != null
                && e.getMessage().contains(RESOURCE_ALREADY_EXISTS_EXCEPTION)) {
              return false;
            }
            throw e instanceof RuntimeException
                ? (RuntimeException) e
                : new ConnectException(e.getMessage(), e);
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
   * @param ctx         the context carrying the original record and offset state
   * @param executionId the execution id of the request
   * @return true if the record was not successfully processed, and we should not commit its offset
   */
  protected boolean handleResponse(BulkResponseItem item, SinkRecordAndOffset ctx,
                                   long executionId) {
    if (item.error() != null) {
      String failureMsg = item.error().type() + ": " + item.error().reason();
      for (String err : MALFORMED_DOC_ERRORS) {
        if (failureMsg.contains(err)) {
          reportBadRecordAndError(item, ctx);
          return handleMalformedDocResponse();
        }
      }
      if (failureMsg.contains(VERSION_CONFLICT_EXCEPTION)) {
        // Now check if this version conflict is caused by external version number
        // which was set by us (set explicitly to the topic's offset), in which case
        // the version conflict is due to a repeated or out-of-order message offset
        // and thus can be ignored, since the newer value (higher offset) should
        // remain the key's value in any case.
        boolean isExternalVersioned = ctx != null && isExternallyVersioned(ctx.operation);
        if (!isExternalVersioned) {
          log.warn("Version conflict for operation {} on document '{}' in index '{}'.",
              item.operationType(), item.id(), item.index()
          );
          // Maybe this was a race condition?  Put it in the DLQ in case someone
          // wishes to investigate.
          reportBadRecordAndError(item, ctx);
        } else {
          // This is an out-of-order or (more likely) repeated topic offset.  Allow the
          // higher offset's value for this key to remain.
          log.debug("Ignoring EXTERNAL version conflict for operation {} on"
              + " document '{}' in index '{}'.",
              item.operationType(), item.id(), item.index()
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
   * @param item the failed response item from ES
   * @param ctx  the context carrying the original record
   */
  private synchronized void reportBadRecordAndError(BulkResponseItem item,
                                                    SinkRecordAndOffset ctx) {
    String failureMsg = item.error().type() + ": " + item.error().reason();
    // RCCA-7507 : Don't push to DLQ if we receive Internal version conflict on data streams
    if (failureMsg.contains(VERSION_CONFLICT_EXCEPTION) && config.isDataStream()) {
      log.debug("Skipping DLQ insertion for DataStream type.");
      return;
    }
    if (reporter != null && ctx != null) {
      reporter.report(
          ctx.sinkRecord,
          new ReportingException("Indexing failed: " + failureMsg)
      );
    }
  }

  private static final class DispatchingTransport implements ElasticsearchTransport {
    private final RestClientTransport delegate;
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;
    private final int maxRetries;
    private final long retryBackoffMs;

    DispatchingTransport(RestClientTransport delegate, ExecutorService executor,
                         ScheduledExecutorService scheduler,
                         int maxRetries, long retryBackoffMs) {
      this.delegate = delegate;
      this.executor = executor;
      this.scheduler = scheduler;
      this.maxRetries = maxRetries;
      this.retryBackoffMs = retryBackoffMs;
    }

    @Override
    public <RequestT, ResponseT, ErrorT> ResponseT performRequest(
        RequestT request, Endpoint<RequestT, ResponseT, ErrorT> endpoint,
        TransportOptions options) throws IOException {
      return delegate.performRequest(request, endpoint, options);
    }

    @Override
    public <RequestT, ResponseT, ErrorT> CompletableFuture<ResponseT> performRequestAsync(
        RequestT request, Endpoint<RequestT, ResponseT, ErrorT> endpoint,
        TransportOptions options) {
      CompletableFuture<ResponseT> result = new CompletableFuture<>();
      sendAsync(request, endpoint, options, result, maxRetries, retryBackoffMs);
      return result;
    }

    private <RequestT, ResponseT, ErrorT> void sendAsync(
        RequestT request, Endpoint<RequestT, ResponseT, ErrorT> endpoint,
        TransportOptions options, CompletableFuture<ResponseT> result,
        int retriesLeft, long backoffMs) {
      delegate.performRequestAsync(request, endpoint, options)
          .whenCompleteAsync(
              (resp, err) -> {
                // Retry any transport failure (SocketTimeoutException, ConnectException,
                // ResponseException for 4xx/5xx, etc.), mirroring callWithRetries() in the
                // original HLRC buildConsumer() which caught Exception unconditionally.
                if (err != null && retriesLeft > 0) {
                  scheduler.schedule(
                      () -> sendAsync(request, endpoint, options, result,
                          retriesLeft - 1, backoffMs * 2),
                      backoffMs, TimeUnit.MILLISECONDS);
                } else if (err != null) {
                  result.completeExceptionally(err);
                } else {
                  result.complete(resp);
                }
              },
              executor
          );
    }

    @Override
    public JsonpMapper jsonpMapper() {
      return delegate.jsonpMapper();
    }

    @Override
    public TransportOptions options() {
      return delegate.options();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
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


