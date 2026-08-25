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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
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

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
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
import co.elastic.clients.transport.BackoffPolicy;
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
 * <p>Batch processing is asynchronous. The BulkIngester executes bulk requests through the
 * client's transport. HTTP 429 (too many requests) responses are retried per operation by the
 * BulkIngester's backoff policy; transport-level failures are retried by re-adding the whole
 * batch with a jittered exponential backoff.
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
  protected final BulkIngester<BulkOpContext> bulkIngester;
  private final ElasticsearchSinkConnectorConfig config;
  private final ErrantRecordReporter reporter;
  private final co.elastic.clients.elasticsearch.ElasticsearchClient client;
  private final JacksonJsonpMapper jsonpMapper;
  private final RestClientTransport transport;
  private final ScheduledExecutorService bulkRetryExecutor;
  private final ScheduledExecutorService bulkIngesterScheduler;
  private final ExecutorService bulkDispatcherExecutor;
  private final Time clock;
  private final Lock inFlightRequestLock = new ReentrantLock();
  private final Condition inFlightRequestsUpdated = inFlightRequestLock.newCondition();
  private final String esVersion;
  private volatile boolean closing = false;

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
    // One thread per possible concurrently-failing bulk: a re-add can park in
    // BulkIngester.add()/flush() (uninterruptibly, until buffer space or a request slot
    // frees), and independent retries must wait those parks out concurrently instead of
    // queueing behind each other on one thread. Mirrors the sizing of the ingester's own
    // internal retry pool.
    String threadPrefix = connectorName + "-" + taskId + "-elasticsearch-";
    ScheduledThreadPoolExecutor retryExecutor = new ScheduledThreadPoolExecutor(
        config.maxInFlightRequests(), namedDaemonThreadFactory(threadPrefix + "bulk-retry-"));
    // Start one thread eagerly so it is visible in thread dumps with its connector/task
    // name; the rest are created on demand.
    retryExecutor.prestartCoreThread();
    this.bulkRetryExecutor = retryExecutor;

    // The BulkIngester's flush timer and listener callbacks run on this scheduler.
    // Passing our own (instead of letting the ingester create an internal one) puts the
    // connector/task name on those threads, and lets closeResources() reclaim them even
    // when the ingester close is skipped for stuck records — which also stops the leaked
    // flush timer's "Error in background flush" logging. Sized like the ingester's own
    // default: the flush timer plus one listener callback per in-flight request.
    this.bulkIngesterScheduler = Executors.newScheduledThreadPool(
        config.maxInFlightRequests() + 1,
        namedDaemonThreadFactory(threadPrefix + "bulk-ingester-"));
    this.bulkDispatcherExecutor = Executors.newFixedThreadPool(
        config.maxInFlightRequests(), namedDaemonThreadFactory(threadPrefix + "bulk-dispatcher-"));

    ConfigCallbackHandler configCallbackHandler = new ConfigCallbackHandler(config);
    RestClient restClient = RestClient
        .builder(
            config.connectionUrls()
                .stream()
                .map(HttpHost::create)
                .collect(toList())
                .toArray(new HttpHost[config.connectionUrls().size()])
        ).setHttpClientConfigCallback(configCallbackHandler).build();

    this.jsonpMapper = new JacksonJsonpMapper();
    this.transport = new RestClientTransport(restClient, jsonpMapper);
    this.client = new co.elastic.clients.elasticsearch.ElasticsearchClient(transport);

    esVersion = getServerVersion();

    long lingerMs = config.lingerMs();
    if (lingerMs == 0) {
      // BulkIngester schedules its flush timer with scheduleWithFixedDelay, which rejects a
      // period <= 0. linger.ms=0 is a valid config that used to mean "flush immediately", so
      // treat it as a 1 ms flush interval instead of failing the task at startup.
      log.warn("{}=0 is treated as 1 ms (flush immediately); the Elasticsearch BulkIngester"
              + " does not support a zero flush interval.", LINGER_MS_CONFIG);
      lingerMs = 1;
    }
    long bulkSize = config.bulkSize();
    if (bulkSize == 0) {
      // BulkIngester treats only a negative max size as "unlimited"; a value of 0 would make
      // every add() wait forever for buffer space. bulk.size.bytes=0 used to flush on every
      // record, so treat it as 1 byte (every operation fills the batch) instead.
      log.warn("{}=0 is treated as 1 byte (flush every record); the Elasticsearch BulkIngester"
              + " does not support a zero bulk size.", BULK_SIZE_BYTES_CONFIG);
      bulkSize = 1;
    }
    final long flushIntervalMs = lingerMs;
    final long maxBulkSizeBytes = bulkSize;

    // The BulkIngester completes bulk request futures on the low-level rest client's I/O
    // reactor threads. Those threads can hold the http connection pool lock while the
    // ingester's own lock is held by a thread waiting for a pooled connection, which
    // deadlocks. Hop every bulk response onto a connector-owned dispatcher thread instead.
    // Must stay whenCompleteAsync (or handleAsync): thenApplyAsync would skip the executor
    // on exceptional completion, putting the failure path back on the I/O thread.
    ElasticsearchAsyncClient dispatchingAsyncClient = new ElasticsearchAsyncClient(transport) {
      @Override
      public CompletableFuture<BulkResponse> bulk(BulkRequest request) {
        return super.bulk(request).whenCompleteAsync((response, throwable) -> { },
            bulkDispatcherExecutor);
      }
    };

    this.bulkIngester = BulkIngester.of(builder -> builder
        .client(dispatchingAsyncClient)
        .maxOperations(config.batchSize())
        .maxSize(maxBulkSizeBytes)
        // Direct total cap on concurrent bulk requests. Before 16.0 the connector passed
        // maxInFlightRequests - 1 to BulkProcessor (whose parameter counted requests beyond
        // the one being built), so max.in.flight.requests=N effectively allowed N-1 requests;
        // as of 16.0 the config means what it says.
        .maxConcurrentRequests(config.maxInFlightRequests())
        .flushInterval(flushIntervalMs, TimeUnit.MILLISECONDS)
        // Connector-owned and named; the ingester will not close an external scheduler,
        // so closeResources() must (and does) shut it down.
        .scheduler(bulkIngesterScheduler)
        // Retries HTTP 429 (too many requests) per operation. All other bulk item errors are
        // handled by the listener; transport failures are retried by re-adding the batch.
        // max.retries=0 must map to noBackoff(): the ingester consults the policy's iterator
        // only after deciding to retry, so an empty exponential-backoff iterator throws
        // NoSuchElementException inside a discarded future, leaking an in-flight slot per 429.
        .backoffPolicy(config.maxRetries() > 0
            ? BackoffPolicy.exponentialBackoff(config.retryBackoffMs(), config.maxRetries())
            : BackoffPolicy.noBackoff())
        .listener(buildListener(afterBulkCallback)));
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

  private String getServerVersion() {
    String esVersionNumber = UNKNOWN_VERSION_TAG;
    try {
      esVersionNumber = client.info().version().number();
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
    closing = true;
    try {
      if (isFailed()) {
        // The task is failing: buffered records will not be committed, so don't send them.
        log.debug("Not flushing buffered records because the client has already failed.");
        return;
      }
      try {
        bulkIngester.flush();
      } catch (IllegalStateException e) {
        log.debug("Tried to flush data to Elasticsearch, but BulkIngester is already closed.", e);
      }
      if (!awaitBufferDrain(config.flushTimeoutMs())) {
        throw new ConnectException(
            "Failed to process outstanding requests in time while closing the ElasticsearchClient."
        );
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
        // Task cancellation interrupts this thread, but Time.SYSTEM's sleep swallows the
        // InterruptedException and re-sets the flag, so every further sleep returns
        // immediately and this loop busy-spins until the flush timeout. Abort right away
        // instead, like BulkProcessor.awaitClose used to; the flag stays set for callers.
        throw new ConnectException(
            "Interrupted while processing all in-flight requests on ElasticsearchClient close."
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
    // TypeMapping.properties() is empty for mappings that only define dynamic settings,
    // dynamic_templates or _meta, but such mappings must still count as existing so the
    // connector does not overwrite them. Serialize the mapping and compare against the empty
    // object, which matches what the old high-level client's sourceAsMap() check saw.
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
    final AtomicInteger transportRetryAttempts;

    BulkOpContext(SinkRecord sinkRecord, OffsetState offsetState, BulkOperation operation) {
      this.sinkRecord = sinkRecord;
      this.offsetState = offsetState;
      this.operation = operation;
      this.transportRetryAttempts = new AtomicInteger(0);
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

      // Hand-rolled transport-level retry: elasticsearch-java 8.x has no built-in retry
      // for whole-request failures (the ingester's backoffPolicy covers item-level 429s
      // only). Client 9.5.0+ retries these below the ingester via the transport's retry
      // config (RetryingHttpClient, elasticsearch-java#954) — when upgrading, configure
      // that instead and delete this retry machinery: this method's retry branch,
      // retryBulkOperations, bulkRetryExecutor, and BulkOpContext.transportRetryAttempts.
      @Override
      public void afterBulk(long executionId, BulkRequest request,
                            List<BulkOpContext> contexts, Throwable failure) {
        int attempt = contexts.stream()
            .mapToInt(context -> context.transportRetryAttempts.incrementAndGet())
            .max()
            .orElse(Integer.MAX_VALUE);
        if (attempt <= config.maxRetries() && !closing) {
          long backoffMs =
              RetryUtil.computeRandomRetryWaitTimeInMillis(attempt, config.retryBackoffMs());
          log.warn("Bulk request {} failed. Retrying attempt ({}/{}) after backoff of {} ms",
              executionId, attempt, config.maxRetries(), backoffMs, failure);
          try {
            bulkRetryExecutor.schedule(
                () -> retryBulkOperations(contexts), backoffMs, TimeUnit.MILLISECONDS);
            // Records stay buffered until the retried operations complete.
            return;
          } catch (RejectedExecutionException e) {
            log.warn("Could not schedule a retry for bulk request {}", executionId, e);
          }
        }
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

      private void retryBulkOperations(List<BulkOpContext> contexts) {
        try {
          for (BulkOpContext context : contexts) {
            bulkIngester.add(context.operation, context);
          }
          // add() only sends when the buffer crosses a batching threshold; a retried batch
          // is usually smaller, and nothing else flushes on its behalf until the linger
          // timer. The backoff has already been served by the scheduled delay, so flush
          // now — otherwise the retry waits up to a full linger.ms, close() times out on
          // deliverable records, and synchronous preCommit parks until the timer fires.
          // (BulkIngester's own 429 retry pairs its re-adds with scheduled flushes too.)
          bulkIngester.flush();
        } catch (Exception e) {
          log.warn("Failed to re-add bulk operations for retry", e);
          error.compareAndSet(null, new ConnectException("Bulk request failed", e));
          bulkFinished(contexts);
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
      // BulkIngester.close() waits for in-flight requests with no timeout. If records are stuck,
      // skip it and let closing the transport below abort them; the ingester threads are daemons.
      log.warn("Skipping bulk ingester close because {} records are still buffered; closing the "
              + "underlying transport will abort them.", numBufferedRecords.get());
    }

    try {
      // Closing the transport also closes the underlying low-level RestClient, aborting any
      // still-pending bulk requests. It must be closed while the dispatcher executor is still
      // alive: a bulk completing after the dispatcher is shut down has its async hop rejected,
      // so the ingester's lock-taking completion runs inline on the I/O reactor thread, which
      // recreates the reactor/ingester lock-order deadlock and hangs this method.
      transport.close();
    } catch (IOException e) {
      log.warn("Failed to close Elasticsearch client.", e);
    }

    bulkRetryExecutor.shutdown();
    bulkDispatcherExecutor.shutdown();
    // Also stops the ingester's flush timer when the ingester close was skipped above.
    bulkIngesterScheduler.shutdown();
    try {
      if (!bulkRetryExecutor.awaitTermination(CLOSE_WAIT_TIME_MS, TimeUnit.MILLISECONDS)) {
        bulkRetryExecutor.shutdownNow();
      }
      if (!bulkDispatcherExecutor.awaitTermination(CLOSE_WAIT_TIME_MS, TimeUnit.MILLISECONDS)) {
        bulkDispatcherExecutor.shutdownNow();
      }
      if (!bulkIngesterScheduler.awaitTermination(CLOSE_WAIT_TIME_MS, TimeUnit.MILLISECONDS)) {
        bulkIngesterScheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      bulkRetryExecutor.shutdownNow();
      bulkDispatcherExecutor.shutdownNow();
      bulkIngesterScheduler.shutdownNow();
      Thread.currentThread().interrupt();
      log.warn("Interrupted while awaiting for executor service shutdown.", e);
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
    return containsErrorType(e.error(), RESOURCE_ALREADY_EXISTS_EXCEPTION)
        || (e.getMessage() != null && e.getMessage().contains(RESOURCE_ALREADY_EXISTS_EXCEPTION));
  }

  /**
   * Checks whether the given error, or any error in its causal chain, matches the given
   * Elasticsearch error type, either structurally (error type) or in the error reason.
   *
   * @param cause the error cause returned by Elasticsearch, may be null
   * @param errorType the Elasticsearch error type to look for
   * @return true if the error matches the type
   */
  private static boolean containsErrorType(ErrorCause cause, String errorType) {
    for (ErrorCause current = cause; current != null; current = current.causedBy()) {
      if (errorType.equals(current.type())
          || (current.reason() != null && current.reason().contains(errorType))) {
        return true;
      }
    }
    return false;
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
    for (String error : MALFORMED_DOC_ERRORS) {
      if (containsErrorType(item.error(), error)) {
        reportBadRecordAndError(item, context);
        return handleMalformedDocResponse();
      }
    }
    if (containsErrorType(item.error(), VERSION_CONFLICT_EXCEPTION)) {
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
    if (containsErrorType(item.error(), VERSION_CONFLICT_EXCEPTION)
            && config.isDataStream()) {
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
