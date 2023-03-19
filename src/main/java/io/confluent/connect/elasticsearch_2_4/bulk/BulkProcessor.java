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

package io.confluent.connect.elasticsearch_2_4.bulk;

import io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig;
import io.confluent.connect.elasticsearch_2_4.LogContext;
import io.confluent.connect.elasticsearch_2_4.RetryUtil;

import io.searchbox.core.BulkResult.BulkResultItem;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @param <R> record type
 * @param <B> bulk request type
 */
public class BulkProcessor<R, B> {

  private static final Logger log = LoggerFactory.getLogger(BulkProcessor.class);

  private static final AtomicLong BATCH_ID_GEN = new AtomicLong();

  private final Time time;
  private final BulkClient<R, B> bulkClient;
  private final int maxBufferedRecords;
  private final int batchSize;
  private final long lingerMs;
  private final int maxRetries;
  private final long retryBackoffMs;
  private final BehaviorOnMalformedDoc behaviorOnMalformedDoc;
  private final ErrantRecordReporter reporter;

  private final Thread farmer;
  private final ExecutorService executor;

  // thread-safe state, can be mutated safely without synchronization,
  // but may be part of synchronized(this) wait() conditions so need to notifyAll() on changes
  private volatile boolean stopRequested = false;
  private volatile boolean flushRequested = false;
  private final AtomicReference<ConnectException> error = new AtomicReference<>();

  // shared state, synchronized on (this), may be part of wait() conditions so need notifyAll() on
  // changes
  private final Deque<R> unsentRecords;
  protected final ConcurrentMap<R, SinkRecord> recordsToReportOnError; // visible for tests
  private int inFlightRecords = 0;
  private final LogContext logContext = new LogContext();

  public BulkProcessor(
      Time time,
      BulkClient<R, B> bulkClient,
      int maxBufferedRecords,
      int maxInFlightRequests,
      int batchSize,
      long lingerMs,
      int maxRetries,
      long retryBackoffMs,
      BehaviorOnMalformedDoc behaviorOnMalformedDoc,
      ErrantRecordReporter reporter
  ) {
    this.time = time;
    this.bulkClient = bulkClient;
    this.maxBufferedRecords = maxBufferedRecords;
    this.batchSize = batchSize;
    this.lingerMs = lingerMs;
    this.maxRetries = maxRetries;
    this.retryBackoffMs = retryBackoffMs;
    this.behaviorOnMalformedDoc = behaviorOnMalformedDoc;
    this.reporter = reporter;

    unsentRecords = new ArrayDeque<>(maxBufferedRecords);
    recordsToReportOnError = reporter != null
        ? new ConcurrentHashMap<>(maxBufferedRecords)
        : null;

    final ThreadFactory threadFactory = makeThreadFactory();
    farmer = threadFactory.newThread(farmerTask());
    executor = Executors.newFixedThreadPool(maxInFlightRequests, threadFactory);
  }

  private ThreadFactory makeThreadFactory() {
    final AtomicInteger threadCounter = new AtomicInteger();
    final Thread.UncaughtExceptionHandler uncaughtExceptionHandler =
        new Thread.UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread t, Throwable e) {
            log.error("Uncaught exception in BulkProcessor thread {}", t, e);
            failAndStop(e);
          }
        };
    return new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        final int threadId = threadCounter.getAndIncrement();
        final int objId = System.identityHashCode(this);
        final Thread t = new BulkProcessorThread(logContext, r, objId, threadId);
        t.setDaemon(true);
        t.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return t;
      }
    };
  }

  // visible for testing
  Runnable farmerTask() {
    return () -> {
      try (LogContext context = logContext.create("Farmer1")) {
        log.debug("Starting farmer task");
        try {
          List<Future<BulkResponse>> futures = new ArrayList<>();
          while (!stopRequested) {
            // submitBatchWhenReady waits for lingerMs so we won't spin here unnecessarily
            futures.add(submitBatchWhenReady());

            // after we submit, look at any previous futures that were completed and call get() on
            // them so that exceptions are propagated.
            List<Future<BulkResponse>> unfinishedFutures = new ArrayList<>();
            for (Future<BulkResponse> f : futures) {
              if (f.isDone()) {
                BulkResponse resp = f.get();
                log.debug("Bulk request completed with status {}", resp);
              } else {
                unfinishedFutures.add(f);
              }
            }
            log.debug("Processing next batch with {} outstanding batch requests in flight",
                    unfinishedFutures.size());
            futures = unfinishedFutures;
          }
        } catch (InterruptedException | ExecutionException e) {
          throw new ConnectException(e);
        }
        log.debug("Finished farmer task");
      }
    };
  }

  // Visible for testing
  synchronized Future<BulkResponse> submitBatchWhenReady() throws InterruptedException {
    for (long waitStartTimeMs = time.milliseconds(), elapsedMs = 0;
         !stopRequested && !canSubmit(elapsedMs);
         elapsedMs = time.milliseconds() - waitStartTimeMs) {
      // when linger time has already elapsed, we still have to ensure the other submission
      // conditions hence the wait(0) in that case
      wait(Math.max(0, lingerMs - elapsedMs));
    }

    // at this point, either stopRequested or canSubmit
    return stopRequested
            ? CompletableFuture.completedFuture(
                    BulkResponse.failure(
                        false,
                        "request not submitted during shutdown",
                        Collections.emptyMap()
                    )
            )
            : submitBatch();
  }

  private synchronized Future<BulkResponse> submitBatch() {
    final int numUnsentRecords = unsentRecords.size();
    assert numUnsentRecords > 0;
    final int batchableSize = Math.min(batchSize, numUnsentRecords);
    final List<R> batch = new ArrayList<>(batchableSize);
    for (int i = 0; i < batchableSize; i++) {
      batch.add(unsentRecords.removeFirst());
    }
    inFlightRecords += batchableSize;
    log.debug(
        "Submitting batch of {} records; {} unsent and {} total in-flight records",
        batchableSize,
        numUnsentRecords,
        inFlightRecords
    );
    return executor.submit(new BulkTask(batch));
  }

  /**
   * Submission is possible when there are unsent records and:
   * <ul>
   * <li>flush is called, or</li>
   * <li>the linger timeout passes, or</li>
   * <li>there are sufficient records to fill a batch</li>
   * </ul>
   */
  private synchronized boolean canSubmit(long elapsedMs) {
    return !unsentRecords.isEmpty()
           && (flushRequested || elapsedMs >= lingerMs || unsentRecords.size() >= batchSize);
  }

  /**
   * Start concurrently creating and sending batched requests using the client.
   */
  public void start() {
    farmer.start();
  }

  /**
   * Initiate shutdown.
   *
   * <p>Pending buffered records are not automatically flushed, so call {@link #flush(long)} before
   * this method if this is desirable.
   */
  public void stop() {
    log.debug("stop");
    stopRequested = true; // this stops the farmer task
    synchronized (this) {
      // shutdown the pool under synchronization to avoid rejected submissions
      executor.shutdown();
      notifyAll();
    }
  }

  /**
   * Block upto {@code timeoutMs} till shutdown is complete.
   *
   * <p>This should only be called after a previous {@link #stop()} invocation.
   */
  public void awaitStop(long timeoutMs) {
    assert stopRequested;
    try {
      if (!executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
        throw new ConnectException("Timed-out waiting for executor termination");
      }
    } catch (InterruptedException e) {
      throw new ConnectException(e);
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * @return whether {@link #stop()} has been requested
   */
  public boolean isStopping() {
    return stopRequested;
  }

  /**
   * @return whether any task failed with an error
   */
  public boolean isFailed() {
    return error.get() != null;
  }

  /**
   * @return {@link #isTerminal()} or {@link #isFailed()}
   */
  public boolean isTerminal() {
    return isStopping() || isFailed();
  }

  /**
   * Throw a {@link ConnectException} if {@link #isStopping()}.
   */
  public void throwIfStopping() {
    if (stopRequested) {
      throw new ConnectException("Stopping");
    }
  }

  /**
   * Throw the relevant {@link ConnectException} if {@link #isFailed()}.
   */
  public void throwIfFailed() {
    if (isFailed()) {
      throw error.get();
    }
  }

  /**
   * {@link #throwIfFailed()} and {@link #throwIfStopping()}
   */
  public void throwIfTerminal() {
    throwIfFailed();
    throwIfStopping();
  }

  /**
   * Add a record, may block upto {@code timeoutMs} if at capacity with respect to
   * {@code maxBufferedRecords}.
   *
   * <p>If any task has failed prior to or while blocked in the add, or if the timeout expires
   * while blocked, {@link ConnectException} will be thrown.
   */
  public synchronized void add(R record, SinkRecord original, long timeoutMs) {
    throwIfTerminal();

    int numBufferedRecords = bufferedRecords();
    if (numBufferedRecords >= maxBufferedRecords) {
      log.trace(
          "Buffer full at {} records, so waiting up to {} ms before adding",
          numBufferedRecords,
          timeoutMs
      );
      final long addStartTimeMs = time.milliseconds();
      for (long elapsedMs = time.milliseconds() - addStartTimeMs;
           !isTerminal() && elapsedMs < timeoutMs && bufferedRecords() >= maxBufferedRecords;
           elapsedMs = time.milliseconds() - addStartTimeMs) {
        try {
          wait(timeoutMs - elapsedMs);
        } catch (InterruptedException e) {
          throw new ConnectException(e);
        }
      }
      throwIfTerminal();
      if (bufferedRecords() >= maxBufferedRecords) {
        throw new ConnectException("Add timeout expired before buffer availability");
      }
      log.debug(
          "Adding record to queue after waiting {} ms",
          time.milliseconds() - addStartTimeMs
      );
    } else {
      log.trace("Adding record to queue");
    }

    unsentRecords.addLast(record);
    addRecordToReport(record, original);
    notifyAll();
  }

  /**
   * Request a flush and block upto {@code timeoutMs} until all pending records have been flushed.
   *
   * <p>If any task has failed prior to or during the flush, {@link ConnectException} will be
   * thrown with that error.
   */
  public void flush(long timeoutMs) {
    final long flushStartTimeMs = time.milliseconds();
    try {
      flushRequested = true;
      synchronized (this) {
        notifyAll();
        for (long elapsedMs = time.milliseconds() - flushStartTimeMs;
             !isTerminal() && elapsedMs < timeoutMs && bufferedRecords() > 0;
             elapsedMs = time.milliseconds() - flushStartTimeMs) {
          wait(timeoutMs - elapsedMs);
        }
        throwIfTerminal();
        if (bufferedRecords() > 0) {
          throw new ConnectException("Flush timeout expired with unflushed records: "
                                     + bufferedRecords());
        }
      }
    } catch (InterruptedException e) {
      throw new ConnectException(e);
    } finally {
      flushRequested = false;
    }
    log.debug("Flushed bulk processor (total time={} ms)", time.milliseconds() - flushStartTimeMs);
  }

  private void addRecordToReport(R record, SinkRecord original) {
    if (reporter != null) {
      // avoid unnecessary operations if not using the reporter
      recordsToReportOnError.put(record, original);
    }
  }

  private void removeReportedRecords(List<R> batch) {
    if (reporter != null) {
      // avoid unnecessary operations if not using the reporter
      recordsToReportOnError.keySet().removeAll(batch);
    }
  }

  private static final class BulkProcessorThread extends Thread {

    private final LogContext parentContext;
    private final int threadId;

    public BulkProcessorThread(
        LogContext parentContext,
        Runnable target,
        int objId,
        int threadId
    ) {
      super(target, String.format("BulkProcessor@%d-%d", objId, threadId));
      this.parentContext = parentContext;
      this.threadId = threadId;
    }

    @Override
    public void run() {
      try (LogContext context = parentContext.create("Thread" + threadId)) {
        super.run();
      }
    }
  }

  private final class BulkTask implements Callable<BulkResponse> {

    final long batchId = BATCH_ID_GEN.incrementAndGet();

    final List<R> batch;

    BulkTask(List<R> batch) {
      this.batch = batch;
    }

    @Override
    public BulkResponse call() throws Exception {
      final BulkResponse rsp;
      try {
        rsp = execute();
      } catch (Exception e) {
        failAndStop(e);
        throw e;
      }
      onBatchCompletion(batch.size());
      return rsp;
    }

    private BulkResponse execute() throws Exception {
      final long startTime = System.currentTimeMillis();
      final B bulkReq;
      try {
        bulkReq = bulkClient.bulkRequest(batch);
      } catch (Exception e) {
        log.error(
            "Failed to create bulk request from batch {} of {} records",
            batchId,
            batch.size(),
            e
        );
        removeReportedRecords(batch);
        throw e;
      }
      final int maxAttempts = maxRetries + 1;
      for (int attempts = 1, retryAttempts = 0; true; ++attempts, ++retryAttempts) {
        boolean retriable = true;
        try {
          log.trace("Executing batch {} of {} records with attempt {}/{}",
                  batchId, batch.size(), attempts, maxAttempts);
          final BulkResponse bulkRsp = bulkClient.execute(bulkReq);
          if (bulkRsp.isSucceeded()) {
            if (log.isDebugEnabled()) {
              log.debug(
                  "Completed batch {} of {} records with attempt {}/{} in {} ms",
                  batchId,
                  batch.size(),
                  attempts,
                  maxAttempts,
                  System.currentTimeMillis() - startTime
              );
            }
            removeReportedRecords(batch);
            return bulkRsp;
          } else if (responseContainsMalformedDocError(bulkRsp)) {
            retriable = bulkRsp.isRetriable();
            handleMalformedDoc(bulkRsp);
            if (reporter != null) {
              for (R record : batch) {
                SinkRecord original = recordsToReportOnError.get(record);
                BulkResultItem result = bulkRsp.failedRecords.get(record);
                String error = result != null ? result.error : null;
                if (error != null && original != null) {
                  reporter.report(
                      original, new ReportingException("Bulk request failed: " + error)
                  );
                }
              }
            }
            removeReportedRecords(batch);
            return bulkRsp;
          } else {
            // for all other errors, throw the error up
            retriable = bulkRsp.isRetriable();
            throw new ConnectException("Bulk request failed: " + bulkRsp.getErrorInfo());
          }
        } catch (Exception e) {
          if (retriable && attempts < maxAttempts) {
            long sleepTimeMs = RetryUtil.computeRandomRetryWaitTimeInMillis(retryAttempts,
                    retryBackoffMs);
            log.warn("Failed to execute batch {} of {} records with attempt {}/{}, "
                      + "will attempt retry after {} ms. Failure reason: {}",
                      batchId, batch.size(), attempts, maxAttempts, sleepTimeMs, e.getMessage());
            time.sleep(sleepTimeMs);
            if (Thread.interrupted()) {
              log.error(
                      "Retrying batch {} of {} records interrupted after attempt {}/{}",
                      batchId, batch.size(), attempts, maxAttempts, e);
              removeReportedRecords(batch);
              throw e;
            }
          } else {
            log.error("Failed to execute batch {} of {} records after total of {} attempt(s)",
                    batchId, batch.size(), attempts, e);
            removeReportedRecords(batch);
            throw e;
          }
        }
      }
    }

    private void handleMalformedDoc(BulkResponse bulkRsp) {
      // if the elasticsearch request failed because of a malformed document,
      // the behavior is configurable.
      switch (behaviorOnMalformedDoc) {
        case IGNORE:
          log.debug("Encountered an illegal document error when executing batch {} of {}"
                  + " records. Ignoring and will not index record. Error was {}",
              batchId, batch.size(), bulkRsp.getErrorInfo());
          return;
        case WARN:
          log.warn("Encountered an illegal document error when executing batch {} of {}"
                  + " records. Ignoring and will not index record. Error was {}",
              batchId, batch.size(), bulkRsp.getErrorInfo());
          return;
        case FAIL:
          log.error("Encountered an illegal document error when executing batch {} of {}"
                  + " records. Error was {} (to ignore future records like this"
                  + " change the configuration property '{}' from '{}' to '{}').",
              batchId, batch.size(), bulkRsp.getErrorInfo(),
              ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG,
              BehaviorOnMalformedDoc.FAIL,
              BehaviorOnMalformedDoc.IGNORE);
          throw new ConnectException("Bulk request failed: " + bulkRsp.getErrorInfo());
        default:
          throw new RuntimeException(String.format(
              "Unknown value for %s enum: %s",
              BehaviorOnMalformedDoc.class.getSimpleName(),
              behaviorOnMalformedDoc
          ));
      }
    }
  }

  private boolean responseContainsMalformedDocError(BulkResponse bulkRsp) {
    return bulkRsp.getErrorInfo().contains("mapper_parsing_exception")
        || bulkRsp.getErrorInfo().contains("illegal_argument_exception")
        || bulkRsp.getErrorInfo().contains("action_request_validation_exception");
  }

  private synchronized void onBatchCompletion(int batchSize) {
    inFlightRecords -= batchSize;
    assert inFlightRecords >= 0;
    notifyAll();
  }

  private void failAndStop(Throwable t) {
    error.compareAndSet(null, toConnectException(t));
    stop();
  }

  /**
   * @return sum of unsent and in-flight record counts
   */
  public synchronized int bufferedRecords() {
    return unsentRecords.size() + inFlightRecords;
  }

  private static ConnectException toConnectException(Throwable t) {
    if (t instanceof ConnectException) {
      return (ConnectException) t;
    } else {
      return new ConnectException(t);
    }
  }

  public enum BehaviorOnMalformedDoc {
    IGNORE,
    WARN,
    FAIL;

    public static final BehaviorOnMalformedDoc DEFAULT = FAIL;

    // Want values for "behavior.on.malformed.doc" property to be case-insensitive
    public static final ConfigDef.Validator VALIDATOR = new ConfigDef.Validator() {
      private final ConfigDef.ValidString validator = ConfigDef.ValidString.in(names());

      @Override
      public void ensureValid(String name, Object value) {
        if (value instanceof String) {
          value = ((String) value).toLowerCase(Locale.ROOT);
        }
        validator.ensureValid(name, value);
      }

      // Overridden here so that ConfigDef.toEnrichedRst shows possible values correctly
      @Override
      public String toString() {
        return validator.toString();
      }

    };

    public static String[] names() {
      BehaviorOnMalformedDoc[] behaviors = values();
      String[] result = new String[behaviors.length];

      for (int i = 0; i < behaviors.length; i++) {
        result[i] = behaviors[i].toString();
      }

      return result;
    }

    @Override
    public String toString() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  /**
   * Exception that hides the stack trace used for reporting errors from Elasticsearch
   * (mapper_parser_exception, illegal_argument_exception, and action_request_validation_exception)
   * resulting from bad records using the AK 2.6 reporter DLQ interface because the error did not
   * come from that line due to multithreading.
   */
  @SuppressWarnings("serial")
  public static class ReportingException extends RuntimeException {

    public ReportingException(String message) {
      super(message);
    }

    /**
     * This method is overriden to swallow the stack trace.
     *
     * @return Throwable
     */
    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }
}
