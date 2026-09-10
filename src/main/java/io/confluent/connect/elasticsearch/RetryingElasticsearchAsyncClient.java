/*
 * Copyright 2020 Confluent Inc.
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

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.transport.ElasticsearchTransport;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retries whole-request bulk failures (transport errors, non-2xx responses incl. a
 * whole-response 429) with jittered backoff, resending the same {@code BulkRequest}
 * verbatim. Item-level failures, 429s included, are not retried here — terminal,
 * handled by the listener.
 *
 * <p>The {@link #bulk(BulkRequest)} future stays incomplete until success or
 * exhausted retries, so {@code BulkIngester} holds its in-flight slot through every
 * backoff. At {@code max.in.flight.requests=1} this preserves record order across
 * retries.
 *
 * <p>Completion always hops through {@code dispatcherExecutor}, off the transport's
 * I/O reactor threads (see the deadlock note at this client's construction site).
 *
 * <p>Only {@link #bulk(BulkRequest)} carries this retry-and-dispatch contract. This class
 * extends the full generated {@code ElasticsearchAsyncClient} because {@code BulkIngester}'s
 * builder only accepts that concrete type, but every other inherited method (search, get,
 * ping, etc.) falls through to the default implementation with no retry and no dispatcher
 * hop. Do not call anything but {@code bulk()} on this instance — use the plain sync or
 * async client for anything else.
 *
 * <p>Superseded by elasticsearch-java 9.5+'s {@code RetryingHttpClient}
 * (elasticsearch-java#954) at that upgrade.
 */
class RetryingElasticsearchAsyncClient extends ElasticsearchAsyncClient {

  private static final Logger log =
      LoggerFactory.getLogger(RetryingElasticsearchAsyncClient.class);

  private final int maxRetries;
  private final long retryBackoffMs;
  private final ScheduledExecutorService retryExecutor;
  private final Executor dispatcherExecutor;
  private final Set<CompletableFuture<BulkResponse>> pendingFutures =
      ConcurrentHashMap.newKeySet();

  RetryingElasticsearchAsyncClient(
      ElasticsearchTransport transport,
      int maxRetries,
      long retryBackoffMs,
      ScheduledExecutorService retryExecutor,
      Executor dispatcherExecutor
  ) {
    super(transport);
    this.maxRetries = maxRetries;
    this.retryBackoffMs = retryBackoffMs;
    this.retryExecutor = retryExecutor;
    this.dispatcherExecutor = dispatcherExecutor;
  }

  @Override
  public CompletableFuture<BulkResponse> bulk(BulkRequest request) {
    CompletableFuture<BulkResponse> result = new CompletableFuture<>();
    pendingFutures.add(result);
    result.whenComplete((response, failure) -> pendingFutures.remove(result));
    attemptBulk(request, 1, result);
    return result;
  }

  /**
   * Completes every still-pending bulk future exceptionally with the given cause.
   *
   * <p>Called during close, after the retry executor is shut down: that shutdown discards
   * a retry queued mid-backoff without ever running it, which would otherwise leave its
   * future incomplete forever — the ingester's in-flight slot held and the listener's
   * buffer accounting never run. Completing a future that already completed is a no-op.
   */
  void failAllPending(Exception cause) {
    for (CompletableFuture<BulkResponse> pending : pendingFutures) {
      pending.completeExceptionally(cause);
    }
  }

  /**
   * The single seam through which every bulk request reaches the transport.
   *
   * @param request the bulk request to send
   * @return the transport's response future
   */
  protected CompletableFuture<BulkResponse> sendBulk(BulkRequest request) {
    return super.bulk(request);
  }

  private void attemptBulk(
      BulkRequest request,
      int attempt,
      CompletableFuture<BulkResponse> result
  ) {
    CompletableFuture<BulkResponse> sendFuture;
    try {
      sendFuture = sendBulk(request);
    } catch (Throwable t) {
      // The transport turns Exceptions into failed futures, but an Error escapes
      // synchronously; complete the future or the ingester's in-flight slot leaks.
      result.completeExceptionally(t);
      return;
    }
    sendFuture.handleAsync((response, failure) -> {
      if (failure == null) {
        result.complete(response);
        return null;
      }
      if (attempt > maxRetries) {
        result.completeExceptionally(new ConnectException(
            String.format("Bulk request failed after %d attempt(s)", attempt), failure));
        return null;
      }
      long backoffMs = RetryUtil.computeRandomRetryWaitTimeInMillis(attempt, retryBackoffMs);
      // WARN carries only the failure summary (RetryUtil's convention — the message can
      // embed the HTTP response body); the full stack goes to TRACE here and to the
      // terminal ConnectException on exhaustion. The denominator counts total attempts,
      // matching RetryUtil's (attempt/maxTotalAttempts).
      log.warn("Bulk request of {} operation(s) failed due to {}. Retrying attempt ({}/{})"
          + " after backoff of {} ms", request.operations().size(), failure, attempt,
          maxRetries + 1, backoffMs);
      log.trace("Bulk request failure detail:", failure);
      try {
        retryExecutor.schedule(
            () -> attemptBulk(request, attempt + 1, result), backoffMs, TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException e) {
        log.warn("Could not schedule a retry for a failed bulk request", e);
        result.completeExceptionally(failure);
      }
      return null;
    }, dispatcherExecutor).exceptionally(e -> {
      // Dispatcher hop rejected (shutdown race) — release the ingester's slot.
      result.completeExceptionally(e);
      return null;
    });
  }
}
