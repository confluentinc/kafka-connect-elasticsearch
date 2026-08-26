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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
    attemptBulk(request, 1, result);
    return result;
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
    sendBulk(request).handleAsync((response, failure) -> {
      if (failure == null) {
        result.complete(response);
        return null;
      }
      if (attempt > maxRetries) {
        result.completeExceptionally(failure);
        return null;
      }
      long backoffMs = RetryUtil.computeRandomRetryWaitTimeInMillis(attempt, retryBackoffMs);
      log.warn("Bulk request of {} operation(s) failed. Retrying attempt ({}/{}) after"
          + " backoff of {} ms", request.operations().size(), attempt, maxRetries,
          backoffMs, failure);
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
