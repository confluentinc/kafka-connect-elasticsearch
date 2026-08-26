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
 * An {@link ElasticsearchAsyncClient} whose {@link #bulk(BulkRequest)} future does not
 * complete until the request has either succeeded or exhausted its retry budget.
 *
 * <p>Keeping the future incomplete across retries is what preserves record ordering: the
 * {@code BulkIngester} counts a request as in flight until this future completes, so at
 * {@code max.in.flight.requests=1} nothing buffered during a backoff can be sent before
 * the failed request is retried. That reproduces the pre-migration behavior, where
 * transport failures were retried while the BulkProcessor's concurrency permit was still
 * held; the previous approach of re-queuing the operations into the ingester released the
 * slot and let newer records overtake a retried (possibly stale) write.
 *
 * <p>Only whole-request failures are retried here — transport errors and non-2xx bulk
 * responses such as an HTTP 429 from the coordinating node, which reach this client as an
 * exceptionally completed future. The same {@code BulkRequest} is re-sent verbatim; its
 * payloads are byte-array backed, so re-serialization is repeatable. This matches master,
 * which wrapped only the whole {@code client.bulk(...)} call in retries and left per-item
 * failures (including item-level 429s inside a 200 response) terminal, handled by the
 * listener. The low-level RestClient already fails over across configured nodes within a
 * single attempt, so one attempt here can mean several node tries below.
 *
 * <p>Every completion path hops through the dispatcher executor so the ingester's
 * lock-taking continuation never runs on the transport's I/O reactor threads (see the
 * deadlock note where this client is constructed). When upgrading to elasticsearch-java
 * 9.5+, its transport-level retry (RetryingHttpClient, elasticsearch-java#954) can
 * replace this class entirely.
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
    // handleAsync (not whenCompleteAsync): it runs the callback on the dispatcher for
    // both outcomes — keeping the failure path off the transport's I/O reactor threads —
    // and completes its own stage with the callback's return value, swallowing the
    // upstream exception. The trailing exceptionally therefore fires only when the
    // callback never ran (the dispatcher rejected the hop during shutdown) or threw,
    // never for an ordinary transport failure the callback already scheduled a retry for.
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
      // The dispatcher rejected the hop (a race with executor shutdown), so the retry
      // decision never ran: complete the future the BulkIngester holds so its in-flight
      // slot is released.
      result.completeExceptionally(e);
      return null;
    });
  }
}
