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

import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.Endpoint;
import co.elastic.clients.transport.TransportOptions;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Wraps an {@link ElasticsearchTransport} to retry transport-level failures (timeouts, connection
 * errors, non-2xx raised as exceptions) on the async path used by BulkIngester, with the same
 * jittered backoff as {@link RetryUtil}.
 *
 * <p>Only {@link #performRequestAsync} is retried; {@link #performRequest} is passed through
 * because its callers already retry via {@link RetryUtil#callWithRetries}, and a second layer here
 * would square the retry budget. Retries are scheduled (never slept) so no thread is held while
 * waiting.
 */
final class RetryingTransport implements ElasticsearchTransport {
  private final ElasticsearchTransport delegate;
  private final ScheduledExecutorService retryScheduler;
  private final int maxRetries;
  private final long retryBackoffMs;
  private final Set<CompletableFuture<?>> pendingRequests = ConcurrentHashMap.newKeySet();
  private volatile boolean closed = false;

  RetryingTransport(ElasticsearchTransport delegate, ScheduledExecutorService retryScheduler,
                     int maxRetries, long retryBackoffMs) {
    this.delegate = delegate;
    this.retryScheduler = retryScheduler;
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
    pendingRequests.add(result);
    result.whenComplete((resp, err) -> pendingRequests.remove(result));
    attempt(request, endpoint, options, result, 0);
    return result;
  }

  private <RequestT, ResponseT, ErrorT> void attempt(
      RequestT request, Endpoint<RequestT, ResponseT, ErrorT> endpoint,
      TransportOptions options, CompletableFuture<ResponseT> result, int retriesSoFar) {
    if (result.isDone()) {
      // failed by failPendingRequests() while this retry sat in the scheduler queue
      return;
    }
    delegate.performRequestAsync(request, endpoint, options).whenComplete((resp, err) -> {
      // Nothing may escape this callback: a throw lands in the discarded dependent stage,
      // leaving `result` pending forever — BulkIngester's request slot then leaks silently
      // and the pipeline stalls blaming a healthy Elasticsearch.
      try {
        if (err == null) {
          result.complete(resp);
        } else if (retriesSoFar < maxRetries && !closed) {
          // Seed with retriesSoFar + 1 (not retriesSoFar): RetryUtil increments before the
          // first call, so seeding with 0 here would halve every backoff versus the sync path.
          long backoffMs = RetryUtil.computeRandomRetryWaitTimeInMillis(
              retriesSoFar + 1, retryBackoffMs);
          try {
            retryScheduler.schedule(
                () -> attempt(request, endpoint, options, result, retriesSoFar + 1),
                backoffMs, TimeUnit.MILLISECONDS);
          } catch (RejectedExecutionException rejected) {
            // Scheduler already shut down. Complete the future instead of leaving it pending
            // forever: BulkIngester.close() waits on every in-flight request.
            err.addSuppressed(rejected);
            result.completeExceptionally(err);
          }
        } else {
          result.completeExceptionally(err);
        }
      } catch (Throwable callbackFailure) {
        if (err != null) {
          err.addSuppressed(callbackFailure);
          result.completeExceptionally(err);
        } else {
          result.completeExceptionally(callbackFailure);
        }
      }
    });
  }

  /**
   * Fails every request still awaiting a response or a scheduled retry, and stops scheduling new
   * retries. Must be called while the BulkIngester scheduler is still running: BulkIngester's
   * completion handling submits afterBulk to that scheduler before releasing the request slot,
   * so failing the futures after the scheduler shutdown would wedge BulkIngester.close() forever.
   */
  void failPendingRequests(Exception reason) {
    closed = true;
    for (CompletableFuture<?> pending : pendingRequests) {
      pending.completeExceptionally(reason);
    }
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
