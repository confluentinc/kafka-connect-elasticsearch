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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Wraps a {@link co.elastic.clients.transport.Transport} to retry transport-level failures
 * (timeouts, connection errors, non-2xx responses raised as exceptions) on the async bulk-request
 * path, using the same jittered exponential backoff as {@link RetryUtil} elsewhere in the client.
 *
 * <p>Only {@link #performRequestAsync} is retried here -- that is the only path BulkIngester uses
 * (via its internal async client). The synchronous {@link #performRequest} path (used by
 * indexExists/createIndex/createMapping/hasMapping, via
 * {@link ElasticsearchSinkClient#callWithRetries}) is passed through unchanged: those calls already
 * have their own retry loop one layer up via
 * {@link RetryUtil#callWithRetries}, and retrying here too would double the retry budget for
 * every attempt (confirmed necessary by tracing PR #920's own history: combining
 * transport-level retry with BulkIngester's separate backoffPolicy compounds retries up to
 * (maxRetries+1)^2 attempts per document -- BulkIngester.backoffPolicy is deliberately never
 * configured in {@link ElasticsearchSinkClient} for the same reason).
 *
 * <p>Note on a design not adopted: PR #920 wraps the transport in a DispatchingTransport that
 * hands async completions to a dedicated executor, to break a lock-ordering deadlock between
 * BulkIngester's internal condition and the HTTP client's NIO threads. That was tried here and
 * measurably made no difference -- the integration-test failures it was expected to fix were
 * caused entirely by WireMock stubs omitting response fields the new client requires
 * (X-Elastic-Product header, ElasticsearchVersionInfo.buildFlavor), not by any deadlock. It was
 * therefore reverted rather than carried as unjustified machinery. If a genuine stall is ever
 * observed under load, that is the design to revisit.
 */
final class RetryingTransport implements ElasticsearchTransport {
  private final ElasticsearchTransport delegate;
  private final ScheduledExecutorService retryScheduler;
  private final int maxRetries;
  private final long retryBackoffMs;

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
    attempt(request, endpoint, options, result, 0);
    return result;
  }

  private <RequestT, ResponseT, ErrorT> void attempt(
      RequestT request, Endpoint<RequestT, ResponseT, ErrorT> endpoint,
      TransportOptions options, CompletableFuture<ResponseT> result, int retriesSoFar) {
    delegate.performRequestAsync(request, endpoint, options).whenComplete((resp, err) -> {
      if (err == null) {
        result.complete(resp);
      } else if (retriesSoFar < maxRetries) {
        // Seed with retriesSoFar + 1, not retriesSoFar: RetryUtil.callWithRetries increments its
        // attempt counter *before* invoking the callable, so it seeds the first retry with 1.
        // Passing retriesSoFar (0-based) here would halve every wait in the schedule relative to
        // the synchronous path and to the pre-migration behaviour -- e.g. with the defaults
        // (retry.backoff.ms=100, max.retries=5) mean total backoff before giving up would be
        // 1550 ms instead of 3100 ms, cutting the recovery window an overwhelmed cluster gets.
        long backoffMs = RetryUtil.computeRandomRetryWaitTimeInMillis(
            retriesSoFar + 1, retryBackoffMs);
        retryScheduler.schedule(
            () -> attempt(request, endpoint, options, result, retriesSoFar + 1),
            backoffMs, TimeUnit.MILLISECONDS);
      } else {
        result.completeExceptionally(err);
      }
    });
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
