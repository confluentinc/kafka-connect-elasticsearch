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

import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.ElasticsearchTransportBase;
import co.elastic.clients.transport.TransportOptions;
import co.elastic.clients.transport.http.TransportHttpClient;
import co.elastic.clients.transport.rest_client.RestClientHttpClient;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.elasticsearch.client.RestClient;

/**
 * Decorates the Java API client's HTTP layer at the {@link TransportHttpClient} seam, the
 * boundary between the typed transport ({@link ElasticsearchTransportBase}: JSON encode and
 * decode) and the byte-level {@link RestClientHttpClient}. It does two things there.
 *
 * <p><b>Outbound: coalesce the request body into one buffer.</b> The transport hands a
 * bulk request down as one {@code ByteBuffer} per NDJSON line plus a shared one-byte
 * separator, four buffers per index operation. {@code RestClientHttpClient} wraps that
 * iterable in a chunked {@code MultiBufferEntity} that writes exactly one buffer per
 * {@code produceContent} call, and httpcore-nio makes one such call per writable event.
 * Each buffer therefore becomes its own HTTP chunk, TLS record and {@code write()} syscall:
 * thousands per bulk instead of the ~20 the High Level REST Client produced from a single
 * byte-array entity. Measured on a 3-core worker this pinned the four I/O reactor threads
 * at ~2.5 cores and capped throughput at half of the old client's. With one merged buffer the
 * chunk encoder fills its session buffer per event and the reactor cost returns to parity.
 * The copy costs one extra pass over the body on the calling thread. Bodies of zero or one
 * buffer (every non-bulk request) pass through untouched.
 *
 * <p><b>Inbound: complete the response future on the connector's dispatcher pool.</b> The
 * delegate completes its future on an I/O reactor thread, and the transport's own
 * continuation decodes the JSON response right there, before any connector code runs.
 * Re-completing on {@code dispatcher} moves that decode, and everything downstream of it
 * (listener callbacks, retry scheduling, offset bookkeeping), off the reactor for every
 * async endpoint. This is the deadlock guard described at the connector's dispatcher pool:
 * connector callbacks must never run on a thread that the HTTP client needs to make
 * progress, and must not share a pool with the ingester's flush scheduler either
 * ({@link RetryingElasticsearchAsyncClient} keeps its own hop as a second line of defence).
 * Success and failure both hop; a dispatcher that rejects the hop (shutdown race) fails the
 * future rather than leaving it incomplete, and cancelling the returned future cancels the
 * in-flight HTTP request as the {@link TransportHttpClient} contract requires.
 */
final class CoalescingHttpClient implements TransportHttpClient {

  private final TransportHttpClient delegate;
  private final Executor dispatcher;

  CoalescingHttpClient(TransportHttpClient delegate, Executor dispatcher) {
    this.delegate = delegate;
    this.dispatcher = dispatcher;
  }

  /**
   * Builds the connector's transport: the stock REST client HTTP layer wrapped by this
   * decorator, beneath the stock typed transport. Closing the transport closes the
   * {@code RestClient} beneath it, as with {@code RestClientTransport}.
   */
  static ElasticsearchTransport transport(
      RestClient restClient,
      Executor dispatcher,
      JsonpMapper mapper
  ) {
    return new Transport(
        new CoalescingHttpClient(new RestClientHttpClient(restClient), dispatcher), mapper);
  }

  /** Named rather than anonymous so stack traces and thread dumps identify it. */
  static final class Transport extends ElasticsearchTransportBase {
    Transport(TransportHttpClient httpClient, JsonpMapper mapper) {
      super(httpClient, null, mapper);
    }
  }

  // Must forward: RestClientHttpClient's options carry the client's default headers.
  @Override
  public TransportOptions createOptions(TransportOptions options) {
    return delegate.createOptions(options);
  }

  @Override
  public Response performRequest(
      String endpointId,
      Node node,
      Request request,
      TransportOptions options
  ) throws IOException {
    return delegate.performRequest(endpointId, node, coalesce(request), options);
  }

  @Override
  public CompletableFuture<Response> performRequestAsync(
      String endpointId,
      Node node,
      Request request,
      TransportOptions options
  ) {
    CompletableFuture<Response> upstream =
        delegate.performRequestAsync(endpointId, node, coalesce(request), options);
    CompletableFuture<Response> result = new CompletableFuture<Response>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        boolean cancelled = super.cancel(mayInterruptIfRunning);
        if (cancelled) {
          upstream.cancel(mayInterruptIfRunning);
        }
        return cancelled;
      }
    };
    // whenCompleteAsync (not thenApplyAsync): an exceptional upstream must hop too, or the
    // failure path would run the transport's continuation on the reactor thread.
    upstream.whenCompleteAsync((response, failure) -> {
      if (failure != null) {
        result.completeExceptionally(failure);
      } else {
        result.complete(response);
      }
    }, dispatcher).exceptionally(hopFailure -> {
      // Reached for an upstream failure (result already completed: no-op) and for a
      // rejected hop (dispatcher shut down): fail the future so no slot leaks, and release
      // the response the transport will now never consume.
      if (result.completeExceptionally(hopFailure)) {
        closeQuietly(upstream);
      }
      return null;
    });
    return result;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  private static void closeQuietly(CompletableFuture<Response> completed) {
    try {
      Response response = completed.getNow(null);
      if (response != null) {
        response.close();
      }
    } catch (Exception ignored) {
      // The future failed or the body was already released; nothing to free.
    }
  }

  /**
   * Returns a request whose body is a single buffer holding the same bytes, or the request
   * itself when the body is absent or already a single buffer. The source buffers are read
   * through duplicates: the transport's NDJSON separator is one shared buffer, and advancing
   * it would corrupt every later request.
   */
  static Request coalesce(Request request) {
    Iterable<ByteBuffer> body = request.body();
    if (body == null) {
      return request;
    }
    List<ByteBuffer> buffers = new ArrayList<>();
    int size = 0;
    for (ByteBuffer buffer : body) {
      buffers.add(buffer);
      size += buffer.remaining();
    }
    if (buffers.size() <= 1) {
      return request;
    }
    ByteBuffer merged = ByteBuffer.allocate(size);
    for (ByteBuffer buffer : buffers) {
      merged.put(buffer.duplicate());
    }
    merged.flip();
    return new Request(
        request.method(),
        request.path(),
        request.queryParams(),
        request.headers(),
        Collections.singletonList(merged));
  }
}
