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
 * Wraps the client's HTTP layer to fix two things the stock {@code RestClientTransport} gets
 * wrong for this connector.
 *
 * <p>Outbound, the transport hands a bulk body down as one {@code ByteBuffer} per NDJSON line,
 * and {@code RestClientHttpClient} writes each buffer as its own HTTP chunk, TLS record and
 * syscall (thousands per bulk). That pinned the I/O reactor threads at ~2.5 cores and halved
 * throughput against the High Level REST Client. Merging the body into one buffer restores
 * the old framing.
 *
 * <p>Inbound, the delegate completes its future on an I/O reactor thread and the transport
 * decodes the response right there. Re-completing on the connector's dispatcher pool keeps
 * response handling off the threads the HTTP client needs to make progress.
 */
final class CoalescingHttpClient implements TransportHttpClient {

  private final TransportHttpClient delegate;
  private final Executor dispatcher;

  CoalescingHttpClient(TransportHttpClient delegate, Executor dispatcher) {
    this.delegate = delegate;
    this.dispatcher = dispatcher;
  }

  /**
   * Builds the connector's transport; closing it closes the {@code RestClient} beneath.
   */
  static ElasticsearchTransport transport(
      RestClient restClient,
      Executor dispatcher,
      JsonpMapper mapper
  ) {
    return new Transport(
        new CoalescingHttpClient(new RestClientHttpClient(restClient), dispatcher), mapper);
  }

  static final class Transport extends ElasticsearchTransportBase {
    Transport(TransportHttpClient httpClient, JsonpMapper mapper) {
      super(httpClient, null, mapper);
    }
  }

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
    // whenCompleteAsync, not thenApplyAsync: a failed upstream must hop too.
    upstream.whenCompleteAsync((response, failure) -> {
      if (failure != null) {
        result.completeExceptionally(failure);
      } else {
        result.complete(response);
      }
    }, dispatcher).exceptionally(hopFailure -> {
      // Dispatcher rejected the hop (shutdown): fail the future rather than leave it hanging.
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
      // Nothing to free.
    }
  }

  /**
   * Merges a multi-buffer body into one buffer. Reads through duplicates: the transport's
   * NDJSON separator is a shared buffer and must not be advanced.
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
