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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RetryingTransportTest {

  private final ScheduledExecutorService retryScheduler =
      Executors.newSingleThreadScheduledExecutor();

  @After
  public void cleanup() {
    retryScheduler.shutdownNow();
  }

  /**
   * The request future must ALWAYS complete, no matter what happens inside the retry callback.
   * With retry.backoff.ms=0 (legal config), the backoff computation used to throw
   * IllegalArgumentException inside whenComplete; the exception landed in the discarded
   * dependent stage, the future never completed, and BulkIngester's request slot leaked
   * silently — the pipeline then stalled blaming a healthy Elasticsearch.
   */
  @Test
  public void testFutureCompletesAfterRetriesExhaustWithZeroBackoff() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    ElasticsearchTransport alwaysFailing = new FailingTransport(attempts);
    RetryingTransport transport =
        new RetryingTransport(alwaysFailing, retryScheduler, 3, 0L);

    CompletableFuture<Object> result = transport.performRequestAsync(null, null, null);

    ExecutionException e = assertThrows(ExecutionException.class,
        () -> result.get(10, TimeUnit.SECONDS));
    assertTrue(String.valueOf(e.getCause()), e.getCause() instanceof IOException);
    assertEquals(4, attempts.get()); // initial attempt + maxRetries
  }

  private static final class FailingTransport implements ElasticsearchTransport {
    private final AtomicInteger attempts;

    private FailingTransport(AtomicInteger attempts) {
      this.attempts = attempts;
    }

    @Override
    public <RequestT, ResponseT, ErrorT> ResponseT performRequest(
        RequestT request, Endpoint<RequestT, ResponseT, ErrorT> endpoint,
        TransportOptions options) throws IOException {
      throw new IOException("sync path not under test");
    }

    @Override
    public <RequestT, ResponseT, ErrorT> CompletableFuture<ResponseT> performRequestAsync(
        RequestT request, Endpoint<RequestT, ResponseT, ErrorT> endpoint,
        TransportOptions options) {
      attempts.incrementAndGet();
      CompletableFuture<ResponseT> failed = new CompletableFuture<>();
      failed.completeExceptionally(new IOException("simulated transport failure"));
      return failed;
    }

    @Override
    public JsonpMapper jsonpMapper() {
      return null;
    }

    @Override
    public TransportOptions options() {
      return null;
    }

    @Override
    public void close() {
    }
  }
}
