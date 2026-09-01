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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.util.BinaryData;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RetryingElasticsearchAsyncClientTest {

  private ScheduledThreadPoolExecutor retryExecutor;
  private ExecutorService dispatcherExecutor;

  @Before
  public void setup() {
    retryExecutor = new ScheduledThreadPoolExecutor(1);
    dispatcherExecutor = Executors.newSingleThreadExecutor();
  }

  @After
  public void cleanup() {
    retryExecutor.shutdownNow();
    dispatcherExecutor.shutdownNow();
  }

  // The retry must re-send the identical request object, not a re-buffered copy.
  @Test
  public void testTransportFailureRetriesSameRequestWhileFutureStaysIncomplete()
      throws Exception {
    ScriptedClient client = client(2);
    client.willFail("connection reset");
    client.willRespond(okItem("a"));

    BulkRequest request = request(indexOp("a"));
    BulkResponse response = client.bulk(request).get(10, TimeUnit.SECONDS);

    assertEquals(1, response.items().size());
    assertNull(response.items().get(0).error());
    assertEquals(2, client.sends.size());
    assertSame(request, client.sends.get(0));
    assertSame(request, client.sends.get(1));
  }

  @Test
  public void testTransportFailureExhaustsRetryBudget() throws Exception {
    ScriptedClient client = client(2);
    client.willFail("failure 1");
    client.willFail("failure 2");
    client.willFail("failure 3");

    CompletableFuture<BulkResponse> result = client.bulk(request(indexOp("a")));

    ExecutionException e =
        assertThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS));
    assertTrue(String.valueOf(e.getCause()), e.getCause() instanceof ConnectException);
    assertEquals("Bulk request failed after 3 attempt(s)", e.getCause().getMessage());
    assertTrue(String.valueOf(e.getCause().getCause()),
        e.getCause().getCause() instanceof IOException);
    assertEquals("failure 3", e.getCause().getCause().getMessage());
    // 1 initial attempt + 2 retries.
    assertEquals(3, client.sends.size());
  }

  @Test
  public void testMaxRetriesZeroSendsExactlyOnce() throws Exception {
    ScriptedClient client = client(0);
    client.willFail("boom");

    CompletableFuture<BulkResponse> result = client.bulk(request(indexOp("a")));

    assertThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS));
    assertEquals(1, client.sends.size());
  }

  // A 200 response carrying per-item errors (including item-level 429s) is a success
  // at the transport level: it is returned to the ingester's listener verbatim, which
  // handles item failures terminally. It must not be retried here.
  @Test
  public void testItemLevelFailuresPassThroughUntouched() throws Exception {
    ScriptedClient client = client(2);
    BulkResponse original = response(okItem("a"), item429("b"), item400("c"));
    client.will(() -> CompletableFuture.completedFuture(original));

    BulkResponse response = client.bulk(request(indexOp("a"), indexOp("b"), indexOp("c")))
        .get(10, TimeUnit.SECONDS);

    assertSame(original, response);
    assertEquals(1, client.sends.size());
  }

  @Test
  public void testRejectedRetrySchedulingCompletesExceptionally() throws Exception {
    ScriptedClient client = client(2);
    client.willFail("boom");
    retryExecutor.shutdown();

    CompletableFuture<BulkResponse> result = client.bulk(request(indexOp("a")));

    ExecutionException e =
        assertThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS));
    assertEquals("boom", e.getCause().getMessage());
    assertEquals(1, client.sends.size());
  }

  // A future that never completes on its own: the same shape as a request stuck
  // in flight or a retry parked mid-backoff when close() discards it.
  @Test
  public void testFailAllPendingCompletesPendingFuture() throws Exception {
    ScriptedClient client = client(2);
    client.will(CompletableFuture::new);

    CompletableFuture<BulkResponse> result = client.bulk(request(indexOp("a")));
    assertTrue(!result.isDone());

    IOException cause = new IOException("client is closing");
    client.failAllPending(cause);

    ExecutionException e =
        assertThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS));
    assertSame(cause, e.getCause());
  }

  @Test
  public void testFailAllPendingLeavesCompletedFuturesUntouched() throws Exception {
    ScriptedClient client = client(2);
    BulkResponse original = response(okItem("a"));
    client.will(() -> CompletableFuture.completedFuture(original));

    CompletableFuture<BulkResponse> result = client.bulk(request(indexOp("a")));
    BulkResponse response = result.get(10, TimeUnit.SECONDS);
    assertSame(original, response);

    client.failAllPending(new IOException("client is closing"));

    // The completed future keeps its successful result.
    assertSame(original, result.get(10, TimeUnit.SECONDS));
  }

  // The response hop is rejected, so the retry decision never runs; the future the
  // ingester holds must still complete so its in-flight slot is released.
  @Test
  public void testDispatcherRejectionCompletesFuture() throws Exception {
    ScriptedClient client = client(2);
    client.willRespond(okItem("a"));
    dispatcherExecutor.shutdown();

    CompletableFuture<BulkResponse> result = client.bulk(request(indexOp("a")));

    assertThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS));
  }

  private ScriptedClient client(int maxRetries) {
    return new ScriptedClient(maxRetries, 1, retryExecutor, dispatcherExecutor);
  }

  /**
   * A client whose transport boundary is scripted: each expected send is answered by the
   * next entry in the script, and every request that reaches the transport is recorded.
   */
  private static class ScriptedClient extends RetryingElasticsearchAsyncClient {

    final Deque<Supplier<CompletableFuture<BulkResponse>>> script = new ArrayDeque<>();
    final List<BulkRequest> sends = Collections.synchronizedList(new ArrayList<>());

    ScriptedClient(int maxRetries, long retryBackoffMs,
        ScheduledThreadPoolExecutor retryExecutor, ExecutorService dispatcherExecutor) {
      super(mock(ElasticsearchTransport.class), maxRetries, retryBackoffMs,
          retryExecutor, dispatcherExecutor);
    }

    @Override
    protected CompletableFuture<BulkResponse> sendBulk(BulkRequest request) {
      sends.add(request);
      Supplier<CompletableFuture<BulkResponse>> next = script.pollFirst();
      if (next == null) {
        throw new AssertionError("Unexpected bulk send #" + sends.size());
      }
      return next.get();
    }

    void will(Supplier<CompletableFuture<BulkResponse>> outcome) {
      script.add(outcome);
    }

    void willFail(String message) {
      will(() -> {
        CompletableFuture<BulkResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IOException(message));
        return failed;
      });
    }

    void willRespond(BulkResponseItem... items) {
      will(() -> CompletableFuture.completedFuture(response(items)));
    }
  }

  private static BulkRequest request(BulkOperation... operations) {
    return BulkRequest.of(b -> b.operations(Arrays.asList(operations)));
  }

  private static BulkOperation indexOp(String id) {
    return BulkOperation.of(b -> b.index(i -> i
        .index("idx")
        .id(id)
        .document(BinaryData.of("{}".getBytes(UTF_8), "application/json"))));
  }

  private static BulkResponse response(BulkResponseItem... items) {
    boolean errors = Arrays.stream(items).anyMatch(item -> item.error() != null);
    return BulkResponse.of(b -> b.errors(errors).took(1).items(Arrays.asList(items)));
  }

  private static BulkResponseItem okItem(String id) {
    return BulkResponseItem.of(b -> b
        .operationType(OperationType.Index)
        .index("idx")
        .id(id)
        .status(201));
  }

  private static BulkResponseItem item429(String id) {
    return errorItem(id, 429, "es_rejected_execution_exception");
  }

  private static BulkResponseItem item400(String id) {
    return errorItem(id, 400, "some_terminal_exception");
  }

  private static BulkResponseItem errorItem(String id, int status, String type) {
    return BulkResponseItem.of(b -> b
        .operationType(OperationType.Index)
        .index("idx")
        .id(id)
        .status(status)
        .error(ErrorCause.of(e -> e.type(type).reason("Reason for " + type))));
  }
}
