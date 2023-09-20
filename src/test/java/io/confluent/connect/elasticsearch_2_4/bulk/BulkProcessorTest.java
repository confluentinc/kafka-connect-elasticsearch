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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.confluent.connect.elasticsearch_2_4.IndexableRecord;
import io.confluent.connect.elasticsearch_2_4.Key;
import io.searchbox.client.JestResult;
import io.searchbox.core.BulkResult;
import io.searchbox.core.BulkResult.BulkResultItem;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static io.confluent.connect.elasticsearch_2_4.bulk.BulkProcessor.BehaviorOnMalformedDoc;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BulkProcessorTest {

  private static final String INDEX = "topic";
  private static final String TYPE = "type";

  private static class Expectation {
    final List<Integer> request;
    final BulkResponse response;

    private Expectation(List<Integer> request, BulkResponse response) {
      this.request = request;
      this.response = response;
    }
  }

  private static final class Client implements BulkClient<IndexableRecord, List<Integer>> {
    private final Queue<Expectation> expectQ = new LinkedList<>();
    private volatile boolean executeMetExpectations = true;

    @Override
    public List<Integer> bulkRequest(List<IndexableRecord> batch) {
      List<Integer> ids = new ArrayList<>(batch.size());
      for (IndexableRecord id : batch) {
        ids.add(Integer.valueOf(id.key.id));
      }
      return ids;
    }

    public void expect(List<Integer> ids, BulkResponse response) {
      expectQ.add(new Expectation(ids, response));
    }

    public boolean expectationsMet() {
      return expectQ.isEmpty() && executeMetExpectations;
    }

    @Override
    public BulkResponse execute(List<Integer> request) {
      final Expectation expectation;
      try {
        expectation = expectQ.remove();
        assertEquals(expectation.request, request);
      } catch (Throwable t) {
        executeMetExpectations = false;
        throw t;
      }
      executeMetExpectations &= true;
      return expectation.response;
    }
  }

  Client client;

  @Before
  public void createClient() {
    client = new Client();
  }

  @After
  public void checkClient() {
    assertTrue(client.expectationsMet());
  }

  @Test
  public void batchingAndLingering() throws InterruptedException, ExecutionException {
    final int maxBufferedRecords = 100;
    final int maxInFlightBatches = 5;
    final int batchSize = 5;
    final int lingerMs = 5;
    final int maxRetries = 0;
    final int retryBackoffMs = 0;
    final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.DEFAULT;
    final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    final BulkProcessor<IndexableRecord, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        maxBufferedRecords,
        maxInFlightBatches,
        batchSize,
        lingerMs,
        maxRetries,
        retryBackoffMs,
        behaviorOnMalformedDoc,
        reporter
    );

    final int addTimeoutMs = 10;
    for (int i = 1; i < 13; i++) {
      bulkProcessor.add(indexableRecord(i), sinkRecord(), addTimeoutMs);
    }

    client.expect(Arrays.asList(1, 2, 3, 4, 5), BulkResponse.success());
    client.expect(Arrays.asList(6, 7, 8, 9, 10), BulkResponse.success());
    client.expect(Arrays.asList(11, 12), BulkResponse.success()); // batch not full, but upon linger timeout
    assertTrue(bulkProcessor.submitBatchWhenReady().get().succeeded);
    assertTrue(bulkProcessor.submitBatchWhenReady().get().succeeded);
    assertTrue(bulkProcessor.submitBatchWhenReady().get().succeeded);
    assertTrue(bulkProcessor.recordsToReportOnError.isEmpty());
    verify(reporter, never()).report(eq(sinkRecord()), any());
  }

  @Test
  public void flushing() {
    final int maxBufferedRecords = 100;
    final int maxInFlightBatches = 5;
    final int batchSize = 5;
    final int lingerMs = 100000; // super high on purpose to make sure flush is what's causing the request
    final int maxRetries = 0;
    final int retryBackoffMs = 0;
    final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.DEFAULT;
    final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    final BulkProcessor<IndexableRecord, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        maxBufferedRecords,
        maxInFlightBatches,
        batchSize,
        lingerMs,
        maxRetries,
        retryBackoffMs,
        behaviorOnMalformedDoc,
        reporter
    );

    client.expect(Arrays.asList(1, 2, 3), BulkResponse.success());

    bulkProcessor.start();

    final int addTimeoutMs = 10;
    bulkProcessor.add(indexableRecord(1), sinkRecord(), addTimeoutMs);
    bulkProcessor.add(indexableRecord(2), sinkRecord(), addTimeoutMs);
    bulkProcessor.add(indexableRecord(3), sinkRecord(), addTimeoutMs);

    assertFalse(client.expectationsMet());

    final int flushTimeoutMs = 100;
    bulkProcessor.flush(flushTimeoutMs);
    assertTrue(bulkProcessor.recordsToReportOnError.isEmpty());
    verify(reporter, never()).report(eq(sinkRecord()), any());
  }

  @Test
  public void addBlocksWhenBufferFull() {
    final int maxBufferedRecords = 1;
    final int maxInFlightBatches = 1;
    final int batchSize = 1;
    final int lingerMs = 10;
    final int maxRetries = 0;
    final int retryBackoffMs = 0;
    final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.DEFAULT;
    final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    final BulkProcessor<IndexableRecord, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        maxBufferedRecords,
        maxInFlightBatches,
        batchSize,
        lingerMs,
        maxRetries,
        retryBackoffMs,
        behaviorOnMalformedDoc,
        reporter
    );

    final int addTimeoutMs = 10;
    bulkProcessor.add(indexableRecord(42), sinkRecord(), addTimeoutMs);
    assertEquals(1, bulkProcessor.bufferedRecords());
    try {
      // BulkProcessor not started, so this add should timeout & throw
      bulkProcessor.add(indexableRecord(43), sinkRecord(), addTimeoutMs);
      fail();
    } catch (ConnectException good) {
    }
    assertEquals(1, bulkProcessor.recordsToReportOnError.size());
    verify(reporter, never()).report(eq(sinkRecord()), any());
  }

  @Test
  public void retriableErrors() throws InterruptedException, ExecutionException {
    final int maxBufferedRecords = 100;
    final int maxInFlightBatches = 5;
    final int batchSize = 2;
    final int lingerMs = 5;
    final int maxRetries = 3;
    final int retryBackoffMs = 1;
    final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.DEFAULT;
    final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, "a retiable error", getFailedRecords(42, 43)));
    client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, "a retriable error again", getFailedRecords(42, 43)));
    client.expect(Arrays.asList(42, 43), BulkResponse.success());

    final BulkProcessor<IndexableRecord, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        maxBufferedRecords,
        maxInFlightBatches,
        batchSize,
        lingerMs,
        maxRetries,
        retryBackoffMs,
        behaviorOnMalformedDoc,
        reporter
    );

    final int addTimeoutMs = 10;
    bulkProcessor.add(indexableRecord(42), sinkRecord(), addTimeoutMs);
    bulkProcessor.add(indexableRecord(43), sinkRecord(), addTimeoutMs);

    assertTrue(bulkProcessor.submitBatchWhenReady().get().succeeded);
    assertTrue(bulkProcessor.recordsToReportOnError.isEmpty());
    verify(reporter, never()).report(eq(sinkRecord()), any());
  }

  @Test
  public void retriableErrorsHitMaxRetries() throws InterruptedException {
    final int maxBufferedRecords = 100;
    final int maxInFlightBatches = 5;
    final int batchSize = 2;
    final int lingerMs = 5;
    final int maxRetries = 2;
    final int retryBackoffMs = 1;
    final String errorInfo = "a final retriable error again";
    final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.DEFAULT;
    final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, "a retriable error", getFailedRecords(42, 43)));
    client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, "a retriable error again", getFailedRecords(42, 43)));
    client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, errorInfo, getFailedRecords(42, 43)));

    final BulkProcessor<IndexableRecord, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        maxBufferedRecords,
        maxInFlightBatches,
        batchSize,
        lingerMs,
        maxRetries,
        retryBackoffMs,
        behaviorOnMalformedDoc,
        reporter
    );

    final int addTimeoutMs = 10;
    bulkProcessor.add(indexableRecord(42), sinkRecord(), addTimeoutMs);
    bulkProcessor.add(indexableRecord(43), sinkRecord(), addTimeoutMs);

    try {
      bulkProcessor.submitBatchWhenReady().get();
      fail();
    } catch (ExecutionException e) {
      assertTrue(e.getCause().getMessage().contains(errorInfo));
      assertTrue(bulkProcessor.recordsToReportOnError.isEmpty());
      verify(reporter, never()).report(eq(sinkRecord()), any());
    }
  }

  @Test
  public void unretriableErrors() throws InterruptedException {
    final int maxBufferedRecords = 100;
    final int maxInFlightBatches = 5;
    final int batchSize = 2;
    final int lingerMs = 5;
    final int maxRetries = 3;
    final int retryBackoffMs = 1;
    final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.DEFAULT;
    final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    final String errorInfo = "an unretriable error";
    client.expect(Arrays.asList(42, 43), BulkResponse.failure(false, errorInfo, getFailedRecords(42, 43)));

    final BulkProcessor<IndexableRecord, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        maxBufferedRecords,
        maxInFlightBatches,
        batchSize,
        lingerMs,
        maxRetries,
        retryBackoffMs,
        behaviorOnMalformedDoc,
        reporter
    );

    final int addTimeoutMs = 10;
    bulkProcessor.add(indexableRecord(42), sinkRecord(), addTimeoutMs);
    bulkProcessor.add(indexableRecord(43), sinkRecord(), addTimeoutMs);

    try {
      bulkProcessor.submitBatchWhenReady().get();
      fail();
    } catch (ExecutionException e) {
      assertTrue(e.getCause().getMessage().contains(errorInfo));
      assertTrue(bulkProcessor.recordsToReportOnError.isEmpty());
      verify(reporter, never()).report(eq(sinkRecord()), any());
    }
  }

  @Test
  public void failOnMalformedDoc() {
    final int maxBufferedRecords = 100;
    final int maxInFlightBatches = 5;
    final int batchSize = 2;
    final int lingerMs = 5;
    final int maxRetries = 3;
    final int retryBackoffMs = 1;
    final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.FAIL;
    final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    final String errorInfo = " [{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse\"," +
        "\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"object\n" +
        " field starting or ending with a [.] makes object resolution ambiguous: [avjpz{{.}}wjzse{{..}}gal9d]\"}}]";
    client.expect(Arrays.asList(42, 43), BulkResponse.failure(false, errorInfo, getFailedRecords(42, 43)));

    final BulkProcessor<IndexableRecord, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        maxBufferedRecords,
        maxInFlightBatches,
        batchSize,
        lingerMs,
        maxRetries,
        retryBackoffMs,
        behaviorOnMalformedDoc,
        reporter
    );

    bulkProcessor.start();

    bulkProcessor.add(indexableRecord(42), sinkRecord(),1);
    bulkProcessor.add(indexableRecord(43), sinkRecord(), 1);

    try {
      final int flushTimeoutMs = 1000;
      bulkProcessor.flush(flushTimeoutMs);
      fail();
    } catch(ConnectException e) {
      // expected
      assertTrue(e.getMessage().contains("mapper_parsing_exception"));
      assertTrue(bulkProcessor.recordsToReportOnError.isEmpty());
      verify(reporter, never()).report(eq(sinkRecord()), any());
    }
  }

  @Test
  public void ignoreOrWarnOnMalformedDoc() {
    final int maxBufferedRecords = 100;
    final int maxInFlightBatches = 5;
    final int batchSize = 2;
    final int lingerMs = 5;
    final int maxRetries = 3;
    final int retryBackoffMs = 1;
    final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    // Test both IGNORE and WARN options
    // There is no difference in logic between IGNORE and WARN, except for the logging.
    // Test to ensure they both work the same logically
    final List<BehaviorOnMalformedDoc> behaviorsToTest =
        Arrays.asList(BehaviorOnMalformedDoc.WARN, BehaviorOnMalformedDoc.IGNORE);

    for(BehaviorOnMalformedDoc behaviorOnMalformedDoc : behaviorsToTest)
    {
      final String errorInfo = " [{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse\"," +
          "\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"object\n" +
          " field starting or ending with a [.] makes object resolution ambiguous: [avjpz{{.}}wjzse{{..}}gal9d]\"}}]";
      client.expect(Arrays.asList(42, 43), BulkResponse.failure(false, errorInfo, getFailedRecords(42, 43)));

      final BulkProcessor<IndexableRecord, ?> bulkProcessor = new BulkProcessor<>(
          Time.SYSTEM,
          client,
          maxBufferedRecords,
          maxInFlightBatches,
          batchSize,
          lingerMs,
          maxRetries,
          retryBackoffMs,
          behaviorOnMalformedDoc,
          reporter
      );

      bulkProcessor.start();

      bulkProcessor.add(indexableRecord(42), sinkRecord(), 1);
      bulkProcessor.add(indexableRecord(43), sinkRecord(), 1);

      try {
        final int flushTimeoutMs = 1000;
        bulkProcessor.flush(flushTimeoutMs);
      } catch (ConnectException e) {
        fail(e.getMessage());
      }
      assertTrue(bulkProcessor.recordsToReportOnError.isEmpty());
    }

    verify(reporter, times(4)).report(eq(sinkRecord()), any());

  }

  @Test
  public void farmerTaskPropogatesException() {
    final int maxBufferedRecords = 100;
    final int maxInFlightBatches = 5;
    final int batchSize = 2;
    final int lingerMs = 5;
    final int maxRetries = 3;
    final int retryBackoffMs = 1;
    final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.DEFAULT;
    final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    final String errorInfo = "an unretriable error";
    client.expect(Arrays.asList(42, 43), BulkResponse.failure(false, errorInfo, getFailedRecords(42, 43)));

    final BulkProcessor<IndexableRecord, ?> bulkProcessor = new BulkProcessor<>(
            Time.SYSTEM,
            client,
            maxBufferedRecords,
            maxInFlightBatches,
            batchSize,
            lingerMs,
            maxRetries,
            retryBackoffMs,
            behaviorOnMalformedDoc,
            reporter
    );

    final int addTimeoutMs = 10;
    bulkProcessor.add(indexableRecord(42), sinkRecord(), addTimeoutMs);
    bulkProcessor.add(indexableRecord(43), sinkRecord(), addTimeoutMs);

    Runnable farmer = bulkProcessor.farmerTask();
    ConnectException e = assertThrows(ConnectException.class, () -> {
        farmer.run();
        // There's a small race condition in the farmer task where a failure on a batch thread
        // causes the stopRequested flag of the BulkProcessor to get set, and subsequently checked
        // on the farmer task thread, before the batch thread has time to actually complete and
        // throw an exception. When this happens, the invocation of farmer::run does not throw an
        // exception. However, we can still verify that a batch failed and that the bulk processor
        // is in the expected state by invoking BulkProcessor::throwIfFailed, which throws the first
        // error encountered on any batch thread. Even if the aforementioned race condition occurs,
        // the error should still have been captured by the bulk processor and if it is not present
        // by this point, it is a legitimate sign of a bug in the processor instead of a benign race
        // condition that just makes testing a little more complicated.
        bulkProcessor.throwIfFailed();
    });
    assertThat(e.getMessage(), containsString(errorInfo));
  }

  @Test
  public void terminateRetriesWhenInterruptedInSleep() {
    final int maxBufferedRecords = 100;
    final int maxInFlightBatches = 5;
    final int batchSize = 2;
    final int lingerMs = 5;
    final int maxRetries = 3;
    final int retryBackoffMs = 1;
    final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.DEFAULT;
    final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    Time mockTime = mock(Time.class);
    doAnswer(invocation -> {
      Thread.currentThread().interrupt();
      return null;
    }).when(mockTime).sleep(anyLong());

    client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, "a retriable error", getFailedRecords(42, 43)));

    final BulkProcessor<IndexableRecord, ?> bulkProcessor = new BulkProcessor<>(
            mockTime,
            client,
            maxBufferedRecords,
            maxInFlightBatches,
            batchSize,
            lingerMs,
            maxRetries,
            retryBackoffMs,
            behaviorOnMalformedDoc,
            reporter
    );

    final int addTimeoutMs = 10;
    bulkProcessor.add(indexableRecord(42), sinkRecord(), addTimeoutMs);
    bulkProcessor.add(indexableRecord(43), sinkRecord(), addTimeoutMs);

    ExecutionException e = assertThrows(ExecutionException.class,
            () -> bulkProcessor.submitBatchWhenReady().get());
    assertThat(e.getMessage(), containsString("a retriable error"));
    assertTrue(bulkProcessor.recordsToReportOnError.isEmpty());
    verify(reporter, never()).report(eq(sinkRecord()), any());
  }

  @Test
  public void testNullReporter() {
    final int maxBufferedRecords = 100;
    final int maxInFlightBatches = 5;
    final int batchSize = 2;
    final int lingerMs = 5;
    final int maxRetries = 3;
    final int retryBackoffMs = 1;
    final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.DEFAULT;

    client.expect(Arrays.asList(42, 43), BulkResponse.success());

    final BulkProcessor<IndexableRecord, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        maxBufferedRecords,
        maxInFlightBatches,
        batchSize,
        lingerMs,
        maxRetries,
        retryBackoffMs,
        behaviorOnMalformedDoc,
        null
    );

    bulkProcessor.start();

    final int addTimeoutMs = 10;
    bulkProcessor.add(indexableRecord(42), sinkRecord(), addTimeoutMs);
    bulkProcessor.add(indexableRecord(43), sinkRecord(), addTimeoutMs);

    assertEquals(2, bulkProcessor.bufferedRecords());
    assertNull(bulkProcessor.recordsToReportOnError);

    bulkProcessor.flush(1000);
    assertEquals(0, bulkProcessor.bufferedRecords());
  }

  @Test
  public void reportOnlyFailedRecords() {
    final int maxBufferedRecords = 100;
    final int maxInFlightBatches = 5;
    final int batchSize = 2;
    final int lingerMs = 5;
    final int maxRetries = 3;
    final int retryBackoffMs = 1;
    final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.IGNORE;
    final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    final String errorInfo = " [{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse\"," +
        "\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"object\n" +
        " field starting or ending with a [.] makes object resolution ambiguous: [avjpz{{.}}wjzse{{..}}gal9d]\"}}]";
    client.expect(Arrays.asList(42, 43), BulkResponse.failure(false, errorInfo, getFailedRecords(42)));

    final BulkProcessor<IndexableRecord, ?> bulkProcessor = new BulkProcessor<>(
        Time.SYSTEM,
        client,
        maxBufferedRecords,
        maxInFlightBatches,
        batchSize,
        lingerMs,
        maxRetries,
        retryBackoffMs,
        behaviorOnMalformedDoc,
        reporter
    );

    bulkProcessor.start();

    bulkProcessor.add(indexableRecord(42), sinkRecord(), 1);
    bulkProcessor.add(indexableRecord(43), sinkRecord(), 1);

    try {
      final int flushTimeoutMs = 1000;
      bulkProcessor.flush(flushTimeoutMs);
    } catch (ConnectException e) {
      fail(e.getMessage());
    }

    assertTrue(bulkProcessor.recordsToReportOnError.isEmpty());
    verify(reporter, times(1)).report(eq(sinkRecord()), any());
  }

  private static IndexableRecord indexableRecord(int id) {
    return new IndexableRecord(new Key(INDEX, TYPE, String.valueOf(id)), String.valueOf(id), null, "","");
  }

  private static SinkRecord sinkRecord() {
    return new SinkRecord(INDEX, 0, Schema.STRING_SCHEMA, "key", Schema.INT32_SCHEMA, 0, 0L);
  }

  private static Map<IndexableRecord, BulkResultItem> getFailedRecords(Integer... ids) {
    JestResult jestResult = new JestResult(new Gson());
    JsonObject result = new JsonObject();
    JsonArray array = new JsonArray();

    for (Integer id : ids) {
      JsonObject error = new JsonObject();
      error.addProperty("type", "awful error");
      error.addProperty("reason", "you write bad code");

      JsonObject values = new JsonObject();
      values.addProperty("_index", INDEX);
      values.addProperty("_type", TYPE);
      values.addProperty("_id", id.toString());
      values.addProperty("status", 404);
      values.addProperty("version", 0);
      values.add("error", error);

      JsonObject bulkItemResult = new JsonObject();
      bulkItemResult.add("operation", values);

      array.add(bulkItemResult);
    }

    result.add("items", array);

    jestResult.setJsonObject(result);

    Map<IndexableRecord, BulkResultItem> failedRecords = new HashMap<>();
    BulkResult bulkResult = new BulkResult(jestResult);
    List<BulkResultItem> items = bulkResult.getFailedItems();
    for (int i = 0; i < ids.length; i++) {
      Integer id = ids[i];
      failedRecords.put(indexableRecord(id), items.get(i));
    }

    return failedRecords;
  }
}
