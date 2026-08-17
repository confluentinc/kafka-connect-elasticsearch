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

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_DATASET_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_TYPE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WRITE_METHOD_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnMalformedDoc;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnNullValues;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WriteMethod;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import io.confluent.connect.elasticsearch.helper.ElasticsearchHelperClient;
import io.confluent.connect.elasticsearch.helper.NetworkErrorContainer;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ElasticsearchClientTest extends ElasticsearchClientTestBase {

  @BeforeClass
  public static void setupBeforeAll() {
    container = ElasticsearchContainer.fromSystemProperties();
    container.start();
  }

  @Before
  public void setup() {
    index = TOPIC;
    props = ElasticsearchSinkConnectorConfigTest.addNecessaryProps(new HashMap<>());
    props.put(CONNECTION_URL_CONFIG, container.getConnectionUrl());
    props.put(IGNORE_KEY_CONFIG, "true");
    props.put(LINGER_MS_CONFIG, "1000");
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);
    helperClient = new ElasticsearchHelperClient(container.getConnectionUrl(), config);
    helperClient.waitForConnection(30000);
    offsetTracker = mock(OffsetTracker.class);
  }

  @After
  public void cleanup() throws IOException {
    if (helperClient != null && helperClient.indexExists(index)){
      helperClient.deleteIndex(index, config.isDataStream());
    }
  }

  @Test
  public void testClose() {

    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.close();
  }

  @Test
  public void testCloseFails() throws Exception {
    props.put(BATCH_SIZE_CONFIG, "1");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    // Keeps the pre-migration approach of overriding close() with a deliberately tiny bound, only
    // expressed against BulkIngester. Driving this through flush.timeout.ms instead is not
    // possible: that config has a Range floor of 1000 ms, and at 1000 ms a single bulk request
    // against a local container completes well inside the window, so close() would succeed.
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink") {
      @Override
      public void close() {
        ExecutorService closeExecutor = Executors.newSingleThreadExecutor();
        try {
          closeExecutor.submit((Runnable) bulkIngester::close)
              .get(1, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          throw new ConnectException("Failed to process all outstanding requests in time.");
        } catch (InterruptedException | ExecutionException e) {
          // not the condition under test
        } finally {
          closeExecutor.shutdownNow();
        }
      }
    };

    writeRecord(sinkRecord(0), client);
    assertThrows(
        "Failed to process outstanding requests in time while closing the ElasticsearchSinkClient.",
        ConnectException.class,
        () -> client.close()
    );
    waitUntilRecordsInES(1);
  }

  @Test
  public void testCreateIndex() throws IOException {
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    assertFalse(helperClient.indexExists(index));

    client.createIndexOrDataStream(index);
    assertTrue(helperClient.indexExists(index));
    client.close();
  }

  @Test
  public void testCreateExistingDataStream() throws Exception {
    props.put(DATA_STREAM_TYPE_CONFIG, DATA_STREAM_TYPE);
    props.put(DATA_STREAM_DATASET_CONFIG, DATA_STREAM_DATASET);
    config = new ElasticsearchSinkConnectorConfig(props);
    index = createIndexName(TOPIC);
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    index = createIndexName(TOPIC);

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));
    assertFalse(client.createIndexOrDataStream(index));
    client.close();
  }

  @Test
  public void testCreateNewDataStream() throws Exception {
    props.put(DATA_STREAM_TYPE_CONFIG, DATA_STREAM_TYPE);
    props.put(DATA_STREAM_DATASET_CONFIG, DATA_STREAM_DATASET);
    config = new ElasticsearchSinkConnectorConfig(props);
    index = createIndexName(TOPIC);
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    index = createIndexName(TOPIC);

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));
    client.close();
  }

  @Test
  public void testDoesNotCreateAlreadyExistingIndex() throws IOException {
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    assertFalse(helperClient.indexExists(index));

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));

    assertFalse(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));
    client.close();
  }

  @Test
  public void testIndexExists() throws IOException {
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    assertFalse(helperClient.indexExists(index));

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(client.indexExists(index));
    client.close();
  }

  @Test
  public void testIndexDoesNotExist() throws IOException {
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    assertFalse(helperClient.indexExists(index));

    assertFalse(client.indexExists(index));
    client.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCreateMapping() throws IOException {
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    client.createMapping(index, schema());

    assertTrue(client.hasMapping(index));

    TypeMapping mapping = helperClient.getMapping(index).mappings();
    Map<String, Property> mappingProps = mapping.properties();
    assertTrue(mappingProps.containsKey("offset"));
    assertTrue(mappingProps.containsKey("another"));
    assertTrue(mappingProps.get("offset").isInteger());
    assertEquals(Integer.valueOf(0), mappingProps.get("offset").integer().nullValue());
    assertTrue(mappingProps.get("another").isInteger());
    assertEquals(Integer.valueOf(0), mappingProps.get("another").integer().nullValue());
    client.close();
  }

  @Test
  public void testHasMapping() {
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    client.createMapping(index, schema());

    assertTrue(client.hasMapping(index));
    client.close();
  }

  @Test
  public void testDoesNotHaveMapping() {
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    assertFalse(client.hasMapping(index));
    client.close();
  }

  @Test
  public void testBuffersCorrectly() throws Exception {
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    props.put(MAX_BUFFERED_RECORDS_CONFIG, "1");
    config = new ElasticsearchSinkConnectorConfig(props);
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    assertEquals(1, client.numBufferedRecords.get());
    client.flush();

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(INDEX));
    assertEquals(0, client.numBufferedRecords.get());

    writeRecord(sinkRecord(1), client);
    assertEquals(1, client.numBufferedRecords.get());

    // will block until the previous record is flushed
    writeRecord(sinkRecord(2), client);
    assertEquals(1, client.numBufferedRecords.get());

    waitUntilRecordsInES(3);
    client.close();
  }

  @Test
  public void testFlush() throws Exception {
    props.put(LINGER_MS_CONFIG, String.valueOf(TimeUnit.DAYS.toMillis(1)));
    config = new ElasticsearchSinkConnectorConfig(props);
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    assertEquals(0, helperClient.getDocCount(index)); // should be empty before flush

    client.flush();

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(index));
    client.close();
  }

  @Test
  public void testIndexRecord() throws Exception {
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    client.flush();

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(index));
    client.close();
  }

  @Test
  public void testDeleteRecord() throws Exception {
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.DELETE.name());
    props.put(IGNORE_KEY_CONFIG, "false");
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord("key0", 0), client);
    writeRecord(sinkRecord("key1", 1), client);
    client.flush();

    waitUntilRecordsInES(2);

    // delete 1
    SinkRecord deleteRecord = sinkRecord("key0", null, null, 3);
    writeRecord(deleteRecord, client);

    waitUntilRecordsInES(1);
    client.close();
  }

  @Test
  public void testUpsertRecords() throws Exception {
    props.put(WRITE_METHOD_CONFIG, WriteMethod.UPSERT.name());
    props.put(IGNORE_KEY_CONFIG, "false");
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord("key0", 0), client);
    writeRecord(sinkRecord("key1", 1), client);
    client.flush();

    waitUntilRecordsInES(2);

    // create modified record for upsert
    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("offset", SchemaBuilder.int32().defaultValue(0).build())
        .field("another", SchemaBuilder.int32().defaultValue(0).build())
        .build();

    Struct value = new Struct(schema).put("offset", 2);
    SinkRecord upsertRecord = sinkRecord("key0", schema, value, 2);
    Struct value2 = new Struct(schema).put("offset", 3);
    SinkRecord upsertRecord2 = sinkRecord("key0", schema, value2, 3);

    // upsert 2, write another
    writeRecord(upsertRecord, client);
    writeRecord(upsertRecord2, client);
    writeRecord(sinkRecord("key2", 4), client);
    client.flush();

    waitUntilRecordsInES(3);
    for (Hit<Map> hit : helperClient.search(index)) {
      if (hit.id().equals("key0")) {
        assertEquals(3, hit.source().get("offset"));
        assertEquals(0, hit.source().get("another"));
      }
    }

    client.close();
  }

  @Test
  public void testIgnoreBadRecord() throws Exception {
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.IGNORE.name());
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);
    client.createMapping(index, schema());

    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("not_mapped_field", SchemaBuilder.int32().defaultValue(0).build())
        .build();
    Struct value = new Struct(schema).put("not_mapped_field", 420);
    SinkRecord badRecord = sinkRecord("key", schema, value, 0);

    writeRecord(sinkRecord(0), client);
    client.flush();

    writeRecord(badRecord, client);
    client.flush();

    writeRecord(sinkRecord(1), client);
    client.flush();

    waitUntilRecordsInES(2);
    assertEquals(2, helperClient.getDocCount(index));
    client.close();
  }

  @Test(expected = ConnectException.class)
  public void testFailOnBadRecord() throws Exception {
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);
    client.createMapping(index, schema());

    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("offset", SchemaBuilder.bool().defaultValue(false).build())
        .build();
    Struct value = new Struct(schema).put("offset", false);
    SinkRecord badRecord = sinkRecord("key", schema, value, 0);

    writeRecord(sinkRecord(0), client);
    client.flush();

    waitUntilRecordsInES(1);
    writeRecord(badRecord, client);
    client.flush();

    // consecutive index calls should cause exception
    try {
      for (int i = 0; i < 5; i++) {
        writeRecord(sinkRecord(i + 1), client);
        client.flush();
        waitUntilRecordsInES(i + 2);
      }
    } catch (ConnectException e) {
      client.close();
      throw e;
    }
  }

  @Test
  public void testRetryRecordsOnSocketTimeoutFailure() throws Exception {
    props.put(LINGER_MS_CONFIG, "60000");
    props.put(BATCH_SIZE_CONFIG, "2");
    props.put(MAX_RETRIES_CONFIG, "100");
    props.put(RETRY_BACKOFF_MS_CONFIG, "1000");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    // mock bulk processor to throw errors
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    // bring down ES service
    NetworkErrorContainer delay = new NetworkErrorContainer(container.getContainerName());
    delay.start();

    // attempt a write
    writeRecord(sinkRecord(0), client);
    client.flush();

    // keep the ES service down for a couple of timeouts
    Thread.sleep(config.readTimeoutMs() * 4L);

    // bring up ES service
    delay.stop();

    waitUntilRecordsInES(1);
  }

  /**
   * Test that verifies the following when behavior.on.malformed.docs is set to IGNORE:
   * - The reporter is called which reports all the errors along with bad records to DLQ.
   * - The connector doesn't fail and keeps processing other records.
   *
   * @throws Exception
   */
  @Test
  public void testReporter() throws Exception {
    props.put(IGNORE_KEY_CONFIG, "false");
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.IGNORE.name());
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    when(reporter.report(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, reporter, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);
    client.createMapping(index, schema());

    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("offset", SchemaBuilder.bool().defaultValue(false).build())
        .build();
    Struct value = new Struct(schema).put("offset", false);
    SinkRecord badRecord = sinkRecord("key0", schema, value, 1);

    writeRecord(sinkRecord("key0", 0), client);
    client.flush();
    waitUntilRecordsInES(1);

    writeRecord(badRecord, client);
    client.flush();

    // failed requests take a bit longer
    for (int i = 2; i < 7; i++) {
      writeRecord(sinkRecord("key" + i, i + 1), client);
      client.flush();
      waitUntilRecordsInES(i);
    }

    verify(reporter, times(1)).report(eq(badRecord), any(Throwable.class));
    client.close();
  }

  /**
   * Test that verifies the following when behavior.on.malformed.docs is set to FAIL:
   * - The reporter is called which reports all the errors along with bad records to DLQ
   * - The connector fails as expected and throws ConnectException.
   *
   * @throws Exception
   */
  @Test(expected = ConnectException.class)
  public void testReporterWithFail() throws Exception {
    props.put(IGNORE_KEY_CONFIG, "false");
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.FAIL.name());
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    when(reporter.report(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, reporter, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);
    client.createMapping(index, schema());

    Schema schema = SchemaBuilder
            .struct()
            .name("record")
            .field("offset", SchemaBuilder.bool().defaultValue(false).build())
            .build();
    Struct value = new Struct(schema).put("offset", false);
    SinkRecord badRecord = sinkRecord("key0", schema, value, 1);

    writeRecord(sinkRecord("key0", 0), client);
    client.flush();
    waitUntilRecordsInES(1);

    writeRecord(badRecord, client);
    client.flush();

    // failed requests take a bit longer
    for (int i = 2; i < 7; i++) {
      writeRecord(sinkRecord("key" + i, i + 1), client);
      client.flush();
      waitUntilRecordsInES(i);
    }

    verify(reporter, times(1)).report(eq(badRecord), any(Throwable.class));
    client.close();
  }

  @Test
  public void testReporterNotCalled() throws Exception {
    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, reporter, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    writeRecord(sinkRecord(1), client);
    writeRecord(sinkRecord(2), client);
    client.flush();

    waitUntilRecordsInES(3);
    assertEquals(3, helperClient.getDocCount(index));
    verify(reporter, never()).report(eq(sinkRecord(0)), any(Throwable.class));
    client.close();
  }


  /**
   * Cause a version conflict error.
   * Assumes that Elasticsearch VersionType is 'EXTERNAL' for the records
   * @param client The Elasticsearch client object to which to send records
   * @return List of duplicated SinkRecord objects
   */
  private List<SinkRecord> causeExternalVersionConflictError(ElasticsearchSinkClient client) throws InterruptedException {
    client.createIndexOrDataStream(index);

    final int conflict_record_count = 2;

    int offset = 0;

    // Sequentially increase out record version (which comes from the offset)
    for (; offset < conflict_record_count; ++offset) {
      writeRecord(sinkRecord(offset), client);
    }

    List<SinkRecord> conflict_list = new LinkedList<SinkRecord>();

    // Write the second half and keep the records
    for (; offset < conflict_record_count * 2; ++offset) {
      SinkRecord sink_record = sinkRecord(offset);
      writeRecord(sink_record, client);
      conflict_list.add(sink_record);
    }

    client.flush();
    client.waitForInFlightRequests();

    // At the end of the day, it's just one record being overwritten
    waitUntilRecordsInES(1);

    // Duplicates arbitrarily in reverse order

    for (SinkRecord sink_record : conflict_list) {
      writeRecord(sink_record, client);
    }

    client.flush();
    client.waitForInFlightRequests();

    return conflict_list;
  }

  /**
   * If the record version is set to VersionType.EXTERNAL (normal case for non-streaming),
   * then same or less version number will throw a version conflict exception.
   * @throws Exception will be thrown if the test fails
   */
  @Test
  public void testExternalVersionConflictReporterNotCalled() throws Exception {
    props.put(IGNORE_KEY_CONFIG, "false");
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, reporter, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");

    List<SinkRecord> duplicate_records = causeExternalVersionConflictError(client);

    // Make sure that no error was reported for any record(s)
    for (SinkRecord duplicated_record : duplicate_records) {
      verify(reporter, never()).report(eq(duplicated_record), any(Throwable.class));
    }
    client.close();
  }

  /**
   * If the record version is set to VersionType.INTERNAL (normal case streaming/logging),
   * then same or less version number will throw a version conflict exception.
   * In this test, we are checking that the client function `handleResponse`
   * properly reports an error for seeing the version conflict error along with
   * VersionType of INTERNAL.  We still actually cause the error via an external
   * version conflict error, but flip the version type to internal before it is interpreted.
   * @throws Exception will be thrown if the test fails
   */
  @Test
  public void testHandleResponseInternalVersionConflictReporterCalled() throws Exception {
    props.put(IGNORE_KEY_CONFIG, "false");
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    // We will cause a version conflict error, but test that handleResponse()
    // correctly reports the error when it interprets the version conflict as
    // "INTERNAL" (version maintained by Elasticsearch) rather than
    // "EXTERNAL" (version maintained by the connector as kafka offset)
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, reporter, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink") {
      @Override
      protected boolean handleResponse(BulkResponseItem item, SinkRecordAndOffset ctx,
                                    long executionId) {
        // Make it think it was an internal version conflict.
        //
        // Pre-migration this flipped request.versionType(VersionType.INTERNAL) in place. A
        // BulkOperation is immutable, so instead we hand super a context carrying an equivalent
        // operation rebuilt with VersionType.Internal, keeping the same record and offset state so
        // DLQ reporting still resolves the original SinkRecord.
        BulkOperation internallyVersioned = BulkOperation.of(b -> b.index(i -> i
            .index(ctx.operation.index().index())
            .id(ctx.operation.index().id())
            .document(ctx.operation.index().document())
            .versionType(VersionType.Internal)
            .version(ctx.operation.index().version())));
        return super.handleResponse(item,
            new SinkRecordAndOffset(ctx.sinkRecord, ctx.offsetState, internallyVersioned),
            executionId);
      }
    };

    List<SinkRecord> duplicate_records = causeExternalVersionConflictError(client);

    // Make sure that error was reported for either offset [1, 2] record(s)
    for (SinkRecord duplicated_record : duplicate_records) {
      verify(reporter, times(1)).report(eq(duplicated_record), any(Throwable.class));
    }
    client.close();
  }

  @Test
  public void testNoVersionConflict() throws Exception {
    props.put(IGNORE_KEY_CONFIG, "false");
    props.put(WRITE_METHOD_CONFIG, WriteMethod.UPSERT.name());
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    ErrantRecordReporter reporter2 = mock(ErrantRecordReporter.class);
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, reporter, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    ElasticsearchSinkClient client2 = new ElasticsearchSinkClient(config, reporter2, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");

    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    writeRecord(sinkRecord(1), client2);
    writeRecord(sinkRecord(2), client);
    writeRecord(sinkRecord(3), client2);
    writeRecord(sinkRecord(4), client);
    writeRecord(sinkRecord(5), client2);

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(index));
    verify(reporter, never()).report(any(SinkRecord.class), any(Throwable.class));
    verify(reporter2, never()).report(any(SinkRecord.class), any(Throwable.class));
    client.close();
    client2.close();
  }

  @Test
  public void testWriteDataStreamInjectTimestamp() throws Exception {
    props.put(DATA_STREAM_TYPE_CONFIG, DATA_STREAM_TYPE);
    props.put(DATA_STREAM_DATASET_CONFIG, DATA_STREAM_DATASET);
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    index = createIndexName(TOPIC);

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));

    // Sink Record does not include the @timestamp field in its value.
    writeRecord(sinkRecord(0), client);
    client.flush();

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(index));
    client.close();
  }

  @Test
  public void testConnectionUrlExtraSlash() {
    props.put(CONNECTION_URL_CONFIG, container.getConnectionUrl() + "/");
    config = new ElasticsearchSinkConnectorConfig(props);
    ElasticsearchSinkClient client = new ElasticsearchSinkClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.close();
  }
  @Test
  public void testThreadNamingWithConnectorNameAndTaskId() throws Exception {
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "2");
    props.put(BATCH_SIZE_CONFIG, "1"); // Force small batches to create multiple threads
    props.put(LINGER_MS_CONFIG, "100"); // Reduce linger time to process batches quickly
    props.put(ElasticsearchSinkTaskConfig.TASK_ID_CONFIG, "1");
    props.put("name", "elasticsearch-sink");
    ElasticsearchSinkTaskConfig taskConfig = new ElasticsearchSinkTaskConfig(props);

    ElasticsearchSinkClient client = new ElasticsearchSinkClient(taskConfig, null, () -> offsetTracker.updateOffsets(),
            1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    // Trigger bulk operations to create threads
    for (int i = 0; i < 10; i++) {
      writeRecord(sinkRecord(i), client);
    }
    client.flush();
    waitUntilRecordsInES(10);

    // Expected pattern: {connectorName}-{taskId}-elasticsearch-{pool}-scheduler-{number}.
    // Pre-migration there was a single pool named "...-elasticsearch-bulk-executor-", which held
    // the blocking bulk calls. BulkIngester executes requests itself, so that pool is gone; the
    // client now owns two schedulers -- one passed to BulkIngester (flush timer + listener
    // callbacks) and one used solely by RetryingTransport for backoff.
    for (String expectedPrefix : new String[] {
        "elasticsearch-sink-1-elasticsearch-bulk-scheduler-",
        "elasticsearch-sink-1-elasticsearch-retry-scheduler-"
    }) {
      Set<String> threadNames = Thread.getAllStackTraces().keySet().stream()
              .map(Thread::getName)
              .filter(name -> name.startsWith(expectedPrefix))
              .collect(java.util.stream.Collectors.toSet());

      assertTrue("Expected threads with prefix " + expectedPrefix + " to exist",
              !threadNames.isEmpty());

      // Verify thread names follow the expected pattern
      for (String threadName : threadNames) {
        String suffix = threadName.substring(expectedPrefix.length());
        assertTrue("Thread name should end with a number, was: " + threadName,
                suffix.matches("\\d+"));
      }
    }

    client.close();
  }
}
