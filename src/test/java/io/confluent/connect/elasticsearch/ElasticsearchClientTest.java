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
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BULK_SIZE_BYTES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WRITE_METHOD_CONFIG;
import static io.confluent.connect.elasticsearch.helper.ElasticsearchHelperClient.sourceAsMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static java.util.stream.Collectors.toSet;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnMalformedDoc;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnNullValues;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WriteMethod;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import io.confluent.connect.elasticsearch.helper.ElasticsearchHelperClient;
import io.confluent.connect.elasticsearch.helper.NetworkErrorContainer;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import org.mockito.ArgumentCaptor;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
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

    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.close();
  }

  @Test
  public void testCreateIndex() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
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
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
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
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    index = createIndexName(TOPIC);

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));
    client.close();
  }

  @Test
  public void testDoesNotCreateAlreadyExistingIndex() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    assertFalse(helperClient.indexExists(index));

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));

    assertFalse(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));
    client.close();
  }

  @Test
  public void testIndexExists() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    assertFalse(helperClient.indexExists(index));

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(client.indexExists(index));
    client.close();
  }

  @Test
  public void testIndexDoesNotExist() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    assertFalse(helperClient.indexExists(index));

    assertFalse(client.indexExists(index));
    client.close();
  }

  @Test
  public void testCreateMapping() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    client.createMapping(index, schema());

    assertTrue(client.hasMapping(index));

    TypeMapping mapping = helperClient.getMapping(index);
    assertNotNull(mapping);
    assertTrue(mapping.properties().containsKey("offset"));
    assertTrue(mapping.properties().containsKey("another"));
    Property offset = mapping.properties().get("offset");
    assertTrue(offset.isInteger());
    assertEquals(0, offset.integer().nullValue().intValue());
    Property another = mapping.properties().get("another");
    assertTrue(another.isInteger());
    assertEquals(0, another.integer().nullValue().intValue());
    client.close();
  }

  @Test
  public void testHasMapping() {
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    client.createMapping(index, schema());

    assertTrue(client.hasMapping(index));
    client.close();
  }

  @Test
  public void testDoesNotHaveMapping() {
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    assertFalse(client.hasMapping(index));
    client.close();
  }

  @Test
  public void testBuffersCorrectly() throws Exception {
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    props.put(MAX_BUFFERED_RECORDS_CONFIG, "1");
    config = new ElasticsearchSinkConnectorConfig(props);
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
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
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
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
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
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
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
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
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
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
    for (Hit<JsonData> hit : helperClient.search(index)) {
      if (hit.id().equals("key0")) {
        Map<String, Object> source = sourceAsMap(hit);
        assertEquals(3, source.get("offset"));
        assertEquals(0, source.get("another"));
      }
    }

    client.close();
  }

  @Test
  public void testIgnoreBadRecord() throws Exception {
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.IGNORE.name());
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
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
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
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
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
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
    ElasticsearchClient client = new ElasticsearchClient(config, reporter, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
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
    ElasticsearchClient client = new ElasticsearchClient(config, reporter, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
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
    ElasticsearchClient client = new ElasticsearchClient(config, reporter, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
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
  private List<SinkRecord> causeExternalVersionConflictError(ElasticsearchClient client) throws InterruptedException {
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
    ElasticsearchClient client = new ElasticsearchClient(config, reporter, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");

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
    ElasticsearchClient client = new ElasticsearchClient(config, reporter, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink") {
      @Override
      protected boolean handleResponse(BulkResponseItem response, BulkOpContext context) {
        // Make it think it was an internal version conflict by stripping the external
        // versioning from the operation before it is interpreted. Note that we don't
        // make any attempt to reset the response version number.
        BulkOpContext internalContext = context;
        if (context.operation.isIndex()) {
          BulkOperation internalOperation = BulkOperation.of(b -> b.index(i -> i
              .index(context.operation.index().index())
              .id(context.operation.index().id())
              .document(context.operation.index().document())));
          internalContext =
              new BulkOpContext(context.sinkRecord, context.offsetState, internalOperation);
        }
        return super.handleResponse(response, internalContext);
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
    ElasticsearchClient client = new ElasticsearchClient(config, reporter, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    ElasticsearchClient client2 = new ElasticsearchClient(config, reporter2, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");

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
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
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
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.close();
  }
  // Connector-owned pools carry the {connectorName}-{taskId} prefix; bulk I/O itself
  // runs on the rest client's own threads.
  @Test
  public void testThreadNamingWithConnectorNameAndTaskId() throws Exception {
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "2");
    props.put(BATCH_SIZE_CONFIG, "1"); // Force small batches to create multiple threads
    props.put(LINGER_MS_CONFIG, "100"); // Reduce linger time to process batches quickly
    props.put(ElasticsearchSinkTaskConfig.TASK_ID_CONFIG, "1");
    props.put("name", "elasticsearch-sink");
    ElasticsearchSinkTaskConfig taskConfig = new ElasticsearchSinkTaskConfig(props);

    ElasticsearchClient client = new ElasticsearchClient(taskConfig, null, () -> offsetTracker.updateOffsets(),
            1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    // Trigger bulk operations to create threads
    for (int i = 0; i < 10; i++) {
      writeRecord(sinkRecord(i), client);
    }
    client.flush();
    waitUntilRecordsInES(10);

    // The bulk-retry pool is not asserted: its single thread starts lazily on the first
    // scheduled retry (4c5dd88b), so it never exists on a healthy path.
    List<String> poolPrefixes = Arrays.asList(
            "elasticsearch-sink-1-elasticsearch-bulk-ingester-",
            "elasticsearch-sink-1-elasticsearch-bulk-dispatcher-");

    Set<String> threadNames = Thread.getAllStackTraces().keySet().stream()
            .map(Thread::getName)
            .collect(toSet());

    for (String prefix : poolPrefixes) {
      Set<String> poolThreads = threadNames.stream()
              .filter(name -> name.startsWith(prefix))
              .collect(toSet());
      assertTrue("Expected a thread named " + prefix + "* to exist, found: " + threadNames,
              !poolThreads.isEmpty());
      for (String threadName : poolThreads) {
        String suffix = threadName.substring(prefix.length());
        assertTrue("Thread name should end with a number: " + threadName,
                suffix.matches("\\d+"));
      }
    }

    client.close();
  }

  // A constructor failure after the RestClient has started its non-daemon I/O reactor
  // threads must close the transport rather than leak the threads.
  @Test(timeout = 30_000)
  public void testConstructorFailureClosesTransport() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      props.put(CONNECTION_URL_CONFIG, "http://localhost:" + socket.getLocalPort());
      config = new ElasticsearchSinkConnectorConfig(props);

      int baseline = countRestClientThreads();

      assertThrows(RuntimeException.class, () ->
          new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1,
              "elasticsearch-sink") {
            @Override
            String getServerVersion(co.elastic.clients.elasticsearch.ElasticsearchClient c) {
              throw new RuntimeException("boom during construction");
            }
          });

      long deadline = System.currentTimeMillis() + 10_000;
      while (countRestClientThreads() > baseline && System.currentTimeMillis() < deadline) {
        Thread.sleep(50);
      }
      assertEquals("elasticsearch-rest-client threads leaked after failed construction",
          baseline, countRestClientThreads());
    }
  }

  // A failed task closes twice (throwIfFailed() closes before put() throws, then the
  // framework's stop() closes again), so the second close must be a no-op.
  @Test
  public void testCloseIsIdempotent() {
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");

    SinkRecord record = sinkRecord(0);
    ElasticsearchClient.BulkOpContext context = new ElasticsearchClient.BulkOpContext(
        record,
        new AsyncOffsetTracker.AsyncOffsetState(record.kafkaOffset()),
        converter.convertRecord(record, index));
    BulkResponseItem failedItem = BulkResponseItem.of(b -> b
        .operationType(OperationType.Index)
        .index(index)
        .status(400)
        .error(e -> e.type("some_terminal_exception").reason("boom")));
    client.handleResponse(failedItem, context);

    assertThrows(ConnectException.class, client::close);
    client.close();
  }

  // close() with a record stuck at an unresponsive endpoint must throw the flush-timeout error, not hang.
  @Test(timeout = 60_000)
  public void testCloseWithStuckRecordsTerminates() throws Exception {
    try (ServerSocket blackhole = new ServerSocket(0)) {
      props.put(CONNECTION_URL_CONFIG, "http://localhost:" + blackhole.getLocalPort());
      props.put(BATCH_SIZE_CONFIG, "1");
      props.put(LINGER_MS_CONFIG, "10");
      props.put(FLUSH_TIMEOUT_MS_CONFIG, "1000");
      config = new ElasticsearchSinkConnectorConfig(props);
      converter = new DataConverter(config);
      ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");

      writeRecord(sinkRecord(0), client);

      ConnectException e = assertThrows(ConnectException.class, client::close);
      assertTrue(e.getMessage(), e.getMessage().contains(
          "Failed to process outstanding requests in time while closing"));
    }
  }

  // A task-cancellation interrupt during close()'s buffer drain must abort the wait
  // immediately, preserving the interrupt as the synthesized cause (clock.sleep swallows
  // the original InterruptedException).
  @Test(timeout = 20_000)
  public void testCloseWithStuckRecordsHonorsInterrupt() throws Exception {
    try (ServerSocket blackhole = new ServerSocket(0)) {
      props.put(CONNECTION_URL_CONFIG, "http://localhost:" + blackhole.getLocalPort());
      props.put(BATCH_SIZE_CONFIG, "1");
      props.put(LINGER_MS_CONFIG, "10");
      props.put(FLUSH_TIMEOUT_MS_CONFIG, "60000");
      config = new ElasticsearchSinkConnectorConfig(props);
      converter = new DataConverter(config);
      ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");

      writeRecord(sinkRecord(0), client);
      try {
        Thread.currentThread().interrupt();
        ConnectException e = assertThrows(ConnectException.class, client::close);
        assertTrue(e.getMessage().contains("Interrupted"));
        assertTrue(String.valueOf(e.getCause()), e.getCause() instanceof InterruptedException);
      } finally {
        // Clear the flag so test teardown is not poisoned.
        Thread.interrupted();
      }
    }
  }

  // Three pools that will not drain within the budget: each holds a task sleeping well
  // past it. With one shared deadline the caller waits ~budget once; awaiting each pool
  // for the full budget in turn would cost ~budget * poolCount.
  @Test(timeout = 30_000)
  public void awaitTerminationWithinSharesOneDeadlineAcrossPools() throws Exception {
    List<ExecutorService> pools = Arrays.asList(
        Executors.newSingleThreadExecutor(),
        Executors.newSingleThreadExecutor(),
        Executors.newSingleThreadExecutor());
    for (ExecutorService pool : pools) {
      pool.submit(() -> {
        try {
          Thread.sleep(30_000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      pool.shutdown();
    }

    long budgetMs = 2_000;
    long start = System.nanoTime();
    ElasticsearchClient.awaitTerminationWithin(pools, budgetMs);
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    // Shared deadline: ~budget total. Sequential-per-pool would be ~3 * budget.
    assertTrue("awaitTerminationWithin took " + elapsedMs + "ms; expected ~" + budgetMs + "ms",
        elapsedMs < budgetMs * 2);
    for (ExecutorService pool : pools) {
      assertTrue("pool was not forced down", pool.isShutdown());
      assertTrue("pool tasks did not terminate", pool.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  // A mapping with only dynamic settings and no properties must still count as existing
  // so the connector does not overwrite it.
  @Test
  public void testHasMappingWithoutProperties() throws Exception {
    helperClient.createIndex(index, "{\"dynamic\": \"strict\"}");
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");

    assertTrue(client.hasMapping(index));
    client.close();
  }

  // linger.ms=0 ("flush immediately") must be clamped rather than fail construction,
  // since the BulkIngester flush timer rejects a zero interval.
  @Test
  public void testWritesWithZeroLingerMs() throws Exception {
    props.put(LINGER_MS_CONFIG, "0");
    config = new ElasticsearchSinkConnectorConfig(props);
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    client.flush();

    waitUntilRecordsInES(1);
    client.close();
  }

  // bulk.size.bytes=0 ("flush every record") must be clamped rather than become a size
  // limit every record exceeds, which would block each add forever.
  @Test
  public void testWritesWithZeroBulkSizeBytes() throws Exception {
    props.put(BULK_SIZE_BYTES_CONFIG, "0");
    config = new ElasticsearchSinkConnectorConfig(props);
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    client.flush();

    waitUntilRecordsInES(1);
    client.close();
  }

  // The actionable detail of a mapping failure lives in the caused_by chain.
  @Test
  public void testDlqMessageIncludesCausedByChain() throws Exception {
    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    ElasticsearchClient client = new ElasticsearchClient(config, reporter, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");

    SinkRecord record = sinkRecord(0);
    ElasticsearchClient.BulkOpContext context = new ElasticsearchClient.BulkOpContext(
        record,
        new AsyncOffsetTracker.AsyncOffsetState(record.kafkaOffset()),
        converter.convertRecord(record, index));
    BulkResponseItem item = BulkResponseItem.of(b -> b
        .operationType(OperationType.Index)
        .index(index)
        .status(400)
        .error(e -> e
            .type("mapper_parsing_exception")
            .reason("failed to parse field [price]")
            .causedBy(c -> c
                .type("illegal_argument_exception")
                .reason("For input string: \"abc\""))));

    assertTrue(client.handleResponse(item, context));

    ArgumentCaptor<Throwable> reported = ArgumentCaptor.forClass(Throwable.class);
    verify(reporter).report(eq(record), reported.capture());
    String message = reported.getValue().getMessage();
    assertTrue(message,
        message.contains("[mapper_parsing_exception] failed to parse field [price]"));
    assertTrue(message,
        message.contains("nested: [illegal_argument_exception] For input string: \"abc\""));
    // close() drains and then re-throws the latched indexing failure.
    assertThrows(ConnectException.class, client::close);
  }

  // A redelivered record must version-conflict on its explicit document id and be
  // ignored, not create a duplicate document in the data stream.
  @Test
  public void testWriteDataStreamDeduplicatesRedeliveredRecords() throws Exception {
    props.put(DATA_STREAM_TYPE_CONFIG, DATA_STREAM_TYPE);
    props.put(DATA_STREAM_DATASET_CONFIG, DATA_STREAM_DATASET);
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    index = createIndexName(TOPIC);
    assertTrue(client.createIndexOrDataStream(index));

    writeRecord(sinkRecord(0), client);
    client.flush();
    client.waitForInFlightRequests();
    waitUntilRecordsInES(1);

    writeRecord(sinkRecord(0), client);
    client.flush();
    client.waitForInFlightRequests();

    assertEquals(1, helperClient.getDocCount(index));
    assertFalse(client.isFailed());
    client.close();
  }

  private static int countRestClientThreads() {
    int count = 0;
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.isAlive() && t.getName().startsWith("elasticsearch-rest-client")) {
        count++;
      }
    }
    return count;
  }
}
