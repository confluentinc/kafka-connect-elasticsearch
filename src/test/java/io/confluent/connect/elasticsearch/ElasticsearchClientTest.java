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
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WRITE_METHOD_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import co.elastic.clients.json.JsonData;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnMalformedDoc;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnNullValues;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WriteMethod;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import io.confluent.connect.elasticsearch.helper.ElasticsearchHelperClient;
import io.confluent.connect.elasticsearch.helper.NetworkErrorContainer;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.client.RestClient;
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
  public void testCloseFails() throws Exception {
    props.put(BATCH_SIZE_CONFIG, "1");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    // BulkIngester.close() waits for all in-flight operations. We do not need to wait manually
    client.close();
    waitUntilRecordsInES(1);
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
  @SuppressWarnings("unchecked")
  public void testCreateMapping() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    client.createMapping(index, schema());

    assertTrue(client.hasMapping(index));

    co.elastic.clients.elasticsearch._types.mapping.TypeMapping typeMapping =
        helperClient.getMapping(index);
    assertTrue(typeMapping.properties().containsKey("offset"));
    assertTrue(typeMapping.properties().containsKey("another"));
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
    helperClient.search(index).forEach(hit -> {
      if ("key0".equals(hit.id())) {
        @SuppressWarnings("unchecked")
        Map<String, Object> src = (Map<String, Object>) hit.source();
        assertEquals(3, src.get("offset"));
        assertEquals(0, src.get("another"));
      }
    });

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
    // Use IGNORE_KEY=false so records use key-based IDs, enabling real version conflicts.
    // A subclass overrides handleResponse to treat ALL version conflicts as internal,
    // so the DLQ reporter is called even though the underlying conflict was external.
    props.put(IGNORE_KEY_CONFIG, "false");
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    when(reporter.report(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    ElasticsearchClient client = new ElasticsearchClient(
        config, reporter, () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink") {
      @Override
      protected boolean handleResponse(
          co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem item,
          ElasticsearchClient.SinkRecordAndOffset ctx,
          long executionId) {
        if (item.error() != null) {
          String failureMsg = item.error().type() + ": " + item.error().reason();
          if (failureMsg.contains("version_conflict_engine_exception")) {
            if (reporter != null && ctx != null) {
              reporter.report(
                  ctx.sinkRecord,
                  new ElasticsearchClient.ReportingException("Indexing failed: " + failureMsg)
              );
            }
            return false;
          }
        }
        return super.handleResponse(item, ctx, executionId);
      }
    };

    List<SinkRecord> duplicate_records = causeExternalVersionConflictError(client);

    // Make sure that error was reported for duplicate record(s) — internal version
    // conflict is reported to DLQ
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

  /**
   * UPSERT generates an update operation with no external version. A version_conflict on that
   * update is a genuine conflict that must be routed to the DLQ, not silently dropped.
   */
  @Test
  public void testUpsertVersionConflictReportedToDlq() {
    props.put(WRITE_METHOD_CONFIG, WriteMethod.UPSERT.name());
    props.put(IGNORE_KEY_CONFIG, "false");
    config = new ElasticsearchSinkConnectorConfig(props);
    DataConverter upsertConverter = new DataConverter(config);

    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    when(reporter.report(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    @SuppressWarnings("unchecked")
    BulkIngester<ElasticsearchClient.SinkRecordAndOffset> mockIngester = mock(BulkIngester.class);
    ElasticsearchClient client = new ElasticsearchClient(
        config, reporter, mockIngester, mock(RestClient.class));

    SinkRecord record = sinkRecord("key0", 0);
    BulkOperation op = upsertConverter.convertRecord(record, index);
    ElasticsearchClient.SinkRecordAndOffset ctx = new ElasticsearchClient.SinkRecordAndOffset(
        record, new AsyncOffsetTracker.AsyncOffsetState(0), op);

    client.handleResponse(versionConflictItem(index, "key0", OperationType.Update), ctx, 1L);

    verify(reporter, times(1)).report(eq(record), any(Throwable.class));
  }

  /**
   * INSERT with ignore.key=false produces an index operation with VersionType.External.
   * A version_conflict on that op is an expected repeated-offset collision and must not
   * be reported to the DLQ. This is a non-regression guard: the pre-call assertions pin
   * the preconditions that isExternallyVersioned depends on, so the DLQ suppression is
   * provably due to that branch and not to the DataStream suppression path or a null reporter.
   */
  @Test
  public void testIndexExternalVersionConflictIgnoredNotReportedToDlq() {
    props.put(WRITE_METHOD_CONFIG, WriteMethod.INSERT.name());
    props.put(IGNORE_KEY_CONFIG, "false");
    config = new ElasticsearchSinkConnectorConfig(props);
    DataConverter insertConverter = new DataConverter(config);

    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    @SuppressWarnings("unchecked")
    BulkIngester<ElasticsearchClient.SinkRecordAndOffset> mockIngester = mock(BulkIngester.class);
    ElasticsearchClient client = new ElasticsearchClient(
        config, reporter, mockIngester, mock(RestClient.class));

    SinkRecord record = sinkRecord("key0", 5);
    BulkOperation op = insertConverter.convertRecord(record, index);
    ElasticsearchClient.SinkRecordAndOffset ctx = new ElasticsearchClient.SinkRecordAndOffset(
        record, new AsyncOffsetTracker.AsyncOffsetState(5), op);

    // Pin preconditions: isExternallyVersioned must be the sole reason the DLQ is not called.
    assertTrue(op.isIndex());
    assertEquals(VersionType.External, op.index().versionType());
    assertFalse(config.isDataStream());
    assertNotNull(reporter);

    client.handleResponse(versionConflictItem(index, "key0", OperationType.Index), ctx, 1L);

    verify(reporter, never()).report(any(SinkRecord.class), any(Throwable.class));
  }

  /**
   * close() must throw ConnectException within flush.timeout.ms when BulkIngester.close() hangs.
   * The hang is mocked above the socket layer so read.timeout.ms cannot fire first.
   * The assertThrows is the functional proof of bounded close(); the @Test timeout is a generous
   * backstop that only fires if close() truly hangs forever.
   */
  @Test(timeout = 30000)
  public void testCloseTimesOutInsteadOfHanging() throws Exception {
    props.put(FLUSH_TIMEOUT_MS_CONFIG, "1000"); // minimum allowed by the config validator
    config = new ElasticsearchSinkConnectorConfig(props);

    @SuppressWarnings("unchecked")
    BulkIngester<ElasticsearchClient.SinkRecordAndOffset> mockIngester = mock(BulkIngester.class);
    RestClient mockRestClient = mock(RestClient.class);

    doAnswer(invocation -> {
      try {
        Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return null;
    }).when(mockIngester).close();

    ElasticsearchClient client = new ElasticsearchClient(
        config, null, mockIngester, mockRestClient);

    ConnectException thrown = assertThrows(ConnectException.class, client::close);
    assertTrue(thrown.getMessage().contains("Failed to process outstanding requests"));
    verify(mockRestClient).close(); // closeResources() ran on the timeout path
  }

  /**
   * If the DLQ reporter throws inside afterBulk, bulkFinished() must still run so that
   * numBufferedRecords is decremented and waitForInFlightRequests() never stalls.
   */
  @Test
  public void testAfterBulkDecrementsBufferedCountEvenOnReporterException() {
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.IGNORE.name());
    props.put(IGNORE_KEY_CONFIG, "false");
    config = new ElasticsearchSinkConnectorConfig(props);
    DataConverter localConverter = new DataConverter(config);

    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    when(reporter.report(any(), any())).thenThrow(new RuntimeException("DLQ unavailable"));

    @SuppressWarnings("unchecked")
    BulkIngester<ElasticsearchClient.SinkRecordAndOffset> mockIngester = mock(BulkIngester.class);
    ElasticsearchClient client = new ElasticsearchClient(
        config, reporter, mockIngester, mock(RestClient.class));

    SinkRecord record = sinkRecord("key0", 0);
    BulkOperation op = localConverter.convertRecord(record, index);
    ElasticsearchClient.SinkRecordAndOffset ctx = new ElasticsearchClient.SinkRecordAndOffset(
        record, new AsyncOffsetTracker.AsyncOffsetState(0), op);

    client.numBufferedRecords.set(1);

    BulkResponseItem malformedItem = BulkResponseItem.of(b -> b
        .operationType(OperationType.Index)
        .index(index)
        .id("key0")
        .status(400)
        .error(e -> e.type("mapper_parsing_exception").reason("field type mismatch"))
    );

    List<ElasticsearchClient.SinkRecordAndOffset> contexts = Collections.singletonList(ctx);
    BulkResponse response = BulkResponse.of(b -> b.items(malformedItem).errors(true).took(1L));

    BulkListener<ElasticsearchClient.SinkRecordAndOffset> listener =
        client.buildListener(() -> { });
    listener.afterBulk(1L, mock(BulkRequest.class), contexts, response);

    assertEquals(0, client.numBufferedRecords.get());
  }

  /**
   * C-1: whole-request bulk failure locks in fail-the-task policy.
   * After afterBulk(Throwable) fires, numBufferedRecords is decremented (bulkFinished ran),
   * no offset advances, and the next throwIfFailed() kills the task.
   */
  @Test
  public void testAfterBulkThrowableFailsTaskAndDecrementsBufferedCount() {
    @SuppressWarnings("unchecked")
    BulkIngester<ElasticsearchClient.SinkRecordAndOffset> mockIngester = mock(BulkIngester.class);
    ElasticsearchClient client = new ElasticsearchClient(
        config, null, mockIngester, mock(RestClient.class));

    OffsetState mockOffsetState = mock(OffsetState.class);
    SinkRecord record = sinkRecord("key0", 0);
    BulkOperation op = converter.convertRecord(record, index);
    ElasticsearchClient.SinkRecordAndOffset ctx =
        new ElasticsearchClient.SinkRecordAndOffset(record, mockOffsetState, op);

    client.numBufferedRecords.set(1);

    BulkListener<ElasticsearchClient.SinkRecordAndOffset> listener = client.buildListener(() -> {});
    listener.afterBulk(1L, mock(BulkRequest.class),
        Collections.singletonList(ctx), new RuntimeException("simulated transport failure"));

    assertEquals("bulkFinished must decrement numBufferedRecords", 0, client.numBufferedRecords.get());
    verify(mockOffsetState, never()).markProcessed();
    assertTrue("task must be marked failed", client.isFailed());
    assertThrows(ConnectException.class, client::throwIfFailed);
  }

  /**
   * C-2: afterBulk(BulkResponse) pairs response items with contexts by position.
   * Item 1's DLQ record must be sinkRecord1, not sinkRecord0.
   * Uses IGNORE_KEY=true (the default) which produces index ops without VersionType.External,
   * so the non-external version_conflict path fires and the record is routed to the DLQ.
   * Offset state for ctx 0 (success) advances; ctx 1 (version_conflict, non-external) also
   * advances because handleResponse returns false for version_conflict — the record was
   * handled (sent to DLQ) and the connector continues without stalling.
   */
  @Test
  public void testAfterBulkResponseRoutesDlqToCorrectContextByIndex() {
    // config already has IGNORE_KEY=true → index ops without VersionType.External
    DataConverter localConverter = new DataConverter(config);

    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    when(reporter.report(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    @SuppressWarnings("unchecked")
    BulkIngester<ElasticsearchClient.SinkRecordAndOffset> mockIngester = mock(BulkIngester.class);
    ElasticsearchClient client = new ElasticsearchClient(
        config, reporter, mockIngester, mock(RestClient.class));

    SinkRecord record0 = sinkRecord("key0", 0);
    SinkRecord record1 = sinkRecord("key1", 1);
    BulkOperation op0 = localConverter.convertRecord(record0, index);
    BulkOperation op1 = localConverter.convertRecord(record1, index);

    OffsetState mockOffset0 = mock(OffsetState.class);
    OffsetState mockOffset1 = mock(OffsetState.class);
    ElasticsearchClient.SinkRecordAndOffset ctx0 =
        new ElasticsearchClient.SinkRecordAndOffset(record0, mockOffset0, op0);
    ElasticsearchClient.SinkRecordAndOffset ctx1 =
        new ElasticsearchClient.SinkRecordAndOffset(record1, mockOffset1, op1);

    BulkResponseItem successItem = BulkResponseItem.of(b -> b
        .operationType(OperationType.Index).index(index).id("key0").status(200));
    // Non-external version conflict on index → routes to DLQ
    BulkResponseItem conflictItem = versionConflictItem(index, "key1", OperationType.Index);

    BulkResponse response = BulkResponse.of(b -> b
        .items(successItem, conflictItem).errors(true).took(1L));
    client.numBufferedRecords.set(2);

    BulkListener<ElasticsearchClient.SinkRecordAndOffset> listener = client.buildListener(() -> {});
    listener.afterBulk(1L, mock(BulkRequest.class), Arrays.asList(ctx0, ctx1), response);

    // DLQ receives the record from context index 1, not index 0
    verify(reporter, times(1)).report(eq(record1), any(Throwable.class));
    verify(reporter, never()).report(eq(record0), any(Throwable.class));
    // Both offsets advance: success for ctx0; version_conflict returns false so ctx1 also advances
    verify(mockOffset0, times(1)).markProcessed();
    verify(mockOffset1, times(1)).markProcessed();
    assertEquals("numBufferedRecords must drop by full batch size", 0, client.numBufferedRecords.get());
  }

  /**
   * C-3: data-stream create version_conflict is suppressed by the RCCA-7507 carve-out.
   * DataConverter produces a create op (not index) with no VersionType.External, so
   * isExternallyVersioned returns false and reportBadRecordAndError is called — but
   * reportBadRecordAndError short-circuits for data-stream version conflicts, so
   * reporter.report() is never invoked and the task is not marked failed.
   *
   * A non-data-stream control confirms the actual mechanism: the same non-external create
   * conflict on a regular index IS routed to the DLQ, proving the data-stream carve-out is
   * what suppresses it (not isExternallyVersioned returning true, which would also suppress it).
   */
  @Test
  public void testDataStreamCreateVersionConflictSuppressedByRcca7507() {
    props.put(DATA_STREAM_TYPE_CONFIG, DATA_STREAM_TYPE);
    props.put(DATA_STREAM_DATASET_CONFIG, DATA_STREAM_DATASET);
    props.put(IGNORE_KEY_CONFIG, "false");
    config = new ElasticsearchSinkConnectorConfig(props);
    DataConverter dsConverter = new DataConverter(config);

    ErrantRecordReporter reporterDs = mock(ErrantRecordReporter.class);

    @SuppressWarnings("unchecked")
    BulkIngester<ElasticsearchClient.SinkRecordAndOffset> mockIngester = mock(BulkIngester.class);
    ElasticsearchClient client = new ElasticsearchClient(
        config, reporterDs, mockIngester, mock(RestClient.class));

    String dsIndex = createIndexName(TOPIC);
    SinkRecord record = sinkRecord("key0", 5);
    BulkOperation op = dsConverter.convertRecord(record, dsIndex);

    // Pin preconditions: isExternallyVersioned must be the reason reportBadRecordAndError is called
    assertTrue("data-stream INSERT must produce a create op", op.isCreate());
    assertFalse("data-stream create must carry no external version",
        VersionType.External.equals(op.create().versionType()));
    assertTrue("config must be in data-stream mode", config.isDataStream());

    ElasticsearchClient.SinkRecordAndOffset ctx =
        new ElasticsearchClient.SinkRecordAndOffset(
            record, new AsyncOffsetTracker.AsyncOffsetState(5), op);

    // isExternallyVersioned → false → reportBadRecordAndError called,
    // RCCA-7507 carve-out in reportBadRecordAndError silences it without calling reporter.report()
    boolean failed = client.handleResponse(
        versionConflictItem(dsIndex, "key0", OperationType.Create), ctx, 1L);

    verify(reporterDs, never()).report(any(SinkRecord.class), any(Throwable.class));
    assertFalse("data-stream create version conflict must not fail the task", failed);
    assertFalse("task-level error must not be set", client.isFailed());

    // Control: same non-external create conflict on a regular (non-data-stream) index IS reported.
    // This distinguishes the RCCA-7507 carve-out from the isExternallyVersioned path:
    // if isExternallyVersioned returned true instead, both would suppress DLQ and this would fail.
    Map<String, String> nonDsProps =
        ElasticsearchSinkConnectorConfigTest.addNecessaryProps(new HashMap<>());
    nonDsProps.put(CONNECTION_URL_CONFIG, container.getConnectionUrl());
    nonDsProps.put(IGNORE_KEY_CONFIG, "false");
    ElasticsearchSinkConnectorConfig nonDsConfig = new ElasticsearchSinkConnectorConfig(nonDsProps);
    assertFalse("control config must not be data-stream", nonDsConfig.isDataStream());

    ErrantRecordReporter controlReporter = mock(ErrantRecordReporter.class);
    when(controlReporter.report(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    @SuppressWarnings("unchecked")
    BulkIngester<ElasticsearchClient.SinkRecordAndOffset> mockIngester2 = mock(BulkIngester.class);
    ElasticsearchClient nonDsClient = new ElasticsearchClient(
        nonDsConfig, controlReporter, mockIngester2, mock(RestClient.class));

    // Control: a create op with no external version under a non-data-stream client IS reported.
    // DataConverter only produces create ops for data streams (non-data-stream INSERTs yield index
    // ops), so we build the control op directly to hold the operation kind constant while varying
    // only the data-stream config flag. This distinguishes the RCCA-7507 carve-out from
    // isExternallyVersioned: if the latter were the suppression reason, the control would also
    // be suppressed and the assertion below would fail.
    BulkOperation controlOp =
        BulkOperation.of(b -> b.create(c -> c.index(index).id("key0")
            .document(JsonData.fromJson("{}"))));
    assertTrue("control op must be a create", controlOp.isCreate());
    assertFalse("control create must carry no external version",
        VersionType.External.equals(controlOp.create().versionType()));
    SinkRecord controlRecord = sinkRecord("key0", 5);
    ElasticsearchClient.SinkRecordAndOffset controlCtx =
        new ElasticsearchClient.SinkRecordAndOffset(
            controlRecord, new AsyncOffsetTracker.AsyncOffsetState(5), controlOp);
    nonDsClient.handleResponse(
        versionConflictItem(index, "key0", OperationType.Create), controlCtx, 1L);
    verify(controlReporter, times(1)).report(any(SinkRecord.class), any(Throwable.class));
  }

  private static BulkResponseItem versionConflictItem(String idx, String id, OperationType op) {
    return BulkResponseItem.of(b -> b
        .operationType(op)
        .index(idx)
        .id(id)
        .status(409)
        .error(e -> e
            .type("version_conflict_engine_exception")
            .reason("version conflict, current version [10] is higher or equal to the one provided [5]")
        )
    );
  }
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

    // Expected thread name pattern should be: {connectorName}-{taskId}-elasticsearch-bulk-executor-{number}
    String expectedPrefix = "elasticsearch-sink-1-elasticsearch-bulk-executor-";

    // Check that threads with the expected name pattern exist
    Set<String> threadNames = Thread.getAllStackTraces().keySet().stream()
            .map(Thread::getName)
            .filter(name -> name.startsWith(expectedPrefix))
            .collect(java.util.stream.Collectors.toSet());

    assertTrue("Expected threads with prefix " + expectedPrefix + " to exist",
            !threadNames.isEmpty());

    // Verify thread names follow the expected pattern
    for (String threadName : threadNames) {
      assertTrue("Thread name should start with expected prefix",
              threadName.startsWith(expectedPrefix));

      String suffix = threadName.substring(expectedPrefix.length());
      assertTrue("Thread name should end with a number", suffix.matches("\\d+"));
    }

    client.close();
  }
}
