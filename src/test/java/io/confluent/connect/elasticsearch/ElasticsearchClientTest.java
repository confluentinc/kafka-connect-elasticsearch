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
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_DATASET_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_TYPE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SECURITY_PROTOCOL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SSL_CONFIG_PREFIX;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WRITE_METHOD_CONFIG;
import static io.confluent.connect.elasticsearch.helper.ElasticsearchContainer.ELASTIC_PASSWORD;
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

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnMalformedDoc;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnNullValues;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SecurityProtocol;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WriteMethod;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import io.confluent.connect.elasticsearch.helper.ElasticsearchHelperClient;
import io.confluent.connect.elasticsearch.helper.NetworkErrorContainer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.test.TestUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ElasticsearchClientTest {

  private static final String TOPIC = "index";

  private static ElasticsearchContainer container;

  private DataConverter converter;
  private ElasticsearchHelperClient helperClient;
  private ElasticsearchSinkConnectorConfig config;
  private Map<String, String> props;
  private String index;

  @BeforeClass
  public static void setupBeforeAll() {
    container = ElasticsearchContainer.fromSystemProperties();
    container.start();
  }

  @AfterClass
  public static void cleanupAfterAll() {
    container.close();
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
  }

  @After
  public void cleanup() throws IOException {
    if (helperClient != null && helperClient.indexExists(index)){
      helperClient.deleteIndex(index, config.isDataStream());
    }
  }

  @Test
  public void testClose() {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.close();
  }

  @Test
  public void testCloseFails() throws Exception {
    props.put(BATCH_SIZE_CONFIG, "1");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    ElasticsearchClient client = new ElasticsearchClient(config, null) {
      @Override
      public void close() {
        try {
          if (!bulkProcessor.awaitClose(1, TimeUnit.MILLISECONDS)) {
            throw new ConnectException("Failed to process all outstanding requests in time.");
          }
        } catch (InterruptedException e) {}
      }
    };

    writeRecord(sinkRecord(0), client);
    assertThrows(
        "Failed to process all outstanding requests in time.",
        ConnectException.class,
        () -> client.close()
    );
    waitUntilRecordsInES(1);
  }

  @Test
  public void testCreateIndex() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    assertFalse(helperClient.indexExists(index));

    client.createIndexOrDataStream(index);
    assertTrue(helperClient.indexExists(index));
    client.close();
  }

  @Test
  public void testCreateExistingDataStream() throws Exception {
    String type = "logs";
    String dataset = "dataset";
    index = String.format("%s-%s-%s", type, dataset, TOPIC); // data stream name
    props.put(DATA_STREAM_TYPE_CONFIG, type);
    props.put(DATA_STREAM_DATASET_CONFIG, dataset);
    config = new ElasticsearchSinkConnectorConfig(props);
    ElasticsearchClient client = new ElasticsearchClient(config, null);

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));
    assertFalse(client.createIndexOrDataStream(index));
    client.close();
  }

  @Test
  public void testCreateNewDataStream() throws Exception {
    String type = "logs";
    String dataset = "dataset";
    index = String.format("%s-%s-%s", type, dataset, TOPIC); // data stream name
    props.put(DATA_STREAM_TYPE_CONFIG, type);
    props.put(DATA_STREAM_DATASET_CONFIG, dataset);
    config = new ElasticsearchSinkConnectorConfig(props);
    ElasticsearchClient client = new ElasticsearchClient(config, null);

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));
    client.close();
  }

  @Test
  public void testDoesNotCreateAlreadyExistingIndex() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    assertFalse(helperClient.indexExists(index));

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));

    assertFalse(client.createIndexOrDataStream(index));
    assertTrue(helperClient.indexExists(index));
    client.close();
  }

  @Test
  public void testIndexExists() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    assertFalse(helperClient.indexExists(index));

    assertTrue(client.createIndexOrDataStream(index));
    assertTrue(client.indexExists(index));
    client.close();
  }

  @Test
  public void testIndexDoesNotExist() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    assertFalse(helperClient.indexExists(index));

    assertFalse(client.indexExists(index));
    client.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCreateMapping() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndexOrDataStream(index);

    client.createMapping(index, schema());

    assertTrue(client.hasMapping(index));

    Map<String, Object> mapping = helperClient.getMapping(index).sourceAsMap();
    assertTrue(mapping.containsKey("properties"));
    Map<String, Object> props = (Map<String, Object>) mapping.get("properties");
    assertTrue(props.containsKey("offset"));
    assertTrue(props.containsKey("another"));
    Map<String, Object> offset = (Map<String, Object>) props.get("offset");
    assertEquals("integer", offset.get("type"));
    assertEquals(0, offset.get("null_value"));
    Map<String, Object> another = (Map<String, Object>) props.get("another");
    assertEquals("integer", another.get("type"));
    assertEquals(0, another.get("null_value"));
    client.close();
  }

  @Test
  public void testHasMapping() {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndexOrDataStream(index);

    client.createMapping(index, schema());

    assertTrue(client.hasMapping(index));
    client.close();
  }

  @Test
  public void testDoesNotHaveMapping() {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndexOrDataStream(index);

    assertFalse(client.hasMapping(index));
    client.close();
  }

  @Test
  public void testBuffersCorrectly() throws Exception {
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    props.put(MAX_BUFFERED_RECORDS_CONFIG, "1");
    config = new ElasticsearchSinkConnectorConfig(props);
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    assertEquals(1, client.numRecords.get());
    client.flush();

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(index));
    assertEquals(0, client.numRecords.get());

    writeRecord(sinkRecord(1), client);
    assertEquals(1, client.numRecords.get());

    // will block until the previous record is flushed
    writeRecord(sinkRecord(2), client);
    assertEquals(1, client.numRecords.get());

    waitUntilRecordsInES(3);
    client.close();
  }

  @Test
  public void testFlush() throws Exception {
    props.put(LINGER_MS_CONFIG, String.valueOf(TimeUnit.DAYS.toMillis(1)));
    config = new ElasticsearchSinkConnectorConfig(props);
    ElasticsearchClient client = new ElasticsearchClient(config, null);
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
    ElasticsearchClient client = new ElasticsearchClient(config, null);
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
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord("key0", 0), client);
    writeRecord(sinkRecord("key1", 1), client);
    client.flush();

    waitUntilRecordsInES(2);

    // delete 1
    SinkRecord deleteRecord = new SinkRecord(TOPIC, 0, Schema.STRING_SCHEMA, "key0", null, null, 3);
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
    ElasticsearchClient client = new ElasticsearchClient(config, null);
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
    SinkRecord upsertRecord = new SinkRecord(index, 0, Schema.STRING_SCHEMA, "key0", schema, value, 2);
    Struct value2 = new Struct(schema).put("offset", 3);
    SinkRecord upsertRecord2 = new SinkRecord(index, 0, Schema.STRING_SCHEMA, "key0", schema, value2, 3);


    // upsert 2, write another
    writeRecord(upsertRecord, client);
    writeRecord(upsertRecord2, client);
    writeRecord(sinkRecord("key2", 4), client);
    client.flush();

    waitUntilRecordsInES(3);
    for (SearchHit hit : helperClient.search(index)) {
      if (hit.getId().equals("key0")) {
        assertEquals(3, hit.getSourceAsMap().get("offset"));
        assertEquals(0, hit.getSourceAsMap().get("another"));
      }
    }

    client.close();
  }

  @Test
  public void testIgnoreBadRecord() throws Exception {
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.IGNORE.name());
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndexOrDataStream(index);
    client.createMapping(index, schema());

    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("not_mapped_field", SchemaBuilder.int32().defaultValue(0).build())
        .build();
    Struct value = new Struct(schema).put("not_mapped_field", 420);
    SinkRecord badRecord = new SinkRecord(TOPIC, 0, Schema.STRING_SCHEMA, "key", schema, value, 0);

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
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndexOrDataStream(index);
    client.createMapping(index, schema());

    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("offset", SchemaBuilder.bool().defaultValue(false).build())
        .build();
    Struct value = new Struct(schema).put("offset", false);
    SinkRecord badRecord = new SinkRecord(TOPIC, 0, Schema.STRING_SCHEMA, "key", schema, value, 0);

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
  public void testRetryRecordsOnFailure() throws Exception {
    props.put(LINGER_MS_CONFIG, "60000");
    props.put(BATCH_SIZE_CONFIG, "2");
    props.put(MAX_RETRIES_CONFIG, "100");
    props.put(RETRY_BACKOFF_MS_CONFIG, "1000");
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);


    // mock bulk processor to throw errors
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndexOrDataStream(index);

    // bring down ES service
    NetworkErrorContainer delay = new NetworkErrorContainer(container.getContainerName());
    delay.start();

    // attempt a write
    writeRecord(sinkRecord(0), client);
    client.flush();

    // keep the ES service down for a couple of timeouts
    Thread.sleep(config.readTimeoutMs() * 4);

    // bring up ES service
    delay.stop();

    waitUntilRecordsInES(1);
  }

  @Test
  public void testReporter() throws Exception {
    props.put(IGNORE_KEY_CONFIG, "false");
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.IGNORE.name());
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    ElasticsearchClient client = new ElasticsearchClient(config, reporter);
    client.createIndexOrDataStream(index);
    client.createMapping(index, schema());

    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("offset", SchemaBuilder.bool().defaultValue(false).build())
        .build();
    Struct value = new Struct(schema).put("offset", false);
    SinkRecord badRecord = new SinkRecord(index, 0, Schema.STRING_SCHEMA, "key0", schema, value, 1);

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
    ElasticsearchClient client = new ElasticsearchClient(config, reporter);
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

  @Test
  public void testNoVersionConflict() throws Exception {
    props.put(IGNORE_KEY_CONFIG, "false");
    props.put(WRITE_METHOD_CONFIG, WriteMethod.UPSERT.name());
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    ErrantRecordReporter reporter2 = mock(ErrantRecordReporter.class);
    ElasticsearchClient client = new ElasticsearchClient(config, reporter);
    ElasticsearchClient client2 = new ElasticsearchClient(config, reporter2);

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
  public void testSsl() throws Exception {
    container.close();
    container = ElasticsearchContainer.fromSystemProperties().withSslEnabled(true);
    container.start();

    String address = container.getConnectionUrl().replace(container.getContainerIpAddress(), container.hostMachineIpAddress());
    props.put(CONNECTION_URL_CONFIG, address);
    props.put(CONNECTION_USERNAME_CONFIG, "elastic");
    props.put(CONNECTION_PASSWORD_CONFIG, ELASTIC_PASSWORD);
    props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, container.getKeystorePath());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, container.getKeystorePassword());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, container.getTruststorePath());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, container.getTruststorePassword());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEY_PASSWORD_CONFIG, container.getKeyPassword());
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ElasticsearchClient client = new ElasticsearchClient(config, null);
    helperClient = new ElasticsearchHelperClient(address, config);
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    client.flush();

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(index));
    client.close();
    helperClient = null;

    container.close();
    container = ElasticsearchContainer.fromSystemProperties();
    container.start();
  }

  private static Schema schema() {
    return SchemaBuilder
        .struct()
        .name("record")
        .field("offset", SchemaBuilder.int32().defaultValue(0).build())
        .field("another", SchemaBuilder.int32().defaultValue(0).build())
        .build();
  }

  private static SinkRecord sinkRecord(int offset) {
    return sinkRecord("key", offset);
  }

  private static SinkRecord sinkRecord(String key, int offset) {
    Struct value = new Struct(schema()).put("offset", offset).put("another", offset + 1);
    return new SinkRecord(TOPIC, 0, Schema.STRING_SCHEMA, key, schema(), value, offset);
  }

  private void waitUntilRecordsInES(int expectedRecords) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> {
          try {
            return helperClient.getDocCount(index) == expectedRecords;
          } catch (ElasticsearchStatusException e) {
            if (e.getMessage().contains("index_not_found_exception")) {
              return false;
            }

            throw e;
          }
        },
        TimeUnit.MINUTES.toMillis(1),
        String.format("Could not find expected documents (%d) in time.", expectedRecords)
    );
  }

  private void writeRecord(SinkRecord record, ElasticsearchClient client) {
    client.index(record, converter.convertRecord(record, record.topic()));
  }
}
