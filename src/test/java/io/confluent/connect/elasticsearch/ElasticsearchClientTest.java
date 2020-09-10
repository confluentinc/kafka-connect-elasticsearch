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

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WRITE_METHOD_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnMalformedDoc;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnNullValues;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WriteMethod;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import io.confluent.connect.elasticsearch.helper.ElasticsearchHelperClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.test.TestUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.shaded.okio.Sink;

public class ElasticsearchClientTest {

  private static final String INDEX = "index";

  private static ElasticsearchContainer container;

  private DataConverter converter;
  private ElasticsearchHelperClient helperClient;
  private ElasticsearchSinkConnectorConfig config;
  private Map<String, String> props;

  @BeforeClass
  public static void setupBeforeAll() {
    container = ElasticsearchContainer.fromSystemProperties();
    container.start();
  }

  @AfterClass
  public static void cleanuoAfterAll() {
    container.close();
  }

  @Before
  public void setup() {
    props = ElasticsearchSinkConnectorConfigTest.addNecessaryProps(new HashMap<>());
    props.put(CONNECTION_URL_CONFIG, container.getConnectionUrl());
    props.put(IGNORE_KEY_CONFIG, "true");
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);
    helperClient = new ElasticsearchHelperClient(container.getConnectionUrl());
  }

  @After
  public void cleanup() throws IOException {
    if (helperClient.indexExists(INDEX)){
      helperClient.deleteIndex(INDEX);
    }
  }

  @Test
  public void testClose() {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.close();
  }

  @Test
  public void testCreateIndex() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    assertFalse(helperClient.indexExists(INDEX));

    client.createIndex(INDEX);
    assertTrue(helperClient.indexExists(INDEX));
  }

  @Test
  public void testIndexAlreadyExists() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    assertFalse(helperClient.indexExists(INDEX));

    client.createIndex(INDEX);
    assertTrue(helperClient.indexExists(INDEX));

    client.createIndex(INDEX);
    assertTrue(helperClient.indexExists(INDEX));
  }

  @Test
  public void testIndexExists() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    assertFalse(helperClient.indexExists(INDEX));

    client.createIndex(INDEX);
    assertTrue(client.indexExists(INDEX));
  }

  @Test
  public void testIndexDoesNotExist() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    assertFalse(helperClient.indexExists(INDEX));

    assertFalse(client.indexExists(INDEX));
  }

  @Test
  public void testCreateMapping() throws IOException {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndex(INDEX);

    client.createMapping(INDEX, schema());

    assertTrue(client.hasMapping(INDEX));

    XContentBuilder builder = Mapping.buildMapping(sinkRecord(0).valueSchema());
    builder.flush();
    ByteArrayOutputStream stream = (ByteArrayOutputStream) builder.getOutputStream();
    System.out.println(stream.toString());

    helperClient.getMapping(INDEX).sourceAsMap();
    XContentBuilder mapping = XContentFactory.jsonBuilder();
    mapping.map(helperClient.getMapping(INDEX).sourceAsMap());
    mapping.flush();
    stream = (ByteArrayOutputStream) mapping.getOutputStream();
    System.out.println(stream.toString());
  }

  @Test
  public void testHasMapping() {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndex(INDEX);

    client.createMapping(INDEX, schema());

    assertTrue(client.hasMapping(INDEX));
  }

  @Test
  public void testDoesNotHaveMapping() {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndex(INDEX);

    assertFalse(client.hasMapping(INDEX));
  }

  @Test
  public void testFlush() throws Exception {
    props.put(LINGER_MS_CONFIG, String.valueOf(Integer.MAX_VALUE));
    config = new ElasticsearchSinkConnectorConfig(props);
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndex(INDEX);

    client.index(sinkRecord(0), converter.convertRecord(sinkRecord(0), INDEX));

    assertEquals(0, helperClient.getDocCount(INDEX)); // should be empty before flush
    client.flush();
    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(INDEX));
  }

  @Test
  public void testIndexRecord() throws Exception {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndex(INDEX);

    client.index(sinkRecord(0), converter.convertRecord(sinkRecord(0), INDEX));
    client.flush();

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(INDEX));
  }

  @Test
  public void testDeleteRecord() throws Exception {
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.DELETE.name());
    props.put(IGNORE_KEY_CONFIG, "false");
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndex(INDEX);

    client.index(sinkRecord("key0", 0), converter.convertRecord(sinkRecord("key0", 0), INDEX));
    client.index(sinkRecord("key1", 1), converter.convertRecord(sinkRecord("key1", 1), INDEX));
    client.flush();

    waitUntilRecordsInES(2);

    // delete 1
    SinkRecord deleteRecord = new SinkRecord(INDEX, 0, Schema.STRING_SCHEMA, "key0", null, null, 3);
    client.index(deleteRecord, converter.convertRecord(deleteRecord, INDEX));

    waitUntilRecordsInES(1);
  }

  @Test
  public void testUpsertRecord() throws Exception {
    props.put(WRITE_METHOD_CONFIG, WriteMethod.UPSERT.name());
    props.put(IGNORE_KEY_CONFIG, "false");
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndex(INDEX);

    client.index(sinkRecord("key0", 0), converter.convertRecord(sinkRecord("key0", 0), INDEX));
    client.index(sinkRecord("key1", 1), converter.convertRecord(sinkRecord("key1", 1), INDEX));
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
    SinkRecord upsertRecord = new SinkRecord(INDEX, 0, Schema.STRING_SCHEMA, "key0", schema, value, 2);

    // upsert 1, write another
    client.index(upsertRecord, converter.convertRecord(upsertRecord, INDEX));
    client.index(sinkRecord("key2", 3), converter.convertRecord(sinkRecord("key2", 3), INDEX));
    client.flush();

    waitUntilRecordsInES(3);
    for (SearchHit hit : helperClient.search(INDEX)) {
      if (hit.getId().equals("key0")) {
        assertEquals(2, hit.getSourceAsMap().get("offset"));
        assertEquals(0, hit.getSourceAsMap().get("another"));
      }
    }
  }

  @Test
  public void testIgnoreBadRecord() throws Exception {
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.IGNORE.name());
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndex(INDEX);
    client.createMapping(INDEX, schema());

    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("not_mapped_field", SchemaBuilder.int32().defaultValue(0).build())
        .build();
    Struct value = new Struct(schema).put("not_mapped_field", 420);
    SinkRecord badRecord = new SinkRecord(INDEX, 0, Schema.STRING_SCHEMA, "key", schema, value, 0);

    client.index(sinkRecord(0), converter.convertRecord(sinkRecord(0), INDEX));
    client.flush();

    client.index(badRecord, converter.convertRecord(badRecord, INDEX));
    client.flush();

    client.index(sinkRecord(1), converter.convertRecord(sinkRecord(1), INDEX));
    client.flush();

    waitUntilRecordsInES(2);
    assertEquals(2, helperClient.getDocCount(INDEX));
  }

  @Test(expected = ConnectException.class)
  public void testFailOnBadRecord() throws Exception {
    ElasticsearchClient client = new ElasticsearchClient(config, null);
    client.createIndex(INDEX);
    client.createMapping(INDEX, schema());

    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("offset", SchemaBuilder.bool().defaultValue(false).build())
        .build();
    Struct value = new Struct(schema).put("offset", false);
    SinkRecord badRecord = new SinkRecord(INDEX, 0, Schema.STRING_SCHEMA, "key", schema, value, 0);

    client.index(sinkRecord(0), converter.convertRecord(sinkRecord(0), INDEX));
    client.flush();

    waitUntilRecordsInES(1);
    client.index(badRecord, converter.convertRecord(badRecord, INDEX));
    client.flush();

    // consecutive index calls should cause exception
    for (int i = 0; i < 10; i++) {
      client.index(sinkRecord(i + 1), converter.convertRecord(sinkRecord(i + 1), INDEX));
      client.flush();
      waitUntilRecordsInES(i + 2);
    }
  }

  @Test
  public void testReporter() throws Exception {
    props.put(IGNORE_KEY_CONFIG, "false");
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.IGNORE.name());
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);

    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    ElasticsearchClient client = new ElasticsearchClient(config, reporter);
    client.createIndex(INDEX);
    client.createMapping(INDEX, schema());

    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("offset", SchemaBuilder.bool().defaultValue(false).build())
        .build();
    Struct value = new Struct(schema).put("offset", false);
    SinkRecord badRecord = new SinkRecord(INDEX, 0, Schema.STRING_SCHEMA, "key", schema, value, 1);

    client.index(sinkRecord(1), converter.convertRecord(sinkRecord(1), INDEX));
    client.flush();
    waitUntilRecordsInES(1);

    client.index(badRecord, converter.convertRecord(badRecord, INDEX));
    client.flush();

    // failed requests take a bit longer
    for (int i = 0; i < 10; i++) {
      client.index(sinkRecord("key" + i, i + 1), converter.convertRecord(sinkRecord("key" + i, i + 1), INDEX));
      client.flush();
      waitUntilRecordsInES(i + 2);
    }

    verify(reporter, times(1)).report(eq(badRecord), any(Throwable.class));
  }

  @Test
  public void testReporterNotCalled() throws Exception {
    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    ElasticsearchClient client = new ElasticsearchClient(config, reporter);
    client.createIndex(INDEX);

    client.index(sinkRecord(0), converter.convertRecord(sinkRecord(0), INDEX));
    client.index(sinkRecord(1), converter.convertRecord(sinkRecord(1), INDEX));
    client.index(sinkRecord(2), converter.convertRecord(sinkRecord(2), INDEX));
    client.flush();

    waitUntilRecordsInES(3);
    assertEquals(3, helperClient.getDocCount(INDEX));
    verify(reporter, never()).report(eq(sinkRecord(0)), any(Throwable.class));
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

    client.createIndex(INDEX);

    client.index(sinkRecord(0), converter.convertRecord(sinkRecord(0), INDEX));
    client2.index(sinkRecord(1), converter.convertRecord(sinkRecord(1), INDEX));
    client.index(sinkRecord(2), converter.convertRecord(sinkRecord(2), INDEX));
    client2.index(sinkRecord(3), converter.convertRecord(sinkRecord(3), INDEX));
    client.index(sinkRecord(4), converter.convertRecord(sinkRecord(4), INDEX));
    client2.index(sinkRecord(5), converter.convertRecord(sinkRecord(5), INDEX));

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(INDEX));
    verify(reporter, never()).report(any(SinkRecord.class), any(Throwable.class));
    verify(reporter2, never()).report(any(SinkRecord.class), any(Throwable.class));
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
    return new SinkRecord(INDEX, 0, Schema.STRING_SCHEMA, key, schema(), value, offset);
  }



  private void waitUntilRecordsInES(int expectedRecords) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> {
          try {
            return helperClient.getDocCount(INDEX) == expectedRecords;
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
}
