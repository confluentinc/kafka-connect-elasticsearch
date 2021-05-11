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

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_DATASET_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_TYPE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DROP_INVALID_MESSAGE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnNullValues;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.elasticsearch.action.DocWriteRequest;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Collections;

public class ElasticsearchSinkTaskTest {

  protected static final String TOPIC = "topic";

  protected ElasticsearchClient client;
  private ElasticsearchSinkTask task;
  private Map<String, String> props;
  private SinkTaskContext context;

  private void setUpTask() {
    task = new ElasticsearchSinkTask();
    task.initialize(context);
    task.start(props, client);
  }

  @Before
  public void setUp() {
    props = ElasticsearchSinkConnectorConfigTest.addNecessaryProps(new HashMap<>());
    props.put(IGNORE_KEY_CONFIG, "true");

    client = mock(ElasticsearchClient.class);
    context = mock(SinkTaskContext.class);

    setUpTask();
  }

  @Test
  public void testPutSkipNullRecords() {
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.IGNORE.name());
    setUpTask();

    // skip null
    SinkRecord nullRecord = record(true, true, 0);
    task.put(Collections.singletonList(nullRecord));
    verify(client, never()).index(eq(nullRecord), any(DocWriteRequest.class));

    // don't skip non-null
    SinkRecord notNullRecord = record(true, false,1);
    task.put(Collections.singletonList(notNullRecord));
    verify(client, times(1)).index(eq(notNullRecord), any(DocWriteRequest.class));
  }

  @Test
  public void testReportNullRecords() {
    ErrantRecordReporter mockReporter = mock(ErrantRecordReporter.class);
    when(context.errantRecordReporter()).thenReturn(mockReporter);

    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.IGNORE.name());
    setUpTask();

    // report null
    SinkRecord nullRecord = record(true, true, 0);
    task.put(Collections.singletonList(nullRecord));
    verify(client, never()).index(eq(nullRecord), any(DocWriteRequest.class));
    verify(mockReporter, times(1)).report(eq(nullRecord), any(ConnectException.class));

    // don't report
    SinkRecord notNullRecord = record(true, false,1);
    task.put(Collections.singletonList(notNullRecord));
    verify(client, times(1)).index(eq(notNullRecord), any(DocWriteRequest.class));
    verify(mockReporter, never()).report(eq(notNullRecord), any(ConnectException.class));
  }

  @Test(expected = DataException.class)
  public void testPutFailNullRecords() {
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.FAIL.name());
    setUpTask();

    // fail null
    SinkRecord nullRecord = record(true, true, 0);
    task.put(Collections.singletonList(nullRecord));
  }

  @Test
  public void testCreateIndex() {
    task.put(Collections.singletonList(record()));
    verify(client, times(1)).createIndex(eq(TOPIC));
  }

  @Test
  public void testCreateUpperCaseIndex() {
    task.put(Collections.singletonList(record()));
    verify(client, times(1)).createIndex(eq(TOPIC.toLowerCase()));
  }

  @Test
  public void testDoNotCreateCachedIndex() {
    task.put(Collections.singletonList(record()));
    verify(client, times(1)).createIndex(eq(TOPIC));

    task.put(Collections.singletonList(record()));
    verify(client, times(1)).createIndex(eq(TOPIC));
  }

  @Test
  public void testIgnoreSchema() {
    props.put(IGNORE_SCHEMA_CONFIG, "true");
    setUpTask();

    SinkRecord record = record();
    task.put(Collections.singletonList(record));
    verify(client, never()).hasMapping(eq(TOPIC));
    verify(client, never()).createMapping(eq(TOPIC), eq(record.valueSchema()));
  }

  @Test
  public void testCheckMapping() {
    when(client.hasMapping(TOPIC)).thenReturn(true);

    SinkRecord record = record();
    task.put(Collections.singletonList(record));
    verify(client, times(1)).hasMapping(eq(TOPIC));
    verify(client, never()).createMapping(eq(TOPIC), eq(record.valueSchema()));
  }

  @Test
  public void testAddMapping() {
    SinkRecord record = record();
    task.put(Collections.singletonList(record));
    verify(client, times(1)).hasMapping(eq(TOPIC));
    verify(client, times(1)).createMapping(eq(TOPIC), eq(record.valueSchema()));
  }

  @Test
  public void testDoNotAddCachedMapping() {
    SinkRecord record = record();
    task.put(Collections.singletonList(record));
    verify(client, times(1)).hasMapping(eq(TOPIC));
    verify(client, times(1)).createMapping(eq(TOPIC), eq(record.valueSchema()));

    task.put(Collections.singletonList(record));
    verify(client, times(1)).hasMapping(eq(TOPIC));
    verify(client, times(1)).createMapping(eq(TOPIC), eq(record.valueSchema()));
  }

  @Test
  public void testPut() {
    SinkRecord record = record();
    task.put(Collections.singletonList(record));
    verify(client, times(1)).index(eq(record), any());
  }

  @Test
  public void testPutSkipInvalidRecord() {
    props.put(DROP_INVALID_MESSAGE_CONFIG, "true");
    props.put(IGNORE_KEY_CONFIG, "false");
    setUpTask();

    // skip invalid
    SinkRecord invalidRecord = record(true, 0);
    task.put(Collections.singletonList(invalidRecord));
    verify(client, never()).index(eq(invalidRecord), any(DocWriteRequest.class));

    // don't skip valid
    SinkRecord validRecord = record(false, 1);
    task.put(Collections.singletonList(validRecord));
    verify(client, times(1)).index(eq(validRecord), any(DocWriteRequest.class));
  }

  @Test
  public void testPutReportInvalidRecord() {
    ErrantRecordReporter mockReporter = mock(ErrantRecordReporter.class);
    when(context.errantRecordReporter()).thenReturn(mockReporter);

    props.put(DROP_INVALID_MESSAGE_CONFIG, "true");
    props.put(IGNORE_KEY_CONFIG, "false");
    setUpTask();

    // report invalid
    SinkRecord invalidRecord = record(true, 0);
    task.put(Collections.singletonList(invalidRecord));
    verify(client, never()).index(eq(invalidRecord), any(DocWriteRequest.class));
    verify(mockReporter, times(1)).report(eq(invalidRecord), any(DataException.class));

    // don't report valid
    SinkRecord validRecord = record(false, 1);
    task.put(Collections.singletonList(validRecord));
    verify(client, times(1)).index(eq(validRecord), any(DocWriteRequest.class));
    verify(mockReporter, never()).report(eq(validRecord), any(DataException.class));
  }

  @Test(expected = DataException.class)
  public void testPutFailsOnInvalidRecord() {
    props.put(DROP_INVALID_MESSAGE_CONFIG, "false");
    props.put(IGNORE_KEY_CONFIG, "false");
    setUpTask();

    SinkRecord invalidRecord = record();
    task.put(Collections.singletonList(invalidRecord));
  }

  @Test
  public void testFlush() {
    setUpTask();
    task.flush(null);
    verify(client, times(1)).flush();
  }

  @Test
  public void testFlushDoesNotThrow() {
    setUpTask();
    doThrow(new IllegalStateException("already closed")).when(client).flush();

    // should not throw
    task.flush(null);
    verify(client, times(1)).flush();
  }

  @Test
  public void testStartAndStop() {
    task = new ElasticsearchSinkTask();
    task.initialize(context);
    task.start(props);
    task.stop();
  }

  @Test
  public void testVersion() {
    setUpTask();
    assertNotNull(task.version());
    assertFalse(task.version().equals("0.0.0.0"));
    // Match semver with potentially a qualifier in the end
    assertTrue(task.version().matches("^(\\d+\\.){2}?(\\*|\\d+)(-.*)?$"));
  }

  @Test
  public void testConvertTopicToDataStreamAllowsDashes() {
    final String type = "logs";
    final String dataset = "a_valid_dataset";
    props.put(DATA_STREAM_TYPE_CONFIG, type);
    props.put(DATA_STREAM_DATASET_CONFIG, dataset);
    setUpTask();

    String startsWithDash = "-dash";
    SinkRecord record = record(startsWithDash, true, false, 0);
    task.put(Collections.singletonList(record));
    String indexName = convertToIndexTemplate(type, dataset, startsWithDash);
    verify(client, times(1)).createIndex(eq(indexName));
  }

  @Test
  public void testConvertTopicToDataStream_allowEmpty() {
    final String type = "logs";
    final String dataset = "a_valid_dataset";
    props.put(DATA_STREAM_TYPE_CONFIG, type);
    props.put(DATA_STREAM_DATASET_CONFIG, dataset);
    setUpTask();

    String emptyStr = "";
    SinkRecord record = record(emptyStr, true, false, 0);
    task.put(Collections.singletonList(record));
    String indexName = convertToIndexTemplate(type, dataset, emptyStr);
    verify(client, times(1)).createIndex(eq(indexName));
  }

  @Test
  public void testConvertTopicToDataStream_allowUnderscores() {
    final String type = "logs";
    final String dataset = "a_valid_dataset";
    props.put(DATA_STREAM_TYPE_CONFIG, type);
    props.put(DATA_STREAM_DATASET_CONFIG, dataset);
    setUpTask();

    String startsWithUnderscore = "_underscore";
    SinkRecord record = record(startsWithUnderscore, true, false, 0);
    task.put(Collections.singletonList(record));
    String indexName = convertToIndexTemplate(type, dataset, startsWithUnderscore);
    verify(client, times(1)).createIndex(eq(indexName));
  }

  @Test
  public void testConvertTopicToDataStream_tooLong() {
    final String type = "logs";
    final String dataset = "a_valid_dataset";
    props.put(DATA_STREAM_TYPE_CONFIG, type);
    props.put(DATA_STREAM_DATASET_CONFIG, dataset);
    setUpTask();

    String tooLongTopic = String.format("%0101d", 1);
    SinkRecord record = record(tooLongTopic, true, false, 0);
    task.put(Collections.singletonList(record));
    String indexName = convertToIndexTemplate(type, dataset, tooLongTopic.substring(0, 100));
    verify(client, times(1)).createIndex(eq(indexName));
  }

  @Test
  public void testConvertTopicToDataStream_upperCase() {
    final String type = "logs";
    final String dataset = "a_valid_dataset";
    props.put(DATA_STREAM_TYPE_CONFIG, type);
    props.put(DATA_STREAM_DATASET_CONFIG, dataset);
    setUpTask();

    String upperCaseTopic = "UPPERCASE";
    SinkRecord record = record(upperCaseTopic, true, false, 0);
    task.put(Collections.singletonList(record));
    String indexName = convertToIndexTemplate(type, dataset, upperCaseTopic.toLowerCase());
    verify(client, times(1)).createIndex(eq(indexName));
  }

  @Test
  public void testConvertTopicToIndexName() {
    setUpTask();

    String upperCaseTopic = "UPPERCASE";
    SinkRecord record = record(upperCaseTopic, true, false, 0);
    task.put(Collections.singletonList(record));
    verify(client, times(1)).createIndex(eq(upperCaseTopic.toLowerCase()));

    String tooLongTopic = String.format("%0256d", 1);
    record = record(tooLongTopic, true, false, 0);
    task.put(Collections.singletonList(record));
    verify(client, times(1)).createIndex(eq(tooLongTopic.substring(0, 255)));

    String startsWithDash = "-dash";
    record = record(startsWithDash, true, false, 0);
    task.put(Collections.singletonList(record));
    verify(client, times(1)).createIndex(eq("dash"));

    String startsWithUnderscore = "_underscore";
    record = record(startsWithUnderscore, true, false, 0);
    task.put(Collections.singletonList(record));
    verify(client, times(1)).createIndex(eq("underscore"));

    String dot = ".";
    record = record(dot, true, false, 0);
    task.put(Collections.singletonList(record));
    verify(client, times(1)).createIndex(eq("dot"));

    String dots = "..";
    record = record(dots, true, false, 0);
    task.put(Collections.singletonList(record));
    verify(client, times(1)).createIndex(eq("dotdot"));
  }

  @Test
  public void testShouldNotThrowIfReporterDoesNotExist() {
    when(context.errantRecordReporter())
        .thenThrow(new NoSuchMethodError("what are you doing"))
        .thenThrow(new NoClassDefFoundError("i no exist"));

    // call start twice for both exceptions
    setUpTask();
    setUpTask();
  }

  private String convertToIndexTemplate(String type, String dataset, String namespace) {
    return String.format("%s-%s-%s", type, dataset, namespace);
  }

  private SinkRecord record() {
    return record(true, false,0);
  }

  private SinkRecord record(boolean nullKey, long offset) {
    return record(nullKey, false, offset);
  }

  private SinkRecord record(boolean nullKey, boolean nullValue, long offset) {
    return record(TOPIC, nullKey, nullValue, offset);
  }

  private SinkRecord record(String topic, boolean nullKey, boolean nullValue, long offset) {
  Schema schema = SchemaBuilder.struct().name("struct")
      .field("user", Schema.STRING_SCHEMA)
      .field("message", Schema.STRING_SCHEMA)
      .build();

  Struct struct = new Struct(schema);
  struct.put("user", "Liquan");
  struct.put("message", "trying out Elastic Search.");

  return new SinkRecord(
      topic,
      1,
      Schema.STRING_SCHEMA,
      nullKey ? null : "key",
      schema,
      nullValue ? null : struct,
      offset
  );
}
}
