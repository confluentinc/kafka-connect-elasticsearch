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
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DROP_INVALID_MESSAGE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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

    // don't skip null
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

  private SinkRecord record() {
    return record(true, false,0);
  }

  private SinkRecord record(boolean nullKey, long offset) {
    return record(nullKey, false, offset);
  }

  private SinkRecord record(boolean nullKey, boolean nullValue, long offset) {
    Schema schema = SchemaBuilder.struct().name("struct")
        .field("user", Schema.STRING_SCHEMA)
        .field("message", Schema.STRING_SCHEMA)
        .build();

    Struct struct = new Struct(schema);
    struct.put("user", "Liquan");
    struct.put("message", "trying out Elastic Search.");

    return new SinkRecord(
        TOPIC,
        1,
        Schema.STRING_SCHEMA,
        nullKey ? null : "key",
        schema,
        nullValue ? null : struct,
        offset
    );
  }
}
