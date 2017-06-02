/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.elasticsearch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.searchbox.client.JestClient;
import org.junit.rules.ExpectedException;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ElasticsearchWriterTest extends ElasticsearchSinkTestBase {

  private final String key = "key";
  private final Schema schema = createSchema();
  private final Struct record = createRecord(schema);
  private final Schema otherSchema = createOtherSchema();
  private final Struct otherRecord = createOtherRecord(otherSchema);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testWriter() throws Exception {
    final boolean ignoreKey = false;
    final boolean ignoreSchema = false;

    Collection<SinkRecord> records = prepareData(2);
    ElasticsearchWriter writer = initWriter(client, ignoreKey, ignoreSchema);
    writeDataAndRefresh(writer, records);

    Collection<SinkRecord> expected = Collections.singletonList(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1));
    verifySearchResults(expected, ignoreKey, ignoreSchema);
  }

  @Test
  public void testWriterIgnoreKey() throws Exception {
    final boolean ignoreKey = true;
    final boolean ignoreSchema = false;

    Collection<SinkRecord> records = prepareData(2);
    ElasticsearchWriter writer = initWriter(client, ignoreKey, ignoreSchema);
    writeDataAndRefresh(writer, records);
    verifySearchResults(records, ignoreKey, ignoreSchema);
  }

  @Test
  public void testWriterIgnoreSchema() throws Exception {
    final boolean ignoreKey = true;
    final boolean ignoreSchema = true;

    Collection<SinkRecord> records = prepareData(2);
    ElasticsearchWriter writer = initWriter(client, ignoreKey, ignoreSchema);
    writeDataAndRefresh(writer, records);
    verifySearchResults(records, ignoreKey, ignoreSchema);
  }

  @Test
  public void testTopicIndexOverride() throws Exception {
    final boolean ignoreKey = true;
    final boolean ignoreSchema = true;

    final String indexOverride = "index";

    Collection<SinkRecord> records = prepareData(2);
    ElasticsearchWriter writer = initWriter(client, ignoreKey, Collections.<String>emptySet(), ignoreSchema, Collections.<String>emptySet(), Collections.singletonMap(TOPIC, indexOverride), false);
    writeDataAndRefresh(writer, records);
    verifySearchResults(records, indexOverride, ignoreKey, ignoreSchema);
  }

  @Test
  public void testIncompatible() throws Exception {
    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, otherSchema, otherRecord, 0);
    records.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client, true, false);

    writer.write(records);
    Thread.sleep(5000);
    records.clear();

    sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1);
    records.add(sinkRecord);
    writer.write(records);

    try {
      writer.flush();
      fail("should fail because of mapper_parsing_exception");
    } catch (ConnectException e) {
      // expected
    }
  }

  @Test
  public void testCompatible() throws Exception {
    final boolean ignoreKey = true;
    final boolean ignoreSchema = false;

    Collection<SinkRecord> records = new ArrayList<>();
    Collection<SinkRecord> expected = new ArrayList<>();
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 0);
    records.add(sinkRecord);
    expected.add(sinkRecord);
    sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1);
    records.add(sinkRecord);
    expected.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client, ignoreKey, ignoreSchema);

    writer.write(records);
    records.clear();

    sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, otherSchema, otherRecord, 2);
    records.add(sinkRecord);
    expected.add(sinkRecord);

    sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, otherSchema, otherRecord, 3);
    records.add(sinkRecord);
    expected.add(sinkRecord);

    writeDataAndRefresh(writer, records);
    verifySearchResults(expected, ignoreKey, ignoreSchema);
  }

  @Test
  public void testSafeRedeliveryRegularKey() throws Exception {
    final boolean ignoreKey = false;
    final boolean ignoreSchema = false;

    final Struct value0 = new Struct(schema);
    value0.put("user", "foo");
    value0.put("message", "hi");
    final SinkRecord sinkRecord0 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, value0, 0);

    final Struct value1 = new Struct(schema);
    value1.put("user", "foo");
    value1.put("message", "bye");
    final SinkRecord sinkRecord1 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, value1, 1);

    final ElasticsearchWriter writer = initWriter(client, ignoreKey, ignoreSchema);
    writer.write(Arrays.asList(sinkRecord0, sinkRecord1));
    writer.flush();

    // write the record with earlier offset again
    writeDataAndRefresh(writer, Collections.singleton(sinkRecord0));

    // last write should have been ignored due to version conflict
    verifySearchResults(Collections.singleton(sinkRecord1), ignoreKey, ignoreSchema);
  }

  @Test
  public void testSafeRedeliveryOffsetInKey() throws Exception {
    final boolean ignoreKey = true;
    final boolean ignoreSchema = false;

    final Struct value0 = new Struct(schema);
    value0.put("user", "foo");
    value0.put("message", "hi");
    final SinkRecord sinkRecord0 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, value0, 0);

    final Struct value1 = new Struct(schema);
    value1.put("user", "foo");
    value1.put("message", "bye");
    final SinkRecord sinkRecord1 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, value1, 1);

    final List<SinkRecord> records = Arrays.asList(sinkRecord0, sinkRecord1);

    final ElasticsearchWriter writer = initWriter(client, ignoreKey, ignoreSchema);
    writer.write(records);
    writer.flush();

    // write them again
    writeDataAndRefresh(writer, records);

    // last write should have been ignored due to version conflict
    verifySearchResults(records, ignoreKey, ignoreSchema);
  }

  @Test
  public void testMap() throws Exception {
    final boolean ignoreKey = false;
    final boolean ignoreSchema = false;

    Schema structSchema = SchemaBuilder.struct().name("struct")
        .field("map", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build())
        .build();

    Map<Integer, String> map = new HashMap<>();
    map.put(1, "One");
    map.put(2, "Two");

    Struct struct = new Struct(structSchema);
    struct.put("map", map);

    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, structSchema, struct, 0);
    records.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client, ignoreKey, ignoreSchema);
    writeDataAndRefresh(writer, records);
    verifySearchResults(records, ignoreKey, ignoreSchema);
  }

  @Test
  public void testStringKeyedMap() throws Exception {
    boolean ignoreKey = false;
    boolean ignoreSchema = false;

    Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();

    Map<String, Integer> map = new HashMap<>();
    map.put("One", 1);
    map.put("Two", 2);

    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, mapSchema, map, 0);

    ElasticsearchWriter writer = initWriter(client, ignoreKey, ignoreSchema);
    writeDataAndRefresh(writer, Collections.singletonList(sinkRecord));

    Collection<?> expectedRecords = Collections.singletonList(new ObjectMapper().writeValueAsString(map));
    verifySearchResults(expectedRecords, TOPIC, ignoreKey, ignoreSchema);
  }

  @Test
  public void testDecimal() throws Exception {
    final boolean ignoreKey = false;
    final boolean ignoreSchema = false;

    int scale = 2;
    byte[] bytes = ByteBuffer.allocate(4).putInt(2).array();
    BigDecimal decimal = new BigDecimal(new BigInteger(bytes), scale);

    Schema structSchema = SchemaBuilder.struct().name("struct")
        .field("decimal", Decimal.schema(scale))
        .build();

    Struct struct = new Struct(structSchema);
    struct.put("decimal", decimal);

    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, structSchema, struct, 0);
    records.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client, ignoreKey, ignoreSchema);
    writeDataAndRefresh(writer, records);
    verifySearchResults(records, ignoreKey, ignoreSchema);
  }

  @Test
  public void testBytes() throws Exception {
    final boolean ignoreKey = false;
    final boolean ignoreSchema = false;

    Schema structSchema = SchemaBuilder.struct().name("struct")
        .field("bytes", SchemaBuilder.BYTES_SCHEMA)
        .build();

    Struct struct = new Struct(structSchema);
    struct.put("bytes", new byte[]{42});

    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, structSchema, struct, 0);
    records.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client, ignoreKey, ignoreSchema);
    writeDataAndRefresh(writer, records);
    verifySearchResults(records, ignoreKey, ignoreSchema);
  }

  @Test
  public void testInvalidRecordException() throws Exception {
    final boolean ignoreKey = false;
    final boolean ignoreSchema = true;

    Collection<SinkRecord> records = new ArrayList<>();

    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, null, new byte[]{42}, 0);
    records.add(sinkRecord);

    final ElasticsearchWriter strictWriter = initWriter(client, ignoreKey, ignoreSchema, false);

    thrown.expect(ConnectException.class);
    thrown.expectMessage("Key is used as document id and can not be null");
    strictWriter.write(records);
  }

  @Test
  public void testDropInvalidRecord() throws Exception {
    final boolean ignoreKey = false;
    final boolean ignoreSchema = true;
    Collection<SinkRecord> inputRecords = new ArrayList<>();
    Collection<SinkRecord> outputRecords = new ArrayList<>();

    Schema structSchema = SchemaBuilder.struct().name("struct")
            .field("bytes", SchemaBuilder.BYTES_SCHEMA)
            .build();

    Struct struct = new Struct(structSchema);
    struct.put("bytes", new byte[]{42});


    SinkRecord invalidRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, structSchema, struct,  0);
    SinkRecord validRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key,  structSchema, struct, 1);

    inputRecords.add(validRecord);
    inputRecords.add(invalidRecord);

    outputRecords.add(validRecord);

    final ElasticsearchWriter nonStrictWriter = initWriter(client, ignoreKey, ignoreSchema, true);

    writeDataAndRefresh(nonStrictWriter, inputRecords);
    verifySearchResults(outputRecords, ignoreKey, ignoreSchema);
  }

  private Collection<SinkRecord> prepareData(int numRecords) {
    Collection<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < numRecords; ++i) {
      SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, i);
      records.add(sinkRecord);
    }
    return records;
  }

  private ElasticsearchWriter initWriter(JestClient client, boolean ignoreKey, boolean ignoreSchema) {
    return initWriter(client, ignoreKey, Collections.<String>emptySet(), ignoreSchema, Collections.<String>emptySet(), Collections.<String, String>emptyMap(), false);
  }

  private ElasticsearchWriter initWriter(JestClient client, boolean ignoreKey, boolean ignoreSchema, boolean dropInvalidMessage) {
    return initWriter(client, ignoreKey, Collections.<String>emptySet(), ignoreSchema, Collections.<String>emptySet(), Collections.<String, String>emptyMap(), dropInvalidMessage);
  }

  private ElasticsearchWriter initWriter(JestClient client, boolean ignoreKey, Set<String> ignoreKeyTopics, boolean ignoreSchema, Set<String> ignoreSchemaTopics, Map<String, String> topicToIndexMap, boolean dropInvalidMessage) {
    ElasticsearchWriter writer = new ElasticsearchWriter.Builder(client)
        .setType(TYPE)
        .setIgnoreKey(ignoreKey, ignoreKeyTopics)
        .setIgnoreSchema(ignoreSchema, ignoreSchemaTopics)
        .setTopicToIndexMap(topicToIndexMap)
        .setFlushTimoutMs(10000)
        .setMaxBufferedRecords(10000)
        .setMaxInFlightRequests(1)
        .setBatchSize(2)
        .setLingerMs(1000)
        .setRetryBackoffMs(1000)
        .setMaxRetry(3)
        .setDropInvalidMessage(dropInvalidMessage)
        .build();
    writer.start();
    writer.createIndicesForTopics(Collections.singleton(TOPIC));
    return writer;
  }

  private void writeDataAndRefresh(ElasticsearchWriter writer, Collection<SinkRecord> records) throws Exception {
    writer.write(records);
    writer.flush();
    writer.stop();
    refresh();
  }
}
