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

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.searchbox.client.JestClient;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ElasticsearchWriterTest extends ElasticsearchSinkTestBase {

  private final String key = "key";
  private final Schema schema = createSchema();
  private final Struct record = createRecord(schema);
  private final Schema otherSchema = createOtherSchema();
  private final Struct otherRecord = createOtherRecord(otherSchema);

  @Test
  public void testWriter() throws Exception {
    Collection<SinkRecord> records = prepareData(2);
    ElasticsearchWriter writer = initWriter(client, false, false, Collections.<String, TopicConfig>emptyMap());
    writeDataAndRefresh(writer, records);

    Collection<SinkRecord> expected = Collections.singletonList(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1));
    verifySearchResults(expected, false);
  }

  @Test
  public void testWriterIgnoreKey() throws Exception {
    Collection<SinkRecord> records = prepareData(2);
    ElasticsearchWriter writer = initWriter(client, true, false, Collections.<String, TopicConfig>emptyMap());
    writeDataAndRefresh(writer, records);
    verifySearchResults(records, true);
  }

  @Test
  public void testWriterIgnoreSchema() throws Exception {
    Collection<SinkRecord> records = prepareData(2);
    ElasticsearchWriter writer = initWriter(client, true, true, Collections.<String, TopicConfig>emptyMap());

    writeDataAndRefresh(writer, records);
    verifySearchResults(records, true);
  }

  @Test
  public void testTopicConfigs() throws Exception {
    Collection<SinkRecord> records = prepareData(2);

    TopicConfig topicConfig = new TopicConfig("index", true, false);
    Map<String, TopicConfig> topicConfigs = new HashMap<>();
    topicConfigs.put(TOPIC, topicConfig);

    ElasticsearchWriter writer = initWriter(client, false, true, topicConfigs);
    writeDataAndRefresh(writer, records);
    verifySearchResults(records, true);
  }

  @Test
  public void testIncompatible() throws Exception {
    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, otherSchema, otherRecord, 0);
    records.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client, true, false, Collections.<String, TopicConfig>emptyMap());

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
    Collection<SinkRecord> records = new ArrayList<>();
    Collection<SinkRecord> expected = new ArrayList<>();
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 0);
    records.add(sinkRecord);
    expected.add(sinkRecord);
    sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1);
    records.add(sinkRecord);
    expected.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client, true, false, Collections.<String, TopicConfig>emptyMap());

    writer.write(records);
    records.clear();

    sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, otherSchema, otherRecord, 2);
    records.add(sinkRecord);
    expected.add(sinkRecord);

    sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, otherSchema, otherRecord, 3);
    records.add(sinkRecord);
    expected.add(sinkRecord);

    writeDataAndRefresh(writer, records);
    verifySearchResults(expected, true);
  }

  @Test
  public void testSafeRedelivery() throws Exception {
    final boolean ignoreKey = false;

    final Struct value0 = new Struct(schema);
    value0.put("user", "foo");
    value0.put("message", "hi");
    final SinkRecord sinkRecord0 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, value0, 0);

    final Struct value1 = new Struct(schema);
    value1.put("user", "foo");
    value1.put("message", "bye");
    final SinkRecord sinkRecord1 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, value1, 1);

    final ElasticsearchWriter writer = initWriter(client, ignoreKey, false, Collections.<String, TopicConfig>emptyMap());
    writer.write(Arrays.asList(sinkRecord0, sinkRecord1));
    writer.flush();

    // write the record with earlier offset again
    writeDataAndRefresh(writer, Collections.singleton(sinkRecord0));

    // last write should have been ignored due to version conflict
    verifySearchResults(Collections.singleton(sinkRecord1), ignoreKey);
  }

  @Test
  public void testMap() throws Exception {
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

    ElasticsearchWriter writer = initWriter(client, false, false, Collections.<String, TopicConfig>emptyMap());

    writeDataAndRefresh(writer, records);

    verifySearchResults(records, false);
  }

  @Test
  public void testDecimal() throws Exception {
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

    ElasticsearchWriter writer = initWriter(client, false, false, Collections.<String, TopicConfig>emptyMap());

    writeDataAndRefresh(writer, records);

    verifySearchResults(records, false);
  }

  private Collection<SinkRecord> prepareData(int numRecords) {
    Collection<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < numRecords; ++i) {
      SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, i);
      records.add(sinkRecord);
    }
    return records;
  }

  private ElasticsearchWriter initWriter(JestClient client, boolean ignoreKey, boolean ignoreSchema, Map<String, TopicConfig> topicConfigs) {
    ElasticsearchWriter writer = new ElasticsearchWriter.Builder(client)
        .setType(TYPE)
        .setIgnoreKey(ignoreKey)
        .setIgnoreSchema(ignoreSchema)
        .setTopicConfigs(topicConfigs)
        .setFlushTimoutMs(10000)
        .setMaxBufferedRecords(10000)
        .setMaxInFlightRequests(1)
        .setBatchSize(2)
        .setLingerMs(1000)
        .setRetryBackoffMs(1000)
        .setMaxRetry(3)
        .build();
    writer.start();
    writer.createIndices(Collections.singleton(TOPIC));
    return writer;
  }

  private void writeDataAndRefresh(ElasticsearchWriter writer, Collection<SinkRecord> records) throws Exception {
    writer.write(records);
    writer.flush();
    writer.stop();
    refresh();
  }
}
