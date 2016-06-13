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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ElasticsearchWriterTest extends ElasticsearchSinkTestBase {

  private static final int NUM_DATA_NODES = 3;
  private final String key = "key";
  private final Schema schema = createSchema();
  private final Struct record = createRecord(schema);
  private final Schema otherSchema = createOtherSchema();
  private final Struct otherRecord = createOtherRecord(otherSchema);

  @Test
  public void testWriter() throws Exception {
    Client client = getClient();
    Collection<SinkRecord> records = prepareData(2);
    ElasticsearchWriter writer = createWriter(client, false, false, Collections.<String, TopicConfig>emptyMap(), false);
    writeDataAndWait(writer, records, SLEEP_INTERVAL_MS);

    Collection<SinkRecord> expected = Collections.singletonList(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1));
    verifySearch(expected, search(client, "user", "liquan"), false);
    verifySearch(expected, search(client, "message", "elastic"), false);
  }

  @Test
  public void testWriterIgnoreKey() throws Exception {
    Client client = getClient();
    Collection<SinkRecord> records = prepareData(2);
    ElasticsearchWriter writer = createWriter(client, true, false, Collections.<String, TopicConfig>emptyMap(),
                                              false);
    writeDataAndWait(writer, records, SLEEP_INTERVAL_MS);

    verifySearch(records, search(client, "user", "liquan"), true);
    verifySearch(records, search(client, "message", "elastic"), true);
  }

  @Test
  public void testWriterIgnoreSchema() throws Exception {
    Client client = getClient();
    Collection<SinkRecord> records = prepareData(2);
    ElasticsearchWriter writer = createWriter(client, true, true, Collections.<String, TopicConfig>emptyMap(), false);
    writeDataAndWait(writer, records, SLEEP_INTERVAL_MS);

    verifySearch(records, search(client, "user", "liquan"), true);
    verifySearch(records, search(client, "message", "elastic"), true);
  }

  @Test
  public void testTopicConfigs() throws Exception {
    Client client = getClient();
    Collection<SinkRecord> records = prepareData(2);

    TopicConfig topicConfig = new TopicConfig("index", true, false);
    Map<String, TopicConfig> topicConfigs = new HashMap<>();
    topicConfigs.put(TOPIC, topicConfig);

    ElasticsearchWriter writer = createWriter(client, false, true, topicConfigs, false);
    writeDataAndWait(writer, records, SLEEP_INTERVAL_MS);

    verifySearch(records, search(client, "user", "liquan"), true);
    verifySearch(records, search(client, "message", "elastic"), true);
  }

  @Test
  public void testIncompatible() throws Exception {
    Client client = getClient();

    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, otherSchema, otherRecord, 0);
    records.add(sinkRecord);
    sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1);
    records.add(sinkRecord);

    ElasticsearchWriter writer = createWriter(client, true, false, Collections.<String, TopicConfig>emptyMap(), false);
    writer.write(records);
    try {
      writer.flush();
      fail("Should throw MapperParsingException: String cannot be casted to integer.");
    } catch (ConnectException e) {
      // expected
    }
  }

  @Test
  public void testCompatible() throws Exception {
    Client client = getClient();

    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 0);
    records.add(sinkRecord);
    sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, otherSchema, otherRecord, 1);
    records.add(sinkRecord);

    ElasticsearchWriter writer = createWriter(client, true, false, Collections.<String, TopicConfig>emptyMap(), false);
    writeDataAndWait(writer, records, SLEEP_INTERVAL_MS);
    verifySearch(records, search(client), true);
  }

  @Test
  public void testWriterFailure() throws Exception {
    Client client = new MockClient("test", 1);
    Collection<SinkRecord> records = prepareData(2);

    ElasticsearchWriter writer = createWriter(client, true, true, Collections.<String, TopicConfig>emptyMap(), true);

    writer.write(records);
    writer.flush();
    BulkRequest request1 = writer.getCurrentBulkRequest();

    writer.write(Collections.<SinkRecord>emptyList());
    BulkRequest request2 = writer.getCurrentBulkRequest();
    assertEquals(request1, request2);

    writer.flush();
    BulkRequest request3= writer.getCurrentBulkRequest();
    assertNotEquals(request2, request3);
    writer.close();
    client.close();
  }

  @Test
  public void testMap() throws Exception {
    Client client = getClient();

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

    ElasticsearchWriter writer = createWriter(client, false, false, Collections.<String, TopicConfig>emptyMap(), false);

    writeDataAndWait(writer, records, SLEEP_INTERVAL_MS);

    verifySearch(records, search(client, "map.value", "one"), false);
  }

  @Test
  public void testDecimal() throws Exception {
    Client client = getClient();
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

    ElasticsearchWriter writer = createWriter(client, false, false, Collections.<String, TopicConfig>emptyMap(), false);

    writeDataAndWait(writer, records, SLEEP_INTERVAL_MS);

    verifySearch(records, search(client, "decimal", ((Double) decimal.doubleValue()).toString()), false);
  }

  private Client getClient() {
    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(NUM_DATA_NODES);
    return cluster.client();
  }

  private Collection<SinkRecord> prepareData(int numRecords) {
    Collection<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < numRecords; ++i) {
      SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, i);
      records.add(sinkRecord);
    }
    return records;
  }

  private ElasticsearchWriter createWriter(Client client, boolean ignoreKey, boolean ignoreSchema, Map<String, TopicConfig> topicConfigs, boolean mock) {
    return new ElasticsearchWriter.Builder(client)
        .setType(TYPE)
        .setIgnoreKey(ignoreKey)
        .setIgnoreSchema(ignoreSchema)
        .setTopicConfigs(topicConfigs)
        .setFlushTimoutMs(10000)
        .setMaxBufferedRecords(10000)
        .setBatchSize(100)
        .setContext(context)
        .setMock(mock)
        .build();
  }

  private void writeDataAndWait(ElasticsearchWriter writer, Collection<SinkRecord> records, long waitInterval) throws InterruptedException {
    writer.write(records);
    writer.flush();
    writer.close();
    Thread.sleep(waitInterval);
  }
}
