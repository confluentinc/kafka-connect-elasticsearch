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

package io.confluent.connect.elasticsearch_2_4;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import io.confluent.connect.elasticsearch_2_4.cluster.mapping.ClusterMapper;
import io.confluent.connect.elasticsearch_2_4.cluster.mapping.DefaultClusterMapper;
import io.confluent.connect.elasticsearch_2_4.index.mapping.DefaultIndexMapper;
import io.confluent.connect.elasticsearch_2_4.type.mapping.DefaultTypeMapper;
import io.confluent.connect.elasticsearch_2_4.type.mapping.TypeMapper;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
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

import static io.confluent.connect.elasticsearch_2_4.DataConverter.BehaviorOnNullValues;
import static org.hamcrest.Matchers.containsString;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ElasticsearchWriterTest extends ElasticsearchSinkTestBase {

  private final String key = "key";
  private final Schema schema = createSchema();
  private final Struct record = createRecord(schema);
  private final Schema otherSchema = createOtherSchema();
  private final Struct otherRecord = createOtherRecord(otherSchema);

  private boolean ignoreKey;
  private boolean ignoreSchema;

  @Before
  public void setUp() throws Exception {
    ignoreKey = false;
    ignoreSchema = false;

    super.setUp();
  }

  @Test
  public void testWriter() throws Exception {
    Collection<SinkRecord> records = prepareData(2);
    ElasticsearchWriter writer = initWriter(client);
    writeDataAndRefresh(writer, records);

    Collection<SinkRecord> expected = Collections.singletonList(
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1)
    );
    verifySearchResults(expected);
  }

  @Test
  public void testWriterIgnoreKey() throws Exception {
    ignoreKey = true;

    Collection<SinkRecord> records = prepareData(2);
    ElasticsearchWriter writer = initWriter(client);
    writeDataAndRefresh(writer, records);
    verifySearchResults(records);
  }

  @Test
  public void testWriterIgnoreSchema() throws Exception {
    ignoreKey = true;
    ignoreSchema = true;

    Collection<SinkRecord> records = prepareData(2);
    ElasticsearchWriter writer = initWriter(client);
    writeDataAndRefresh(writer, records);
    verifySearchResults(records);
  }

  @Test
  public void testTopicIndexOverride() throws Exception {
    ignoreKey = true;
    ignoreSchema = true;

    String indexOverride = "index";

    Collection<SinkRecord> records = prepareData(2);
    ElasticsearchWriter writer = initWriter(
        client,
        Collections.<String>emptySet(),
        Collections.<String>emptySet(),
        Collections.singletonMap(TOPIC, indexOverride),
        false,
        BehaviorOnNullValues.IGNORE);
    writeDataAndRefresh(writer, records);
    verifySearchResults(records);
  }

  @Test
  public void testIncompatible() throws Exception {
    ignoreKey = true;

    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, otherSchema, otherRecord, 0);
    records.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client);

    writer.write(records);
    Thread.sleep(5000);
    records.clear();

    sinkRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1);
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
    ignoreKey = true;

    Collection<SinkRecord> records = new ArrayList<>();
    Collection<SinkRecord> expected = new ArrayList<>();
    SinkRecord sinkRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 0);
    records.add(sinkRecord);
    expected.add(sinkRecord);
    sinkRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1);
    records.add(sinkRecord);
    expected.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client);

    writer.write(records);
    records.clear();

    sinkRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, otherSchema, otherRecord, 2);
    records.add(sinkRecord);
    expected.add(sinkRecord);

    sinkRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, otherSchema, otherRecord, 3);
    records.add(sinkRecord);
    expected.add(sinkRecord);

    writeDataAndRefresh(writer, records);
    verifySearchResults(expected);
  }

  @Test
  public void testSafeRedeliveryRegularKey() throws Exception {
    Struct value0 = new Struct(schema);
    value0.put("user", "foo");
    value0.put("message", "hi");
    SinkRecord sinkRecord0 =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, value0, 0);

    Struct value1 = new Struct(schema);
    value1.put("user", "foo");
    value1.put("message", "bye");
    SinkRecord sinkRecord1 =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, value1, 1);

    ElasticsearchWriter writer = initWriter(client);
    writer.write(Arrays.asList(sinkRecord0, sinkRecord1));
    writer.flush();

    // write the record with earlier offset again
    writeDataAndRefresh(writer, Collections.singleton(sinkRecord0));

    // last write should have been ignored due to version conflict
    verifySearchResults(Collections.singleton(sinkRecord1));
  }

  @Test
  public void testSafeRedeliveryOffsetInKey() throws Exception {
    ignoreKey = true;

    Struct value0 = new Struct(schema);
    value0.put("user", "foo");
    value0.put("message", "hi");
    SinkRecord sinkRecord0 =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, value0, 0);

    Struct value1 = new Struct(schema);
    value1.put("user", "foo");
    value1.put("message", "bye");
    SinkRecord sinkRecord1 =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, value1, 1);

    List<SinkRecord> records = Arrays.asList(sinkRecord0, sinkRecord1);

    ElasticsearchWriter writer = initWriter(client);
    writer.write(records);
    writer.flush();

    // write them again
    writeDataAndRefresh(writer, records);

    // last write should have been ignored due to version conflict
    verifySearchResults(records);
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
    SinkRecord sinkRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, structSchema, struct, 0);
    records.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client);
    writeDataAndRefresh(writer, records);
    verifySearchResults(records);
  }

  @Test
  public void testStringKeyedMap() throws Exception {
    Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();

    Map<String, Integer> map = new HashMap<>();
    map.put("One", 1);
    map.put("Two", 2);

    SinkRecord sinkRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, mapSchema, map, 0);

    ElasticsearchWriter writer = initWriter(client);
    writeDataAndRefresh(writer, Collections.singletonList(sinkRecord));

    verifySearchResults(Collections.singleton(sinkRecord));
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
    SinkRecord sinkRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, structSchema, struct, 0);
    records.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client);
    writeDataAndRefresh(writer, records);
    verifySearchResults(records);
  }

  @Test
  public void testBytes() throws Exception {
    Schema structSchema = SchemaBuilder.struct().name("struct")
        .field("bytes", SchemaBuilder.BYTES_SCHEMA)
        .build();

    Struct struct = new Struct(structSchema);
    struct.put("bytes", new byte[]{42});

    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, structSchema, struct, 0);
    records.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client);
    writeDataAndRefresh(writer, records);
    verifySearchResults(records);
  }

  @Test
  public void testIgnoreNullValue() throws Exception {
    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, null, 0);
    records.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client, BehaviorOnNullValues.IGNORE);
    writeDataAndRefresh(writer, records);
    // Send an empty list of records to the verify method, since the empty record should have been
    // skipped
    verifySearchResults(new ArrayList<SinkRecord>());
  }

  @Test
  public void testDeleteOnNullValue() throws Exception {
    String key1 = "key1";
    String key2 = "key2";

    ElasticsearchWriter writer = initWriter(client, BehaviorOnNullValues.DELETE);

    Collection<SinkRecord> records = new ArrayList<>();

    // First, write a couple of actual (non-null-valued) records
    SinkRecord insertRecord1 =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key1, schema, record, 0);
    records.add(insertRecord1);
    SinkRecord insertRecord2 =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key2, otherSchema, otherRecord, 1);
    records.add(insertRecord2);
    // Can't call writeDataAndRefresh(writer, records) since it stops the writer
    writer.write(records);
    writer.flush();
    refresh();
    // Make sure the record made it there successfully
    verifySearchResults(records);

    // Then, write a record with the same key as the first inserted record but a null value
    SinkRecord deleteRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key1, schema, null, 2);

    // Don't want to resend the first couple of records
    records.clear();
    records.add(deleteRecord);
    writeDataAndRefresh(writer, records);

    // The only remaining record should be the second inserted record
    records.clear();
    records.add(insertRecord2);
    verifySearchResults(records);
  }

  @Test
  public void testIneffectiveDelete() throws Exception {
    // Just a sanity check to make sure things don't blow up if an attempt is made to delete a
    // record that doesn't exist in the first place

    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, null, 0);
    records.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client, BehaviorOnNullValues.DELETE);
    writeDataAndRefresh(writer, records);
    verifySearchResults(new ArrayList<SinkRecord>());
  }

  @Test
  public void testDeleteWithNullKey() throws Exception {
    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, null, 0);
    records.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client, BehaviorOnNullValues.DELETE);
    writeDataAndRefresh(writer, records);
    verifySearchResults(new ArrayList<SinkRecord>());
  }

  @Test
  public void testFailOnNullValue() throws Exception {
    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, null, 0);
    records.add(sinkRecord);

    ElasticsearchWriter writer = initWriter(client, BehaviorOnNullValues.FAIL);
    try {
      writeDataAndRefresh(writer, records);
      fail("should fail because of behavior.on.null.values=fail");
    } catch (DataException e) {
      // expected
    }
  }

  @Test
  public void testInvalidRecordException() throws Exception {
    ignoreSchema = true;

    Collection<SinkRecord> records = new ArrayList<>();

    SinkRecord sinkRecord =
        new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, null, new byte[]{42}, 0);
    records.add(sinkRecord);

    final ElasticsearchWriter strictWriter = initWriter(client);

    Exception e = assertThrows(ConnectException.class, () -> {
      strictWriter.write(records);
    });
    assertEquals("Key is used as document id and can not be null.", e.getMessage());
  }

  @Test
  public void testDropInvalidRecord() throws Exception {
    ignoreSchema = true;
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

    final ElasticsearchWriter nonStrictWriter = initWriter(client, true);

    writeDataAndRefresh(nonStrictWriter, inputRecords);
    verifySearchResults(outputRecords, ignoreKey, ignoreSchema);
  }

  @Test
  public void testDropInvalidRecordThrowsOnOtherErrors() throws Exception {
    ignoreSchema = true;
    Collection<SinkRecord> inputRecords = new ArrayList<>();

    Schema structSchema = SchemaBuilder.struct().name("struct")
            .field("bytes", SchemaBuilder.BYTES_SCHEMA)
            .build();

    Struct struct = new Struct(structSchema);
    struct.put("bytes", new byte[]{42});

    SinkRecord validRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key,  structSchema, struct, 1);

    inputRecords.add(validRecord);

    final ElasticsearchWriter nonStrictWriter = initWriter(client, true);

    nonStrictWriter.write(inputRecords);

    // stop the bulk processor
    nonStrictWriter.stop();

    // try to write on a stopped writer, should throw
    ConnectException e = assertThrows(ConnectException.class,
            () -> nonStrictWriter.write(inputRecords));
    MatcherAssert.assertThat(e.getMessage(), containsString("Stopping"));
  }

  private Collection<SinkRecord> prepareData(int numRecords) {
    Collection<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < numRecords; ++i) {
      SinkRecord sinkRecord =
          new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, i);
      records.add(sinkRecord);
    }
    return records;
  }

  private ElasticsearchWriter initWriter(ElasticsearchClient client) {
    return initWriter(client, false, BehaviorOnNullValues.IGNORE);
  }

  private ElasticsearchWriter initWriter(ElasticsearchClient client, boolean dropInvalidMessage) {
    return initWriter(client, dropInvalidMessage, BehaviorOnNullValues.IGNORE);
  }

  private ElasticsearchWriter initWriter(ElasticsearchClient client, BehaviorOnNullValues behavior) {
    return initWriter(client, false, behavior);
  }

  private ElasticsearchWriter initWriter(
      ElasticsearchClient client,
      boolean dropInvalidMessage,
      BehaviorOnNullValues behavior) {
    return initWriter(
        client,
        Collections.<String>emptySet(),
        Collections.<String>emptySet(),
        Collections.<String, String>emptyMap(),
        dropInvalidMessage,
        behavior
    );
  }

  private ElasticsearchWriter initWriter(
      ElasticsearchClient client,
      Set<String> ignoreKeyTopics,
      Set<String> ignoreSchemaTopics,
      Map<String, String> topicToIndexMap,
      boolean dropInvalidMessage,
      BehaviorOnNullValues behavior
  ) {

    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    ClusterMapper mapper = new DefaultClusterMapper();
    mapper.configure(config);

    TypeMapper typeMapper = new DefaultTypeMapper();
    typeMapper.configure(config);

    return new ElasticsearchWriter.Builder(props)
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
        .setBehaviorOnNullValues(behavior)
        .setIndexMapper(new DefaultIndexMapper())
        .setClusterMapper(mapper)
        .setTypeMapper(typeMapper)
        .build();
  }

  private void writeDataAndRefresh(ElasticsearchWriter writer, Collection<SinkRecord> records)
      throws Exception {
    writer.write(records);
    writer.flush();
    writer.stop();
    refresh();
  }

  private void verifySearchResults(Collection<SinkRecord> records) throws Exception {
    verifySearchResults(records, ignoreKey, ignoreSchema);
  }
}
