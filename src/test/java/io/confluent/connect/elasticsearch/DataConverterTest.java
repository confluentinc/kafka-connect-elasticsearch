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

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnNullValues;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.VersionType;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static io.confluent.connect.elasticsearch.DataConverter.MAP_KEY;
import static io.confluent.connect.elasticsearch.DataConverter.MAP_VALUE;
import static io.confluent.connect.elasticsearch.DataConverter.TIMESTAMP_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class DataConverterTest {
  
  private DataConverter converter;
  private Map<String, String> props;

  private String key;
  private String topic;
  private int partition;
  private long offset;
  private long recordTimestamp;
  private String index;
  private Schema schema;

  @Before
  public void setUp() {
    props = ElasticsearchSinkConnectorConfigTest.addNecessaryProps(new HashMap<>());
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG, "true");
    props.put(ElasticsearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, "true");

    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));
    key = "key";
    topic = "topic";
    partition = 0;
    offset = 0;
    recordTimestamp = System.currentTimeMillis();
    index = "index";
    schema = SchemaBuilder
        .struct()
        .name("struct")
        .field("string", Schema.STRING_SCHEMA)
        .build();
  }

  @Test
  public void primitives() {
    assertIdenticalAfterPreProcess(Schema.INT8_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.INT16_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.INT32_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.INT64_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.FLOAT32_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.FLOAT64_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.BOOLEAN_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.STRING_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.BYTES_SCHEMA);

    assertIdenticalAfterPreProcess(Schema.OPTIONAL_INT16_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.OPTIONAL_INT32_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.OPTIONAL_INT64_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.OPTIONAL_FLOAT32_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.OPTIONAL_FLOAT64_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.OPTIONAL_STRING_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.OPTIONAL_BYTES_SCHEMA);

    assertIdenticalAfterPreProcess(SchemaBuilder.int8().defaultValue((byte) 42).build());
    assertIdenticalAfterPreProcess(SchemaBuilder.int16().defaultValue((short) 42).build());
    assertIdenticalAfterPreProcess(SchemaBuilder.int32().defaultValue(42).build());
    assertIdenticalAfterPreProcess(SchemaBuilder.int64().defaultValue(42L).build());
    assertIdenticalAfterPreProcess(SchemaBuilder.float32().defaultValue(42.0f).build());
    assertIdenticalAfterPreProcess(SchemaBuilder.float64().defaultValue(42.0d).build());
    assertIdenticalAfterPreProcess(SchemaBuilder.bool().defaultValue(true).build());
    assertIdenticalAfterPreProcess(SchemaBuilder.string().defaultValue("foo").build());
    assertIdenticalAfterPreProcess(SchemaBuilder.bytes().defaultValue(new byte[0]).build());
  }

  private void assertIdenticalAfterPreProcess(Schema schema) {
    assertEquals(schema, converter.preProcessSchema(schema));
  }

  @Test
  public void decimal() {
    Schema origSchema = Decimal.schema(2);
    Schema preProcessedSchema = converter.preProcessSchema(origSchema);
    assertEquals(Schema.FLOAT64_SCHEMA, preProcessedSchema);

    assertEquals(0.02, converter.preProcessValue(new BigDecimal("0.02"), origSchema, preProcessedSchema));

    // optional
    assertEquals(
        Schema.OPTIONAL_FLOAT64_SCHEMA,
        converter.preProcessSchema(Decimal.builder(2).optional().build())
    );

    // defval
    assertEquals(
        SchemaBuilder.float64().defaultValue(0.00).build(),
        converter.preProcessSchema(Decimal.builder(2).defaultValue(new BigDecimal("0.00")).build())
    );
  }

  @Test
  public void array() {
    Schema origSchema = SchemaBuilder.array(Decimal.schema(2)).schema();
    Schema preProcessedSchema = converter.preProcessSchema(origSchema);
    assertEquals(SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build(), preProcessedSchema);

    assertEquals(
        Arrays.asList(0.02, 0.42),
        converter.preProcessValue(Arrays.asList(new BigDecimal("0.02"), new BigDecimal("0.42")), origSchema, preProcessedSchema)
    );

    // optional
    assertEquals(
        SchemaBuilder.array(preProcessedSchema.valueSchema()).optional().build(),
        converter.preProcessSchema(SchemaBuilder.array(Decimal.schema(2)).optional().build())
    );

    // defval
    assertEquals(
        SchemaBuilder.array(preProcessedSchema.valueSchema()).defaultValue(Collections.emptyList()).build(),
        converter.preProcessSchema(SchemaBuilder.array(Decimal.schema(2)).defaultValue(Collections.emptyList()).build())
    );
  }

  @Test
  public void map() {
    Schema origSchema = SchemaBuilder.map(Schema.INT32_SCHEMA, Decimal.schema(2)).build();
    Schema preProcessedSchema = converter.preProcessSchema(origSchema);
    assertEquals(
        SchemaBuilder.array(
            SchemaBuilder.struct().name(Schema.INT32_SCHEMA.type().name() + "-" + Decimal.LOGICAL_NAME)
                .field(MAP_KEY, Schema.INT32_SCHEMA)
                .field(MAP_VALUE, Schema.FLOAT64_SCHEMA)
                .build()
        ).build(),
        preProcessedSchema
    );

    Map<Object, Object> origValue = new HashMap<>();
    origValue.put(1, new BigDecimal("0.02"));
    origValue.put(2, new BigDecimal("0.42"));
    assertEquals(
        new HashSet<>(Arrays.asList(
            new Struct(preProcessedSchema.valueSchema())
                .put(MAP_KEY, 1)
                .put(MAP_VALUE, 0.02),
            new Struct(preProcessedSchema.valueSchema())
                .put(MAP_KEY, 2)
                .put(MAP_VALUE, 0.42)
        )),
        new HashSet<>((List<?>) converter.preProcessValue(origValue, origSchema, preProcessedSchema))
    );

    // optional
    assertEquals(
        SchemaBuilder.array(preProcessedSchema.valueSchema()).optional().build(),
        converter.preProcessSchema(SchemaBuilder.map(Schema.INT32_SCHEMA, Decimal.schema(2)).optional().build())
    );

    // defval
    assertEquals(
        SchemaBuilder.array(preProcessedSchema.valueSchema()).defaultValue(Collections.emptyList()).build(),
        converter.preProcessSchema(SchemaBuilder.map(Schema.INT32_SCHEMA, Decimal.schema(2)).defaultValue(Collections.emptyMap()).build())
    );
  }

  @Test
  public void stringKeyedMapNonCompactFormat() {
    Schema origSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();

    Map<Object, Object> origValue = new HashMap<>();
    origValue.put("field1", 1);
    origValue.put("field2", 2);

    // Use the older non-compact format for map entries with string keys
    props.put(ElasticsearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, "false");
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));

    Schema preProcessedSchema = converter.preProcessSchema(origSchema);
    assertEquals(
        SchemaBuilder.array(
            SchemaBuilder.struct().name(Schema.STRING_SCHEMA.type().name() + "-" + Schema.INT32_SCHEMA.type().name())
                         .field(MAP_KEY, Schema.STRING_SCHEMA)
                         .field(MAP_VALUE, Schema.INT32_SCHEMA)
                         .build()
        ).build(),
        preProcessedSchema
    );
    assertEquals(
        new HashSet<>(Arrays.asList(
                new Struct(preProcessedSchema.valueSchema())
                        .put(MAP_KEY, "field1")
                        .put(MAP_VALUE, 1),
                new Struct(preProcessedSchema.valueSchema())
                        .put(MAP_KEY, "field2")
                        .put(MAP_VALUE, 2)
        )),
        new HashSet<>((List<?>) converter.preProcessValue(origValue, origSchema, preProcessedSchema))
    );
  }

  @Test
  public void stringKeyedMapCompactFormat() {
    Schema origSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();

    Map<Object, Object> origValue = new HashMap<>();
    origValue.put("field1", 1);
    origValue.put("field2", 2);

    // Use the newer compact format for map entries with string keys
    props.put(ElasticsearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, "true");
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));
    Schema preProcessedSchema = converter.preProcessSchema(origSchema);
    assertEquals(
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(),
        preProcessedSchema
    );
    HashMap<?, ?> newValue = (HashMap<?, ?>) converter.preProcessValue(origValue, origSchema, preProcessedSchema);
    assertEquals(origValue, newValue);
  }

  @Test
  public void struct() {
    Schema origSchema = SchemaBuilder.struct().name("struct").field("decimal", Decimal.schema(2)).build();
    Schema preProcessedSchema = converter.preProcessSchema(origSchema);
    assertEquals(
        SchemaBuilder.struct().name("struct").field("decimal", Schema.FLOAT64_SCHEMA).build(),
        preProcessedSchema
    );

    assertEquals(
        new Struct(preProcessedSchema).put("decimal", 0.02),
        converter.preProcessValue(new Struct(origSchema).put("decimal", new BigDecimal("0.02")), origSchema, preProcessedSchema)
    );

    // optional
    assertEquals(
        SchemaBuilder.struct().name("struct").field("decimal", Schema.FLOAT64_SCHEMA).optional().build(),
        converter.preProcessSchema(SchemaBuilder.struct().name("struct").field("decimal", Decimal.schema(2)).optional().build())
    );
  }

  @Test
  public void optionalFieldsWithoutDefaults() {
    // One primitive type should be enough
    testOptionalFieldWithoutDefault(SchemaBuilder.bool());
    // Logical types
    testOptionalFieldWithoutDefault(Decimal.builder(2));
    testOptionalFieldWithoutDefault(Time.builder());
    testOptionalFieldWithoutDefault(Timestamp.builder());
    // Complex types
    testOptionalFieldWithoutDefault(SchemaBuilder.array(Schema.BOOLEAN_SCHEMA));
    testOptionalFieldWithoutDefault(SchemaBuilder.struct().field("innerField", Schema.BOOLEAN_SCHEMA));
    testOptionalFieldWithoutDefault(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA));
    // Have to test maps with useCompactMapEntries set to true and set to false
    props.put(ElasticsearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, "false");
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));
    testOptionalFieldWithoutDefault(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA));
  }

  private void testOptionalFieldWithoutDefault(
    SchemaBuilder optionalFieldSchema
  ) {
    Schema origSchema = SchemaBuilder.struct().name("struct").field(
        "optionalField", optionalFieldSchema.optional().build()
    ).build();
    Schema preProcessedSchema = converter.preProcessSchema(origSchema);

    Object preProcessedValue = converter.preProcessValue(
        new Struct(origSchema).put("optionalField", null), origSchema, preProcessedSchema
    );

    assertEquals(new Struct(preProcessedSchema).put("optionalField", null), preProcessedValue);
  }

  @Test
  public void ignoreOnNullValue() {

    props.put(ElasticsearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, "true");
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.IGNORE.name());
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));

    SinkRecord sinkRecord = createSinkRecordWithValue(null);
    assertNull(converter.convertRecord(sinkRecord, index));
  }

  @Test
  public void throwExceptionOnEmptyOrNullKey() {

    props.put(ElasticsearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, "true");
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.IGNORE.name());
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));

    Schema preProcessedSchema = converter.preProcessSchema(schema);
    Struct struct = new Struct(preProcessedSchema).put("string", "myValue");

    key = "";
    SinkRecord sinkRecord1 = createSinkRecordWithValue(struct);
    assertThrows(DataException.class, () -> converter.convertRecord(sinkRecord1, index));

    key = null;
    SinkRecord sinkRecord2 = createSinkRecordWithValue(struct);
    assertThrows(DataException.class, () -> converter.convertRecord(sinkRecord2, index));
  }

  @Test
  public void deleteOnNullValue() {
    props.put(ElasticsearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, "true");
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.DELETE.name());
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));
    SinkRecord sinkRecord = createSinkRecordWithValue(null);
    DeleteRequest actualRecord = (DeleteRequest) converter.convertRecord(sinkRecord, index);

    assertEquals(key, actualRecord.id());
    assertEquals(index, actualRecord.index());
    assertEquals(sinkRecord.kafkaOffset(), actualRecord.version());
  }

  @Test
  public void externalVersionHeaderOnDelete() {
    String externalVersionHeader = "version";
    long expectedExternalVersion = 123l;

    props.put(ElasticsearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, "true");
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.EXTERNAL_VERSION_HEADER_CONFIG, externalVersionHeader);
    props.put(ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.DELETE.name());
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));

    SinkRecord sinkRecord = createSinkRecordWithValue(null);
    sinkRecord.headers().addLong(externalVersionHeader, expectedExternalVersion);

    DeleteRequest actualRecord = (DeleteRequest) converter.convertRecord(sinkRecord, index);

    assertEquals(key, actualRecord.id());
    assertEquals(index, actualRecord.index());
    assertEquals(expectedExternalVersion, actualRecord.version());
  }

  @Test
  public void externalVersionHeaderOnIndex() {
    String externalVersionHeader = "version";
    long expectedExternalVersion = 123l;

    props.put(ElasticsearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, "true");
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.EXTERNAL_VERSION_HEADER_CONFIG, externalVersionHeader);
    props.put(ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.DELETE.name());
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));


    Schema preProcessedSchema = converter.preProcessSchema(schema);
    Struct struct = new Struct(preProcessedSchema).put("string", "myValue");
    SinkRecord sinkRecord = createSinkRecordWithValue(struct);
    sinkRecord.headers().addLong(externalVersionHeader, expectedExternalVersion);

    IndexRequest actualRecord = (IndexRequest) converter.convertRecord(sinkRecord, index);

    assertEquals(key, actualRecord.id());
    assertEquals(index, actualRecord.index());
    assertEquals(expectedExternalVersion, actualRecord.version());
  }

  @Test
  public void ignoreDeleteOnNullValueWithNullKey() {
    props.put(ElasticsearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, "true");
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.DELETE.name());
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));

    key = null;

    SinkRecord sinkRecord = createSinkRecordWithValue(null);
    assertNull(converter.convertRecord(sinkRecord, index));
  }

  @Test
  public void failOnNullValue() {
    props.put(ElasticsearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, "true");
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.FAIL.name());
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));

    SinkRecord sinkRecord = createSinkRecordWithValue(null);
    try {
      converter.convertRecord(sinkRecord, index);
      fail("should fail on null-valued record with behaviorOnNullValues = FAIL");
    } catch (DataException e) {
      // expected
    }
  }

  @Test
  public void useAutogeneratedIds() {
    props.put(ElasticsearchSinkConnectorConfig.USE_AUTO_GENERATED_IDS_CONFIG, "true");
    props.put(ElasticsearchSinkConnectorConfig.WRITE_METHOD_CONFIG, "INSERT");

    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));

    SinkRecord sinkRecord = createSinkRecordWithValue(new Struct(schema).put("string","test"));
    DocWriteRequest<?> req = converter.convertRecord(sinkRecord, index);
    assertNull(req.id());
  }

  @Test
  public void ignoreAutogeneratedIdsForUpsert() {
    props.put(ElasticsearchSinkConnectorConfig.USE_AUTO_GENERATED_IDS_CONFIG, "true");
    props.put(ElasticsearchSinkConnectorConfig.WRITE_METHOD_CONFIG, "UPSERT");

    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));

    SinkRecord sinkRecord = createSinkRecordWithValue(new Struct(schema).put("string","test"));
    DocWriteRequest<?> req = converter.convertRecord(sinkRecord, index);
    assertNotNull(req.id());
  }

  public SinkRecord createSinkRecordWithValue(Object value) {
    return new SinkRecord(
         topic, 
         partition, 
         Schema.STRING_SCHEMA, 
         key, schema, 
         value, 
         offset,
         recordTimestamp,
         TimestampType.CREATE_TIME
    );
  }

  @Test
  public void testDoNotInjectPayloadTimestampIfNotDataStream() {
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));
    Schema preProcessedSchema = converter.preProcessSchema(schema);
    Struct struct = new Struct(preProcessedSchema).put("string", "myValue");
    SinkRecord sinkRecord = createSinkRecordWithValue(struct);

    IndexRequest actualRecord = (IndexRequest) converter.convertRecord(sinkRecord, index);

    assertFalse(actualRecord.sourceAsMap().containsKey(TIMESTAMP_FIELD));
  }

  @Test
  public void testDoNotInjectMissingPayloadTimestampIfDataStreamAndTimestampMapNotFound() {
    configureDataStream();
    props.put(ElasticsearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_CONFIG, "timestampFieldNotPresent");
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));
    Schema preProcessedSchema = converter.preProcessSchema(schema);
    Struct struct = new Struct(preProcessedSchema).put("string", "myValue");
    SinkRecord sinkRecord = createSinkRecordWithValue(struct);

    IndexRequest actualRecord = (IndexRequest) converter.convertRecord(sinkRecord, index);
    assertFalse(actualRecord.sourceAsMap().containsKey(TIMESTAMP_FIELD));
  }

  @Test
  public void testInjectPayloadTimestampIfDataStreamAndNoTimestampMapSet() {
    configureDataStream();
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));
    Schema preProcessedSchema = converter.preProcessSchema(schema);
    Struct struct = new Struct(preProcessedSchema).put("string", "myValue");
    SinkRecord sinkRecord = createSinkRecordWithValue(struct);

    IndexRequest actualRecord = (IndexRequest) converter.convertRecord(sinkRecord, index);

    assertEquals(recordTimestamp, actualRecord.sourceAsMap().get(TIMESTAMP_FIELD));
  }

  @Test
  public void testInjectPayloadTimestampEvenIfAlreadyExistsAndTimestampMapNotSet() {
    configureDataStream();
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));
    schema = SchemaBuilder
        .struct()
        .name("struct")
        .field(TIMESTAMP_FIELD, Schema.STRING_SCHEMA)
        .build();
    Schema preProcessedSchema = converter.preProcessSchema(schema);
    String timestamp = "2021-05-14T11:11:22.000Z";
    Struct struct = new Struct(preProcessedSchema).put(TIMESTAMP_FIELD, timestamp);
    SinkRecord sinkRecord = createSinkRecordWithValue(struct);

    IndexRequest actualRecord = (IndexRequest) converter.convertRecord(sinkRecord, index);

    assertEquals(recordTimestamp, actualRecord.sourceAsMap().get(TIMESTAMP_FIELD));
  }

  @Test
  public void testMapPayloadTimestampIfDataStreamSetAndOneTimestampMapSet() {
    String timestampFieldMap = "onefield";
    configureDataStream();
    props.put(ElasticsearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_CONFIG, timestampFieldMap);
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));
    schema = SchemaBuilder
        .struct()
        .name("struct")
        .field(timestampFieldMap, Schema.STRING_SCHEMA)
        .build();
    Schema preProcessedSchema = converter.preProcessSchema(schema);
    String timestamp = "2021-05-14T11:11:22.000Z";
    Struct struct = new Struct(preProcessedSchema).put(timestampFieldMap, timestamp);
    SinkRecord sinkRecord = createSinkRecordWithValue(struct);

    IndexRequest actualRecord = (IndexRequest) converter.convertRecord(sinkRecord, index);

    assertEquals(timestamp, actualRecord.sourceAsMap().get(TIMESTAMP_FIELD));
  }

  @Test
  public void testMapPayloadTimestampByPriorityIfMultipleTimestampMapsSet() {
    String timestampFieldToUse = "two";
    configureDataStream();
    props.put(ElasticsearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_CONFIG, "one, two, field");
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));
    schema = SchemaBuilder
        .struct()
        .name("struct")
        .field(timestampFieldToUse, Schema.STRING_SCHEMA)
        .field("field", Schema.STRING_SCHEMA)
        .build();
    Schema preProcessedSchema = converter.preProcessSchema(schema);
    String timestamp = "2021-05-14T11:11:22.000Z";
    Struct struct = new Struct(preProcessedSchema).put(timestampFieldToUse, timestamp).put("field", "other");
    SinkRecord sinkRecord = createSinkRecordWithValue(struct);

    IndexRequest actualRecord = (IndexRequest) converter.convertRecord(sinkRecord, index);

    assertEquals(timestamp, actualRecord.sourceAsMap().get(TIMESTAMP_FIELD));
  }

  @Test(expected = DataException.class)
  public void testExceptionWhenNonObjectPayloadInDataStream() {
    configureDataStream();
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));
    SinkRecord record = new SinkRecord("t", 0, Schema.STRING_SCHEMA, key,
        SchemaBuilder.array(Schema.STRING_SCHEMA).build(), Arrays.asList("a", "b"), offset,
        recordTimestamp, TimestampType.CREATE_TIME);
    converter.convertRecord(record, index);
  }

  @Test
  public void testDoNotAddExternalVersioningIfDataStream() {
    configureDataStream();
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG, "false");
    converter = new DataConverter(new ElasticsearchSinkConnectorConfig(props));
    Schema preProcessedSchema = converter.preProcessSchema(schema);
    Struct struct = new Struct(preProcessedSchema).put("string", "myValue");
    SinkRecord sinkRecord = createSinkRecordWithValue(struct);

    IndexRequest actualRecord = (IndexRequest) converter.convertRecord(sinkRecord, index);

    assertEquals(VersionType.INTERNAL, actualRecord.versionType());
  }

  private void configureDataStream() {
    props.put(ElasticsearchSinkConnectorConfig.DATA_STREAM_TYPE_CONFIG, "logs");
    props.put(ElasticsearchSinkConnectorConfig.DATA_STREAM_DATASET_CONFIG, "dataset");
  }
}
