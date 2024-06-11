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

import com.github.tomakehurst.wiremock.common.Json;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Test;

import static io.confluent.connect.elasticsearch.Mapping.KEYWORD_TYPE;
import static io.confluent.connect.elasticsearch.Mapping.KEY_FIELD;
import static io.confluent.connect.elasticsearch.Mapping.TEXT_TYPE;
import static io.confluent.connect.elasticsearch.Mapping.VALUE_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@SuppressWarnings("deprecation")
public class MappingTest {

  @Test(expected = DataException.class)
  public void testBuildMappingWithNullSchema() {
    XContentBuilder builder = Mapping.buildMapping(null);
  }

  @Test
  public void testBuildMapping() throws IOException {
    JsonObject result = runTest(createSchema());
    verifyMapping(createSchema(), result);
  }

  @Test
  public void testBuildMappingForString() throws IOException {
    Schema schema = SchemaBuilder.struct()
        .name("record")
        .field("string", Schema.STRING_SCHEMA)
        .build();

    JsonObject result = runTest(schema);
    JsonObject string = result.getAsJsonObject("properties").getAsJsonObject("string");
    JsonObject keyword = string.getAsJsonObject("fields").getAsJsonObject("keyword");

    assertEquals(TEXT_TYPE, string.get("type").getAsString());
    assertEquals(KEYWORD_TYPE, keyword.get("type").getAsString());
    assertEquals(256, keyword.get("ignore_above").getAsInt());
  }

  @Test
  public void testBuildMappingSetsDefaultValue() throws IOException {
    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("boolean", SchemaBuilder.bool().defaultValue(true).build())
        .field("int8", SchemaBuilder.int8().defaultValue((byte) 1).build())
        .field("int16", SchemaBuilder.int16().defaultValue((short) 1).build())
        .field("int32", SchemaBuilder.int32().defaultValue(1).build())
        .field("int64", SchemaBuilder.int64().defaultValue((long) 1).build())
        .field("float32", SchemaBuilder.float32().defaultValue((float) 1).build())
        .field("float64", SchemaBuilder.float64().defaultValue((double) 1).build())
        .build();

    JsonObject properties = runTest(schema).getAsJsonObject("properties");
    assertEquals(1, properties.getAsJsonObject("int8").get("null_value").getAsInt());
    assertEquals(1, properties.getAsJsonObject("int16").get("null_value").getAsInt());
    assertEquals(1, properties.getAsJsonObject("int32").get("null_value").getAsInt());
    assertEquals(1, properties.getAsJsonObject("int64").get("null_value").getAsInt());
    assertEquals(1, properties.getAsJsonObject("float32").get("null_value").getAsInt());
    assertEquals(1, properties.getAsJsonObject("float64").get("null_value").getAsInt());
    assertEquals(true, properties.getAsJsonObject("boolean").get("null_value").getAsBoolean());
  }

  @Test
  public void testBuildMappingSetsDefaultValueForDate() throws IOException {
    java.util.Date expected = new java.util.Date();
    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("date", Date.builder().defaultValue(expected).build())
        .build();

    JsonObject result = runTest(schema);

    assertEquals(
        expected.getTime(),
        result.getAsJsonObject("properties").getAsJsonObject("date").get("null_value").getAsLong()
    );
  }

  @Test
  public void testBuildMappingSetsNoDefaultValueForStrings() throws IOException {
    Schema schema = SchemaBuilder
        .struct()
        .name("record")
        .field("string", SchemaBuilder.string().defaultValue("0").build())
        .build();

    JsonObject result = runTest(schema);

    assertNull(result.getAsJsonObject("properties").getAsJsonObject("string").get("null_value"));
  }

  @Test
  public void testDecimalTypeMapping() throws Exception {
    Schema schema = new ConnectSchema(Type.INT64, true, BigDecimal.valueOf(100),
        Decimal.LOGICAL_NAME, 1, "");
    JsonObject result = runTest(schema);
    assertEquals("double", result.get("type").getAsString());
    assertEquals(100, result.get("null_value").getAsInt());
  }

  private Schema createSchema() {
    return createSchemaBuilder("record")
        .field("struct", createSchemaBuilder("inner").build())
        .build();
  }

  private SchemaBuilder createSchemaBuilder(String name) {
    return SchemaBuilder.struct().name(name)
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("bytes", Schema.BYTES_SCHEMA)
        .field("int8", Schema.INT8_SCHEMA)
        .field("int16", Schema.INT16_SCHEMA)
        .field("int32", Schema.INT32_SCHEMA)
        .field("int64", Schema.INT64_SCHEMA)
        .field("float32", Schema.FLOAT32_SCHEMA)
        .field("float64", Schema.FLOAT64_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
        .field("decimal", Decimal.schema(2))
        .field("date", Date.SCHEMA)
        .field("time", Time.SCHEMA)
        .field("timestamp", Timestamp.SCHEMA);
  }

  private static JsonObject runTest(Schema schema) throws IOException {
    XContentBuilder builder = Mapping.buildMapping(schema);
    builder.flush();
    ByteArrayOutputStream stream = (ByteArrayOutputStream) builder.getOutputStream();
    return  (JsonObject) JsonParser.parseString(stream.toString());
  }

  private void verifyMapping(Schema schema, JsonObject mapping) {
    String schemaName = schema.name();
    Object type = mapping.get("type");
    if (schemaName != null) {
      switch (schemaName) {
        case Date.LOGICAL_NAME:
        case Time.LOGICAL_NAME:
        case Timestamp.LOGICAL_NAME:
          assertEquals("\"" + Mapping.DATE_TYPE + "\"", type.toString());
          return;
        case Decimal.LOGICAL_NAME:
          assertEquals("\"" + Mapping.DOUBLE_TYPE + "\"", type.toString());
          return;
      }
    }

    DataConverter converter = new DataConverter(new ElasticsearchSinkConnectorConfig(ElasticsearchSinkConnectorConfigTest.addNecessaryProps(new HashMap<>())));
    Schema.Type schemaType = schema.type();
    switch (schemaType) {
      case ARRAY:
        verifyMapping(schema.valueSchema(), mapping);
        break;
      case MAP:
        Schema newSchema = converter.preProcessSchema(schema);
        JsonObject mapProperties = mapping.get("properties").getAsJsonObject();
        verifyMapping(newSchema.keySchema(), mapProperties.get(KEY_FIELD).getAsJsonObject());
        verifyMapping(newSchema.valueSchema(), mapProperties.get(VALUE_FIELD).getAsJsonObject());
        break;
      case STRUCT:
        JsonObject properties = mapping.get("properties").getAsJsonObject();
        for (Field field: schema.fields()) {
          verifyMapping(field.schema(), properties.get(field.name()).getAsJsonObject());
        }
        break;
      default:
        assertEquals("\"" + Mapping.getElasticsearchType(schemaType) + "\"", type.toString());
    }
  }
}
