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

import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonObject;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import static io.confluent.connect.elasticsearch.DataConverter.BehaviorOnNullValues;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConstants.KEYWORD_TYPE;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConstants.TEXT_TYPE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MappingTest extends ElasticsearchSinkTestBase {

  private static final String INDEX = "kafka-connect";
  private static final String TYPE = "kafka-connect-type";

  @Test
  @SuppressWarnings("unchecked")
  public void testMapping() throws Exception {
    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(1);

    createIndex(INDEX);
    Schema schema = createSchema();
    Mapping.createMapping(client, INDEX, TYPE, schema);

    JsonObject mapping = Mapping.getMapping(client, INDEX, TYPE);
    assertNotNull(mapping);
    verifyMapping(schema, mapping);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStringMappingForES6() throws Exception {
    ElasticsearchClient client = mock(ElasticsearchClient.class);
    when(client.getVersion()).thenReturn(ElasticsearchClient.Version.ES_V6);

    Schema schema = SchemaBuilder.struct().name("textRecord")
            .field("string", Schema.STRING_SCHEMA)
            .build();
    ObjectNode mapping = (ObjectNode) Mapping.inferMapping(client, schema);
    ObjectNode properties = mapping.with("properties");
    ObjectNode string = properties.with("string");
    TextNode stringType = (TextNode) string.get("type");
    ObjectNode fields = string.with("fields");
    ObjectNode keyword = fields.with("keyword");
    TextNode keywordType = (TextNode) keyword.get("type");
    NumericNode ignoreAbove = (NumericNode) keyword.get("ignore_above");

    assertEquals(TEXT_TYPE, stringType.asText());
    assertEquals(KEYWORD_TYPE, keywordType.asText());
    assertEquals(256, ignoreAbove.asInt());
  }

  @Test
  public void testInferMapping() throws Exception {

    Schema stringSchema = SchemaBuilder
        .struct()
        .name("record")
        .field("foo", SchemaBuilder.string().defaultValue("0").build())
        .build();
    JsonNode stringMapping = Mapping.inferMapping(client, stringSchema);

    assertNull(stringMapping.get("properties").get("foo").get("null_value"));

    Schema intSchema =SchemaBuilder
        .struct()
        .name("record")
        .field("foo", SchemaBuilder.int32().defaultValue(0).build())
        .build();

    JsonNode intMapping = Mapping.inferMapping(client, intSchema);
    assertNotNull(intMapping.get("properties").get("foo").get("null_value"));
    assertEquals(0, intMapping.get("properties").get("foo").get("null_value").asInt());
  }

  @Test
  public void testInferMappingDefaultDate()  {
    java.util.Date expected = new java.util.Date();

    Schema dateSchema = SchemaBuilder
        .struct()
        .name("record")
        .field("foo", Date.builder().defaultValue(expected).build())
        .build();

    JsonNode dateMapping = Mapping.inferMapping(client, dateSchema);
    assertNotNull(dateMapping.get("properties").get("foo").get("null_value"));
    assertEquals(
        expected.getTime(),
        dateMapping.get("properties").get("foo").get("null_value").asLong()
    );
  }

  protected Schema createSchema() {
    Schema structSchema = createInnerSchema();
    return SchemaBuilder.struct().name("record")
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
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
        .field("struct", structSchema)
        .field("decimal", Decimal.schema(2))
        .field("date", Date.SCHEMA)
        .field("time", Time.SCHEMA)
        .field("timestamp", Timestamp.SCHEMA)
        .build();
  }

  private Schema createInnerSchema() {
    return SchemaBuilder.struct().name("inner")
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
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
        .field("decimal", Decimal.schema(2))
        .field("date", Date.SCHEMA)
        .field("time", Time.SCHEMA)
        .field("timestamp", Timestamp.SCHEMA)
        .build();
  }

  @SuppressWarnings("unchecked")
  private void verifyMapping(Schema schema, JsonObject mapping) throws Exception {
    String schemaName = schema.name();
    Object type = mapping.get("type");
    if (schemaName != null) {
      switch (schemaName) {
        case Date.LOGICAL_NAME:
        case Time.LOGICAL_NAME:
        case Timestamp.LOGICAL_NAME:
          assertEquals("\"" + ElasticsearchSinkConnectorConstants.DATE_TYPE + "\"", type.toString());
          return;
        case Decimal.LOGICAL_NAME:
          assertEquals("\"" + ElasticsearchSinkConnectorConstants.DOUBLE_TYPE + "\"", type.toString());
          return;
      }
    }

    DataConverter converter = new DataConverter(true, BehaviorOnNullValues.IGNORE, DataConverter.DocumentVersionType.LEGACY);
    Schema.Type schemaType = schema.type();
    switch (schemaType) {
      case ARRAY:
        verifyMapping(schema.valueSchema(), mapping);
        break;
      case MAP:
        Schema newSchema = converter.preProcessSchema(schema);
        JsonObject mapProperties = mapping.get("properties").getAsJsonObject();
        verifyMapping(newSchema.keySchema(), mapProperties.get(ElasticsearchSinkConnectorConstants.MAP_KEY).getAsJsonObject());
        verifyMapping(newSchema.valueSchema(), mapProperties.get(ElasticsearchSinkConnectorConstants.MAP_VALUE).getAsJsonObject());
        break;
      case STRUCT:
        JsonObject properties = mapping.get("properties").getAsJsonObject();
        for (Field field: schema.fields()) {
          verifyMapping(field.schema(), properties.get(field.name()).getAsJsonObject());
        }
        break;
      default:
        assertEquals("\"" + Mapping.getElasticsearchType(client, schemaType) + "\"", type.toString());
    }
  }
}
