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

import com.google.gson.JsonObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConstants.MAP_KEY;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConstants.MAP_VALUE;

public class Mapping {

  private static final Logger log = LoggerFactory.getLogger(Mapping.class);

  /**
   * Create an explicit mapping.
   * @param client The client to connect to Elasticsearch.
   * @param index The index to write to Elasticsearch.
   * @param type The type to create mapping for.
   * @param schema The schema used to infer mapping.
   * @throws IOException from underlying JestClient
   */
  public static void createMapping(JestClient client, String index, String type, Schema schema)
      throws IOException {
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set(type, inferMapping(schema));
    PutMapping putMapping = new PutMapping.Builder(index, type, obj.toString()).build();
    JestResult result = client.execute(putMapping);
    if (!result.isSucceeded()) {
      throw new ConnectException(
          "Cannot create mapping " + obj + " -- " + result.getErrorMessage()
      );
    }
  }

  /**
   * Get the JSON mapping for given index and type. Returns {@code null} if it does not exist.
   */
  public static JsonObject getMapping(JestClient client, String index, String type)
      throws IOException {
    final JestResult result = client.execute(
        new GetMapping.Builder().addIndex(index).addType(type).build()
    );
    final JsonObject indexRoot = result.getJsonObject().getAsJsonObject(index);
    if (indexRoot == null) {
      return null;
    }
    final JsonObject mappingsJson = indexRoot.getAsJsonObject("mappings");
    if (mappingsJson == null) {
      return  null;
    }
    return mappingsJson.getAsJsonObject(type);
  }

  /**
   * Infer mapping from the provided schema.
   * @param schema The schema used to infer mapping.
   */
  public static JsonNode inferMapping(Schema schema) {
    if (schema == null) {
      throw new DataException("Cannot infer mapping without schema.");
    }

    // Handle logical types
    JsonNode logicalConversion = inferLogicalMapping(schema);
    if (logicalConversion != null) {
      return logicalConversion;
    }

    Schema.Type schemaType = schema.type();
    ObjectNode properties = JsonNodeFactory.instance.objectNode();
    ObjectNode fields = JsonNodeFactory.instance.objectNode();
    switch (schemaType) {
      case ARRAY:
        return inferMapping(schema.valueSchema());
      case MAP:
        properties.set("properties", fields);
        fields.set(MAP_KEY, inferMapping(schema.keySchema()));
        fields.set(MAP_VALUE, inferMapping(schema.valueSchema()));
        return properties;
      case STRUCT:
        properties.set("properties", fields);
        for (Field field : schema.fields()) {
          fields.set(field.name(), inferMapping(field.schema()));
        }
        return properties;
      default:
        String esType = ElasticsearchSinkConnectorConstants.TYPES.get(schemaType);
        return inferPrimitive(esType, schema.defaultValue());
    }
  }

  private static JsonNode inferLogicalMapping(Schema schema) {
    String schemaName = schema.name();
    Object defaultValue = schema.defaultValue();
    if (schemaName == null) {
      return null;
    }

    switch (schemaName) {
      case Date.LOGICAL_NAME:
      case Time.LOGICAL_NAME:
      case Timestamp.LOGICAL_NAME:
        return inferPrimitive(ElasticsearchSinkConnectorConstants.DATE_TYPE, defaultValue);
      case Decimal.LOGICAL_NAME:
        return inferPrimitive(ElasticsearchSinkConnectorConstants.DOUBLE_TYPE, defaultValue);
      default:
        // User-defined type or unknown built-in
        return null;
    }
  }

  private static JsonNode inferPrimitive(String type, Object defaultValue) {
    if (type == null) {
      throw new ConnectException("Invalid primitive type.");
    }

    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("type", JsonNodeFactory.instance.textNode(type));
    JsonNode defaultValueNode = null;
    if (defaultValue != null) {
      switch (type) {
        case ElasticsearchSinkConnectorConstants.BYTE_TYPE:
          defaultValueNode = JsonNodeFactory.instance.numberNode((byte) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.SHORT_TYPE:
          defaultValueNode = JsonNodeFactory.instance.numberNode((short) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.INTEGER_TYPE:
          defaultValueNode = JsonNodeFactory.instance.numberNode((int) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.LONG_TYPE:
          defaultValueNode = JsonNodeFactory.instance.numberNode((long) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.FLOAT_TYPE:
          defaultValueNode = JsonNodeFactory.instance.numberNode((float) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.DOUBLE_TYPE:
          defaultValueNode = JsonNodeFactory.instance.numberNode((double) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.STRING_TYPE:
          defaultValueNode = JsonNodeFactory.instance.textNode((String) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.BINARY_TYPE:
          defaultValueNode = JsonNodeFactory.instance.binaryNode(bytes(defaultValue));
          break;
        case ElasticsearchSinkConnectorConstants.BOOLEAN_TYPE:
          defaultValueNode = JsonNodeFactory.instance.booleanNode((boolean) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.DATE_TYPE:
          defaultValueNode = JsonNodeFactory.instance.numberNode((long) defaultValue);
          break;
        default:
          throw new DataException("Invalid primitive type.");
      }
    }
    if (defaultValueNode != null) {
      obj.set("null_value", defaultValueNode);
    }
    return obj;
  }

  private static byte[] bytes(Object value) {
    final byte[] bytes;
    if (value instanceof ByteBuffer) {
      final ByteBuffer buffer = ((ByteBuffer) value).slice();
      bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
    } else {
      bytes = (byte[]) value;
    }
    return bytes;
  }

}
