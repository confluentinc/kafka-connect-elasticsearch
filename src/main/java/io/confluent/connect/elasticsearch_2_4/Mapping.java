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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonObject;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConstants.BINARY_TYPE;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConstants.BOOLEAN_TYPE;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConstants.BYTE_TYPE;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConstants.DOUBLE_TYPE;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConstants.FLOAT_TYPE;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConstants.INTEGER_TYPE;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConstants.KEYWORD_TYPE;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConstants.LONG_TYPE;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConstants.MAP_KEY;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConstants.MAP_VALUE;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConstants.SHORT_TYPE;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConstants.STRING_TYPE;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConstants.TEXT_TYPE;

public class Mapping {

  /**
   * Create an explicit mapping.
   *
   * @param client The client to connect to Elasticsearch.
   * @param index  The index to write to Elasticsearch.
   * @param type   The type to create mapping for.
   * @param schema The schema used to infer mapping.
   * @throws IOException from underlying JestClient
   */
  public static void createMapping(
      ElasticsearchClient client,
      String index,
      String type,
      Schema schema
  )
      throws IOException {
    client.createMapping(index, type, schema);
  }

  /**
   * Get the JSON mapping for given index and type. Returns {@code null} if it does not exist.
   */
  public static JsonObject getMapping(ElasticsearchClient client, String index, String type)
      throws IOException {
    return client.getMapping(index, type);
  }

  /**
   * Infer mapping from the provided schema.
   *
   * @param schema The schema used to infer mapping.
   */
  public static JsonNode inferMapping(ElasticsearchClient client, Schema schema) {
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
        return inferMapping(client, schema.valueSchema());
      case MAP:
        properties.set("properties", fields);
        fields.set(MAP_KEY, inferMapping(client, schema.keySchema()));
        fields.set(MAP_VALUE, inferMapping(client, schema.valueSchema()));
        return properties;
      case STRUCT:
        properties.set("properties", fields);
        for (Field field : schema.fields()) {
          fields.set(field.name(), inferMapping(client, field.schema()));
        }
        return properties;
      default:
        String esType = getElasticsearchType(client, schemaType);
        return inferPrimitive(esType, schema.defaultValue());
    }
  }

  // visible for testing
  protected static String getElasticsearchType(ElasticsearchClient client, Schema.Type schemaType) {
    switch (schemaType) {
      case BOOLEAN:
        return BOOLEAN_TYPE;
      case INT8:
        return BYTE_TYPE;
      case INT16:
        return SHORT_TYPE;
      case INT32:
        return INTEGER_TYPE;
      case INT64:
        return LONG_TYPE;
      case FLOAT32:
        return FLOAT_TYPE;
      case FLOAT64:
        return DOUBLE_TYPE;
      case STRING:
        switch (client.getVersion()) {
          case ES_V1:
          case ES_V2:
            return STRING_TYPE;
          case ES_V5:
          case ES_V6:
          default:
            return TEXT_TYPE;
        }
      case BYTES:
        return BINARY_TYPE;
      default:
        return null;
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
    if (type.equals(TEXT_TYPE)) {
      addTextMapping(obj);
    }
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
        case ElasticsearchSinkConnectorConstants.TEXT_TYPE:
        case ElasticsearchSinkConnectorConstants.BINARY_TYPE:
          // IGNORE default values for text and binary types as this is not supported by ES side.
          // see https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html
          // https://www.elastic.co/guide/en/elasticsearch/reference/current/binary.html
          // for more details.
          //defaultValueNode = null;
          break;
        case ElasticsearchSinkConnectorConstants.BOOLEAN_TYPE:
          defaultValueNode = JsonNodeFactory.instance.booleanNode((boolean) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.DATE_TYPE:
          long value = ((java.util.Date) defaultValue).getTime();
          defaultValueNode = JsonNodeFactory.instance.numberNode(value);
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

  private static void addTextMapping(ObjectNode obj) {
    // Add additional mapping for indexing, per https://www.elastic.co/blog/strings-are-dead-long-live-strings
    ObjectNode keyword = JsonNodeFactory.instance.objectNode();
    keyword.set("type", JsonNodeFactory.instance.textNode(KEYWORD_TYPE));
    keyword.set("ignore_above", JsonNodeFactory.instance.numberNode(256));
    ObjectNode fields = JsonNodeFactory.instance.objectNode();
    fields.set("keyword", keyword);
    obj.set("fields", fields);
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
