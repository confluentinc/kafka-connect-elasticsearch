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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

public class Mapping {

  // Elasticsearch types
  public static final String BOOLEAN_TYPE = "boolean";
  public static final String BYTE_TYPE = "byte";
  public static final String BINARY_TYPE = "binary";
  public static final String SHORT_TYPE = "short";
  public static final String INTEGER_TYPE = "integer";
  public static final String LONG_TYPE = "long";
  public static final String FLOAT_TYPE = "float";
  public static final String DOUBLE_TYPE = "double";
  public static final String STRING_TYPE = "string";
  public static final String TEXT_TYPE = "text";
  public static final String KEYWORD_TYPE = "keyword";
  public static final String DATE_TYPE = "date";

  // Elasticsearch mapping fields
  private static final String DEFAULT_VALUE_FIELD = "null_value";
  private static final String FIELDS_FIELD = "fields";
  private static final String IGNORE_ABOVE_FIELD = "ignore_above";
  public static final String KEY_FIELD = "key";
  private static final String KEYWORD_FIELD = "keyword";
  private static final String PROPERTIES_FIELD = "properties";
  private static final String TYPE_FIELD = "type";
  public static final String VALUE_FIELD = "value";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Build mapping from the provided schema.
   *
   * @param schema The schema used to build the mapping.
   * @return the schema as a JSON mapping
   */
  public static ObjectNode buildMapping(Schema schema) {
    ObjectNode mapping = OBJECT_MAPPER.createObjectNode();
    buildMapping(schema, mapping);
    return mapping;
  }

  private static void buildMapping(Schema schema, ObjectNode node) {
    if (schema == null) {
      throw new DataException("Cannot infer mapping without schema.");
    }

    // Handle logical types
    if (inferLogicalMapping(node, schema)) {
      return;
    }

    Schema.Type schemaType = schema.type();
    switch (schemaType) {
      case ARRAY:
        buildMapping(schema.valueSchema(), node);
        return;

      case MAP:
        buildMap(schema, node);
        return;

      case STRUCT:
        buildStruct(schema, node);
        return;

      default:
        inferPrimitive(node, getElasticsearchType(schemaType), schema.defaultValue());
    }
  }

  /**
   * Build mapping from the provided schema, serialized as a JSON string.
   *
   * @param schema The schema used to build the mapping.
   * @return the schema as a JSON mapping string
   */
  public static String buildMappingJson(Schema schema) {
    return buildMapping(schema).toString();
  }

  private static void addTextMapping(ObjectNode node) {
    // Add additional mapping for indexing, per https://www.elastic.co/blog/strings-are-dead-long-live-strings
    ObjectNode keyword = node.putObject(FIELDS_FIELD).putObject(KEYWORD_FIELD);
    keyword.put(TYPE_FIELD, KEYWORD_TYPE);
    keyword.put(IGNORE_ABOVE_FIELD, 256);
  }

  private static void buildMap(Schema schema, ObjectNode node) {
    ObjectNode properties = node.putObject(PROPERTIES_FIELD);
    buildMapping(schema.keySchema(), properties.putObject(KEY_FIELD));
    buildMapping(schema.valueSchema(), properties.putObject(VALUE_FIELD));
  }

  private static void buildStruct(Schema schema, ObjectNode node) {
    ObjectNode properties = node.putObject(PROPERTIES_FIELD);
    for (Field field : schema.fields()) {
      buildMapping(field.schema(), properties.putObject(field.name()));
    }
  }

  private static void inferPrimitive(ObjectNode node, String type, Object defaultValue) {
    if (type == null) {
      throw new DataException(String.format("Invalid primitive type %s.", type));
    }

    node.put(TYPE_FIELD, type);
    if (type.equals(TEXT_TYPE)) {
      addTextMapping(node);
    }

    if (defaultValue == null) {
      return;
    }

    switch (type) {
      case BYTE_TYPE:
        node.put(DEFAULT_VALUE_FIELD, (byte) defaultValue);
        return;
      case SHORT_TYPE:
        node.put(DEFAULT_VALUE_FIELD, (short) defaultValue);
        return;
      case INTEGER_TYPE:
        node.put(DEFAULT_VALUE_FIELD, (int) defaultValue);
        return;
      case LONG_TYPE:
        node.put(DEFAULT_VALUE_FIELD, (long) defaultValue);
        return;
      case FLOAT_TYPE:
        node.put(DEFAULT_VALUE_FIELD, (float) defaultValue);
        return;
      case DOUBLE_TYPE:
        node.put(DEFAULT_VALUE_FIELD, (double) defaultValue);
        return;
      case BOOLEAN_TYPE:
        node.put(DEFAULT_VALUE_FIELD, (boolean) defaultValue);
        return;
      case DATE_TYPE:
        node.put(DEFAULT_VALUE_FIELD, ((java.util.Date) defaultValue).getTime());
        return;
      /*
       * IGNORE default values for text and binary types as this is not supported by ES side.
       * see https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html and
       * https://www.elastic.co/guide/en/elasticsearch/reference/current/binary.html for details.
       */
      case STRING_TYPE:
      case TEXT_TYPE:
      case BINARY_TYPE:
        return;
      default:
        throw new DataException("Invalid primitive type " + type + ".");
    }
  }

  private static boolean inferLogicalMapping(ObjectNode node, Schema schema) {
    if (schema.name() == null) {
      return false;
    }

    switch (schema.name()) {
      case Date.LOGICAL_NAME:
      case Time.LOGICAL_NAME:
      case Timestamp.LOGICAL_NAME:
        inferPrimitive(node, DATE_TYPE, schema.defaultValue());
        return true;
      case Decimal.LOGICAL_NAME:
        Double defaultValue = schema.defaultValue() != null ? ((BigDecimal) schema.defaultValue())
            .doubleValue() : null;
        inferPrimitive(node, DOUBLE_TYPE, defaultValue);
        return true;
      default:
        // User-defined type or unknown built-in
        return false;
    }
  }

  // visible for testing
  protected static String getElasticsearchType(Schema.Type schemaType) {
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
        return TEXT_TYPE;
      case BYTES:
        return BINARY_TYPE;
      default:
        return null;
    }
  }
}
