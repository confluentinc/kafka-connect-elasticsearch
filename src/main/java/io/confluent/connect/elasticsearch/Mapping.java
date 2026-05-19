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

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
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

  /**
   * Build mapping from the provided schema.
   *
   * @param schema The schema used to build the mapping.
   * @return the schema as a mapping structure
   */
  public static Map<String, Object> buildMapping(Schema schema) {
    Map<String, Object> root = new LinkedHashMap<>();
    buildMapping(schema, root);
    return root;
  }

  private static void buildMapping(Schema schema, Map<String, Object> out) {
    if (schema == null) {
      throw new DataException("Cannot infer mapping without schema.");
    }

    // Handle logical types
    Map<String, Object> logicalConversion = inferLogicalMapping(schema);
    if (logicalConversion != null) {
      out.putAll(logicalConversion);
      return;
    }

    Schema.Type schemaType = schema.type();
    switch (schemaType) {
      case ARRAY:
        buildMapping(schema.valueSchema(), out);
        return;

      case MAP:
        out.putAll(buildMap(schema));
        return;

      case STRUCT:
        out.putAll(buildStruct(schema));
        return;

      default:
        out.putAll(inferPrimitive(getElasticsearchType(schemaType), schema.defaultValue()));
    }
  }

  private static Map<String, Object> addTextMapping() {
    // Add additional mapping for indexing, per https://www.elastic.co/blog/strings-are-dead-long-live-strings
    Map<String, Object> keyword = new LinkedHashMap<>();
    keyword.put(TYPE_FIELD, KEYWORD_TYPE);
    keyword.put(IGNORE_ABOVE_FIELD, 256);
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put(KEYWORD_FIELD, keyword);
    return fields;
  }

  private static Map<String, Object> buildMap(Schema schema) {
    Map<String, Object> keyMap = new LinkedHashMap<>();
    buildMapping(schema.keySchema(), keyMap);
    Map<String, Object> valueMap = new LinkedHashMap<>();
    buildMapping(schema.valueSchema(), valueMap);

    Map<String, Object> properties = new LinkedHashMap<>();
    properties.put(KEY_FIELD, keyMap);
    properties.put(VALUE_FIELD, valueMap);

    Map<String, Object> result = new LinkedHashMap<>();
    result.put(PROPERTIES_FIELD, properties);
    return result;
  }

  private static Map<String, Object> buildStruct(Schema schema) {
    Map<String, Object> properties = new LinkedHashMap<>();
    for (Field field : schema.fields()) {
      Map<String, Object> fieldMap = new LinkedHashMap<>();
      buildMapping(field.schema(), fieldMap);
      properties.put(field.name(), fieldMap);
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put(PROPERTIES_FIELD, properties);
    return result;
  }

  private static Map<String, Object> inferPrimitive(String type, Object defaultValue) {
    if (type == null) {
      throw new DataException(String.format("Invalid primitive type %s.", type));
    }

    Map<String, Object> m = new LinkedHashMap<>();
    m.put(TYPE_FIELD, type);
    if (TEXT_TYPE.equals(type)) {
      m.put(FIELDS_FIELD, addTextMapping());
    }

    if (defaultValue == null) {
      return m;
    }

    switch (type) {
      case BYTE_TYPE:
        m.put(DEFAULT_VALUE_FIELD, (byte) defaultValue);
        break;
      case SHORT_TYPE:
        m.put(DEFAULT_VALUE_FIELD, (short) defaultValue);
        break;
      case INTEGER_TYPE:
        m.put(DEFAULT_VALUE_FIELD, (int) defaultValue);
        break;
      case LONG_TYPE:
        m.put(DEFAULT_VALUE_FIELD, (long) defaultValue);
        break;
      case FLOAT_TYPE:
        m.put(DEFAULT_VALUE_FIELD, (float) defaultValue);
        break;
      case DOUBLE_TYPE:
        m.put(DEFAULT_VALUE_FIELD, (double) defaultValue);
        break;
      case BOOLEAN_TYPE:
        m.put(DEFAULT_VALUE_FIELD, (boolean) defaultValue);
        break;
      case DATE_TYPE:
        m.put(DEFAULT_VALUE_FIELD, ((java.util.Date) defaultValue).getTime());
        break;
      /*
       * IGNORE default values for text and binary types as this is not supported by ES side.
       * see https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html and
       * https://www.elastic.co/guide/en/elasticsearch/reference/current/binary.html for details.
       */
      case STRING_TYPE:
      case TEXT_TYPE:
      case BINARY_TYPE:
        break;
      default:
        throw new DataException("Invalid primitive type " + type + ".");
    }
    return m;
  }

  private static Map<String, Object> inferLogicalMapping(Schema schema) {
    if (schema.name() == null) {
      return null;
    }

    switch (schema.name()) {
      case Date.LOGICAL_NAME:
      case Time.LOGICAL_NAME:
      case Timestamp.LOGICAL_NAME:
        return inferPrimitive(DATE_TYPE, schema.defaultValue());
      case Decimal.LOGICAL_NAME:
        Double defaultValue = schema.defaultValue() != null
            ? ((BigDecimal) schema.defaultValue()).doubleValue() : null;
        return inferPrimitive(DOUBLE_TYPE, defaultValue);
      default:
        // User-defined type or unknown built-in
        return null;
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
