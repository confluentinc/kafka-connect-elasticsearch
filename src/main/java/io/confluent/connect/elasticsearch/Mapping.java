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
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

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
   * @return the schema as a JSON mapping
   */
  public static XContentBuilder buildMapping(Schema schema) {
    try {
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.startObject();
      {
        buildMapping(schema, builder);
      }
      builder.endObject();
      return builder;
    } catch (IOException e) {
      throw new ConnectException("Failed to build mapping for schema " + schema, e);
    }
  }

  private static XContentBuilder buildMapping(Schema schema, XContentBuilder builder)
      throws IOException {

    if (schema == null) {
      throw new DataException("Cannot infer mapping without schema.");
    }

    // Handle logical types
    XContentBuilder logicalConversion = inferLogicalMapping(builder, schema);
    if (logicalConversion != null) {
      return logicalConversion;
    }

    Schema.Type schemaType = schema.type();
    switch (schema.type()) {
      case ARRAY:
        return buildMapping(schema.valueSchema(), builder);

      case MAP:
        return buildMap(schema, builder);

      case STRUCT:
        return buildStruct(schema, builder);

      default:
        return inferPrimitive(builder, getElasticsearchType(schemaType), schema.defaultValue());
    }
  }

  private static void addTextMapping(XContentBuilder builder) throws IOException {
    // Add additional mapping for indexing, per https://www.elastic.co/blog/strings-are-dead-long-live-strings
    builder.startObject(FIELDS_FIELD);
    {
      builder.startObject(KEYWORD_FIELD);
      {
        builder.field(TYPE_FIELD, KEYWORD_TYPE);
        builder.field(IGNORE_ABOVE_FIELD, 256);
      }
      builder.endObject();
    }
    builder.endObject();
  }

  private static XContentBuilder buildMap(Schema schema, XContentBuilder builder)
      throws IOException {

    builder.startObject(PROPERTIES_FIELD);
    {
      builder.startObject(KEY_FIELD);
      {
        buildMapping(schema.keySchema(), builder);
      }
      builder.endObject();
      builder.startObject(VALUE_FIELD);
      {
        buildMapping(schema.valueSchema(), builder);
      }
      builder.endObject();
    }
    return builder.endObject();
  }

  private static XContentBuilder buildStruct(Schema schema, XContentBuilder builder)
      throws IOException {

    builder.startObject(PROPERTIES_FIELD);
    {
      for (Field field : schema.fields()) {
        builder.startObject(field.name());
        {
          buildMapping(field.schema(), builder);
        }
        builder.endObject();
      }
    }
    return builder.endObject();
  }

  private static XContentBuilder inferPrimitive(
      XContentBuilder builder,
      String type,
      Object defaultValue
  ) throws IOException {

    if (type == null) {
      throw new DataException(String.format("Invalid primitive type %s.", type));
    }

    builder.field(TYPE_FIELD, type);
    if (type.equals(TEXT_TYPE)) {
      addTextMapping(builder);
    }

    if (defaultValue == null) {
      return builder;
    }

    switch (type) {
      case BYTE_TYPE:
        return builder.field(DEFAULT_VALUE_FIELD, (byte) defaultValue);
      case SHORT_TYPE:
        return builder.field(DEFAULT_VALUE_FIELD, (short) defaultValue);
      case INTEGER_TYPE:
        return builder.field(DEFAULT_VALUE_FIELD, (int) defaultValue);
      case LONG_TYPE:
        return builder.field(DEFAULT_VALUE_FIELD, (long) defaultValue);
      case FLOAT_TYPE:
        return builder.field(DEFAULT_VALUE_FIELD, (float) defaultValue);
      case DOUBLE_TYPE:
        return builder.field(DEFAULT_VALUE_FIELD, (double) defaultValue);
      case BOOLEAN_TYPE:
        return builder.field(DEFAULT_VALUE_FIELD, (boolean) defaultValue);
      case DATE_TYPE:
        return builder.field(DEFAULT_VALUE_FIELD, ((java.util.Date) defaultValue).getTime());
      /*
       * IGNORE default values for text and binary types as this is not supported by ES side.
       * see https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html and
       * https://www.elastic.co/guide/en/elasticsearch/reference/current/binary.html for details.
       */
      case STRING_TYPE:
      case TEXT_TYPE:
      case BINARY_TYPE:
        return builder;
      default:
        throw new DataException("Invalid primitive type " + type + ".");
    }
  }

  private static XContentBuilder inferLogicalMapping(XContentBuilder builder, Schema schema)
      throws IOException {

    if (schema.name() == null) {
      return null;
    }

    switch (schema.name()) {
      case Date.LOGICAL_NAME:
      case Time.LOGICAL_NAME:
      case Timestamp.LOGICAL_NAME:
        return inferPrimitive(builder, DATE_TYPE, schema.defaultValue());
      case Decimal.LOGICAL_NAME:
        Double defaultValue = schema.defaultValue() != null ? ((BigDecimal) schema.defaultValue())
            .doubleValue() : null;
        return inferPrimitive(builder, DOUBLE_TYPE, defaultValue);
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
