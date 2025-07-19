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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnNullValues;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataConverter {

  private static final Logger log = LoggerFactory.getLogger(DataConverter.class);

  private static final Converter JSON_CONVERTER;
  private static final HeaderConverter HEADER_CONVERTER = new SimpleHeaderConverter();
  protected static final String MAP_KEY = "key";
  protected static final String MAP_VALUE = "value";
  protected static final String TIMESTAMP_FIELD = "@timestamp";

  private ObjectMapper objectMapper;

  static {
    JSON_CONVERTER = new JsonConverter();
    JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
  }

  private final ElasticsearchSinkConnectorConfig config;

  /**
   * Create a DataConverter, specifying how map entries with string keys within record
   * values should be written to JSON. Compact map entries are written as
   * <code>"entryKey": "entryValue"</code>, while the non-compact form are written as a nested
   * document such as <code>{"key": "entryKey", "value": "entryValue"}</code>. All map entries
   * with non-string keys are always written as nested documents.
   *
   * @param config connector config
   */
  public DataConverter(ElasticsearchSinkConnectorConfig config) {
    this.config = config;
    this.objectMapper = new ObjectMapper();
  }

  private String convertKey(Schema keySchema, Object key) {
    if (key == null) {
      throw new DataException("Key is used as document id and can not be null.");
    }
    if (String.valueOf(key).isEmpty()) {
      throw new DataException("Key is used as document id and can not be empty.");
    }

    final Schema.Type schemaType;
    if (keySchema == null) {
      schemaType = ConnectSchema.schemaType(key.getClass());
      if (schemaType == null) {
        throw new DataException(
            "Java class " + key.getClass() + " does not have corresponding schema type."
        );
      }
    } else {
      schemaType = keySchema.type();
    }

    switch (schemaType) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case STRING:
        return String.valueOf(key);
      default:
        throw new DataException(schemaType.name() + " is not supported as the document id.");
    }
  }

  public DocWriteRequest<?> convertRecord(SinkRecord record, String resourceName) {
    if (record.value() == null) {
      switch (config.behaviorOnNullValues()) {
        case IGNORE:
          log.trace("Ignoring {} with null value.", recordString(record));
          return null;
        case DELETE:
          if (record.key() == null) {
            // Since the record key is used as the ID of the index to delete and we don't have a key
            // for this record, we can't delete anything anyways, so we ignore the record.
            // We can also disregard the value of the ignoreKey parameter, since even if it's true
            // the resulting index we'd try to delete would be based solely off topic/partition/
            // offset information for the SinkRecord. Since that information is guaranteed to be
            // unique per message, we can be confident that there wouldn't be any corresponding
            // index present in ES to delete anyways.
            log.trace(
                "Ignoring {} with null key, since the record key is used as the ID of the index",
                recordString(record)
            );
            return null;
          }
          // Will proceed as normal, ultimately creating a DeleteRequest
          log.trace("Deleting {} from Elasticsearch", recordString(record));
          break;
        case FAIL:
        default:
          throw new DataException(
              String.format(
                  "%s has a null value (to ignore future records like"
                      + " this change the configuration property '%s' from '%s' to '%s')",
                  recordString(record),
                  ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG,
                  BehaviorOnNullValues.FAIL,
                  BehaviorOnNullValues.IGNORE
              )
          );
      }
    }

    final String id = config.shouldIgnoreKey(record.topic())
        ? String.format("%s+%d+%d", record.topic(), record.kafkaPartition(), record.kafkaOffset())
        : convertKey(record.keySchema(), record.key());

    // delete
    if (record.value() == null) {
      return maybeAddExternalVersioning(new DeleteRequest(resourceName).id(id), record);
    }

    String payload = getPayload(record);
    payload = maybeAddTimestamp(payload, record.timestamp());

    // index
    switch (config.writeMethod()) {
      case UPSERT:
        return new UpdateRequest(resourceName, id)
            .doc(payload, XContentType.JSON)
            .upsert(payload, XContentType.JSON)
            .retryOnConflict(Math.min(config.maxInFlightRequests(), 5));
      case INSERT:
        OpType opType = config.isDataStream() ? OpType.CREATE : OpType.INDEX;
        IndexRequest req =
                new IndexRequest(resourceName).source(payload, XContentType.JSON).opType(opType);
        if (config.useAutogeneratedIds()) {
          return req;
        }
        return maybeAddExternalVersioning(req.id(id), record);
      default:
        return null; // shouldn't happen
    }
  }

  private String getPayload(SinkRecord record) {
    if (record.value() == null) {
      return null;
    }

    Schema schema = config.shouldIgnoreSchema(record.topic())
        ? record.valueSchema()
        : preProcessSchema(record.valueSchema());
    Object value = config.shouldIgnoreSchema(record.topic())
        ? record.value()
        : preProcessValue(record.value(), record.valueSchema(), schema);

    byte[] rawJsonPayload = JSON_CONVERTER.fromConnectData(record.topic(), schema, value);
    return new String(rawJsonPayload, StandardCharsets.UTF_8);
  }

  private String maybeAddTimestamp(String payload, Long timestamp) {
    if (!config.isDataStream()) {
      return payload;
    }
    try {
      JsonNode jsonNode = objectMapper.readTree(payload);

      if (!jsonNode.isObject()) {
        throw new DataException("Top level payload contains data of Json type "
            + jsonNode.getNodeType() + ". Required Json object.");
      }

      if (!config.dataStreamTimestampField().isEmpty()) {
        for (String timestampField : config.dataStreamTimestampField()) {
          if (jsonNode.has(timestampField)) {
            ((ObjectNode) jsonNode).put(TIMESTAMP_FIELD, jsonNode.get(timestampField).asText());
            return objectMapper.writeValueAsString(jsonNode);
          } else {
            log.debug("Timestamp field {} is not present in payload. This record may fail or "
                    + "be skipped",
                timestampField);
          }
        }
      } else {
        ((ObjectNode) jsonNode).put(TIMESTAMP_FIELD, timestamp);
        return objectMapper.writeValueAsString(jsonNode);
      }
    } catch (JsonProcessingException e) {
      // Should not happen if the payload was retrieved correctly.
    }
    return payload;
  }

  /**
   * In many cases, we explicitly set the record version using the topic's offset.
   * This version will, in turn, be checked by Elasticsearch and will throw a versioning
   * error if the request represents an equivalent or older version of the record.
   *
   * @param request the request currently being constructed for `record`
   * @param record the record to be processed
   * @return the (possibly modified) request which was passed in
   */
  private DocWriteRequest<?> maybeAddExternalVersioning(
      DocWriteRequest<?> request,
      SinkRecord record
  ) {
    if (!config.isDataStream() && !config.shouldIgnoreKey(record.topic())) {
      request.versionType(VersionType.EXTERNAL);
      if (config.hasExternalVersionHeader()) {
        final Header versionHeader = record.headers().lastWithName(config.externalVersionHeader());
        final byte[] versionValue = HEADER_CONVERTER.fromConnectHeader(
                record.topic(),
                versionHeader.key(),
                versionHeader.schema(),
                versionHeader.value()
        );
        try {
          //fromConnectHeader byte output is UTF_8
          request.version(Long.parseLong(new String(versionValue, StandardCharsets.UTF_8)));
        } catch (NumberFormatException e) {
          throw new ConnectException("Error converting to long: "
                  + new String(versionValue, StandardCharsets.UTF_8), e);
        }
      } else {
        request.version(record.kafkaOffset());
      }
    }

    return request;
  }

  // We need to pre process the Kafka Connect schema before converting to JSON as Elasticsearch
  // expects a different JSON format from the current JSON converter provides. Rather than
  // completely rewrite a converter for Elasticsearch, we will refactor the JSON converter to
  // support customized translation. The pre process is no longer needed once we have the JSON
  // converter refactored.
  // visible for testing
  Schema preProcessSchema(Schema schema) {
    if (schema == null) {
      return null;
    }
    // Handle logical types
    String schemaName = schema.name();
    if (schemaName != null) {
      switch (schemaName) {
        case Decimal.LOGICAL_NAME:
          return copySchemaBasics(schema, SchemaBuilder.float64()).build();
        case Date.LOGICAL_NAME:
        case Time.LOGICAL_NAME:
        case Timestamp.LOGICAL_NAME:
          return schema;
        default:
          // User type or unknown logical type
          break;
      }
    }

    Schema.Type schemaType = schema.type();
    switch (schemaType) {
      case ARRAY:
        return preProcessArraySchema(schema);
      case MAP:
        return preProcessMapSchema(schema);
      case STRUCT:
        return preProcessStructSchema(schema);
      default:
        return schema;
    }
  }

  private Schema preProcessArraySchema(Schema schema) {
    Schema valSchema = preProcessSchema(schema.valueSchema());
    return copySchemaBasics(schema, SchemaBuilder.array(valSchema)).build();
  }

  private Schema preProcessMapSchema(Schema schema) {
    Schema keySchema = schema.keySchema();
    Schema valueSchema = schema.valueSchema();
    String keyName = keySchema.name() == null ? keySchema.type().name() : keySchema.name();
    String valueName = valueSchema.name() == null ? valueSchema.type().name() : valueSchema.name();
    Schema preprocessedKeySchema = preProcessSchema(keySchema);
    Schema preprocessedValueSchema = preProcessSchema(valueSchema);
    if (config.useCompactMapEntries() && keySchema.type() == Schema.Type.STRING) {
      SchemaBuilder result = SchemaBuilder.map(preprocessedKeySchema, preprocessedValueSchema);
      return copySchemaBasics(schema, result).build();
    }
    Schema elementSchema = SchemaBuilder.struct().name(keyName + "-" + valueName)
        .field(MAP_KEY, preprocessedKeySchema)
        .field(MAP_VALUE, preprocessedValueSchema)
        .build();
    return copySchemaBasics(schema, SchemaBuilder.array(elementSchema)).build();
  }

  private Schema preProcessStructSchema(Schema schema) {
    SchemaBuilder builder = copySchemaBasics(schema, SchemaBuilder.struct().name(schema.name()));
    for (Field field : schema.fields()) {
      builder.field(field.name(), preProcessSchema(field.schema()));
    }
    return builder.build();
  }

  private SchemaBuilder copySchemaBasics(Schema source, SchemaBuilder target) {
    if (source.isOptional()) {
      target.optional();
    }
    if (source.defaultValue() != null && source.type() != Schema.Type.STRUCT) {
      final Object defaultVal = preProcessValue(source.defaultValue(), source, target);
      target.defaultValue(defaultVal);
    }
    return target;
  }

  // visible for testing
  Object preProcessValue(Object value, Schema schema, Schema newSchema) {
    // Handle missing schemas and acceptable null values
    if (schema == null) {
      return value;
    }

    if (value == null) {
      return preProcessNullValue(schema);
    }

    // Handle logical types
    String schemaName = schema.name();
    if (schemaName != null) {
      Object result = preProcessLogicalValue(schemaName, value);
      if (result != null) {
        return result;
      }
    }

    Schema.Type schemaType = schema.type();
    switch (schemaType) {
      case ARRAY:
        return preProcessArrayValue(value, schema, newSchema);
      case MAP:
        return preProcessMapValue(value, schema, newSchema);
      case STRUCT:
        return preProcessStructValue(value, schema, newSchema);
      default:
        return value;
    }
  }

  private Object preProcessNullValue(Schema schema) {
    if (schema.defaultValue() != null) {
      return schema.defaultValue();
    }
    if (schema.isOptional()) {
      return null;
    }
    throw new DataException("null value for field that is required and has no default value");
  }

  // @returns the decoded logical value or null if this isn't a known logical type
  private Object preProcessLogicalValue(String schemaName, Object value) {
    switch (schemaName) {
      case Decimal.LOGICAL_NAME:
        return ((BigDecimal) value).doubleValue();
      case Date.LOGICAL_NAME:
      case Time.LOGICAL_NAME:
      case Timestamp.LOGICAL_NAME:
        return value;
      default:
        // User-defined type or unknown built-in
        return null;
    }
  }

  private Object preProcessArrayValue(Object value, Schema schema, Schema newSchema) {
    Collection<?> collection = (Collection<?>) value;
    List<Object> result = new ArrayList<>();
    for (Object element: collection) {
      result.add(preProcessValue(element, schema.valueSchema(), newSchema.valueSchema()));
    }
    return result;
  }

  private Object preProcessMapValue(Object value, Schema schema, Schema newSchema) {
    Schema keySchema = schema.keySchema();
    Schema valueSchema = schema.valueSchema();
    Schema newValueSchema = newSchema.valueSchema();
    Map<?, ?> map = (Map<?, ?>) value;
    if (config.useCompactMapEntries() && keySchema.type() == Schema.Type.STRING) {
      Map<Object, Object> processedMap = new HashMap<>();
      for (Map.Entry<?, ?> entry: map.entrySet()) {
        processedMap.put(
            preProcessValue(entry.getKey(), keySchema, newSchema.keySchema()),
            preProcessValue(entry.getValue(), valueSchema, newValueSchema)
        );
      }
      return processedMap;
    }
    List<Struct> mapStructs = new ArrayList<>();
    for (Map.Entry<?, ?> entry: map.entrySet()) {
      Struct mapStruct = new Struct(newValueSchema);
      Schema mapKeySchema = newValueSchema.field(MAP_KEY).schema();
      Schema mapValueSchema = newValueSchema.field(MAP_VALUE).schema();
      mapStruct.put(MAP_KEY, preProcessValue(entry.getKey(), keySchema, mapKeySchema));
      mapStruct.put(MAP_VALUE, preProcessValue(entry.getValue(), valueSchema, mapValueSchema));
      mapStructs.add(mapStruct);
    }
    return mapStructs;
  }

  private Object preProcessStructValue(Object value, Schema schema, Schema newSchema) {
    Struct struct = (Struct) value;
    Struct newStruct = new Struct(newSchema);
    for (Field field : schema.fields()) {
      Schema newFieldSchema = newSchema.field(field.name()).schema();
      Object converted = preProcessValue(struct.get(field), field.schema(), newFieldSchema);
      newStruct.put(field.name(), converted);
    }
    return newStruct;
  }

  private static String recordString(SinkRecord record) {
    return String.format(
        "record from topic=%s partition=%s offset=%s",
        record.topic(),
        record.kafkaPartition(),
        record.kafkaOffset()
    );
  }
}
