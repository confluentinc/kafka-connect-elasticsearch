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

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConstants.MAP_KEY;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConstants.MAP_VALUE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Mapping {

  private static final Logger log = LoggerFactory.getLogger(Mapping.class);

  /**
   * Create an explicit mapping.
   * @param client The client to connect to Elasticsearch.
   * @param index The index to write to Elasticsearch.
   * @param type The type to create mapping for.
   * @param schema The schema used to infer mapping.
   * @throws IOException
   */
  public static void createMapping(Client client, String index, String type, Schema schema) throws IOException {
    XContentBuilder xContentBuilder = jsonBuilder();
    xContentBuilder.startObject();
    inferMapping(type, schema, xContentBuilder);
    xContentBuilder.endObject();

    PutMappingResponse response = client.admin().indices()
        .preparePutMapping(index)
        .setType(type)
        .setSource(xContentBuilder)
        .execute()
        .actionGet();

    if (!response.isAcknowledged()) {
      throw new ConnectException("Cannot create mapping:" + xContentBuilder.bytes().toUtf8());
    }
  }

  /**
   * Check the whether a mapping exists or not for a type.
   * @param client The client to connect to Elasticsearch.
   * @param index The index to write to Elasticsearch.
   * @param type The type to check.
   * @return Whether the type exists or not.
   */
  public static boolean doesMappingExist(Client client, String index, String type, Set<String> mappings) {
    if (mappings.contains(index)) {
      return true;
    }
    GetMappingsResponse mappingsResponse = client.admin().indices().prepareGetMappings(index).setTypes(type).get();
    if (mappingsResponse.getMappings().get(index) == null) {
      return false;
    } else {
      boolean exist = mappingsResponse.getMappings().get(index).containsKey(type);
      if (exist) {
        mappings.add(index);
      }
      return exist;
    }
  }

  /**
   * Infer mapping from the provided schema.
   * @param name The name of a field.
   * @param schema The schema used to infer mapping.
   * @param xContentBuilder Builder of xContent. This is used to construct mapping.
   */
  public static void inferMapping(String name, Schema schema, XContentBuilder xContentBuilder) {
    if (schema == null) {
      throw new DataException("Cannot infer mapping without schema.");
    }

    // Handle logical types
    String schemaName = schema.name();
    Object defaultValue = schema.defaultValue();
    if (schemaName != null) {
      try {
        switch (schemaName) {
          case Date.LOGICAL_NAME:
          case Time.LOGICAL_NAME:
          case Timestamp.LOGICAL_NAME:
            inferPrimitive(name, ElasticsearchSinkConnectorConstants.DATE_TYPE, defaultValue, xContentBuilder);
            return;
          case Decimal.LOGICAL_NAME:
            inferPrimitive(name, ElasticsearchSinkConnectorConstants.DOUBLE_TYPE, defaultValue, xContentBuilder);
            return;
        }
      } catch (IOException e) {
        log.error("Error infer mapping from schema.");
      }
    }

    Schema keySchema;
    Schema valueSchema;
    Schema.Type schemaType = schema.type();
    try {
      switch (schemaType) {
        case ARRAY:
          valueSchema = schema.valueSchema();
          inferMapping(name, valueSchema, xContentBuilder);
          break;
        case MAP:
          keySchema = schema.keySchema();
          valueSchema = schema.valueSchema();
          xContentBuilder.startObject(name);
          xContentBuilder.startObject("properties");
          inferMapping(MAP_KEY, keySchema, xContentBuilder);
          inferMapping(MAP_VALUE, valueSchema, xContentBuilder);
          xContentBuilder.endObject();
          xContentBuilder.endObject();
          break;
        case STRUCT:
          xContentBuilder.startObject(name);
          xContentBuilder.startObject("properties");
          for (Field field : schema.fields()) {
            String fieldName = field.name();
            Schema fieldSchema = field.schema();
            inferMapping(fieldName, fieldSchema, xContentBuilder);
          }
          xContentBuilder.endObject();
          xContentBuilder.endObject();
          break;
        default:
          // Primitive types
          inferPrimitive(name, ElasticsearchSinkConnectorConstants.TYPES.get(schemaType), defaultValue, xContentBuilder);
      }
    } catch (IOException e) {
      log.error("Error infer mapping from schema.");
    }
  }

  private static void inferPrimitive(String name, String type, Object defaultValue, XContentBuilder xContentBuilder) throws IOException {
    if (type == null) {
      throw new ConnectException("Invalid primitive type.");
    }
    xContentBuilder.startObject(name);
    xContentBuilder.field("type", type);
    if (defaultValue != null) {
      xContentBuilder.field("null_value", defaultValue);
    }
    xContentBuilder.endObject();
  }
}
