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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.util.Map;

public class MappingTest extends ESIntegTestCase {

  private static final String INDEX = "kafka-connect";
  private static final String TYPE = "kafka-connect-type";

  @Test
  @SuppressWarnings("unchecked")
  public void testMapping() throws Exception {
    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(1);

    Client client = cluster.client();
    createIndex(INDEX);
    Schema schema = createSchema();
    Mapping.createMapping(client, INDEX, TYPE, schema);

    GetMappingsResponse mappingsResponse = client.admin().indices().
        prepareGetMappings(INDEX).
        setTypes(TYPE).get();

    MappingMetaData mappingMetaData = mappingsResponse.getMappings().get(INDEX).get(TYPE);
    assertNotNull(mappingMetaData);

    Map<String, Object> mapping = mappingMetaData.getSourceAsMap();
    verifyMapping(schema, mapping);
  }

  private Schema createSchema() {
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
  private void verifyMapping(Schema schema, Map<String, Object> mapping) throws Exception {
    String schemaName = schema.name();

    Object type = mapping.get("type");
    if (schemaName != null) {
      switch (schemaName) {
        case Date.LOGICAL_NAME:
        case Time.LOGICAL_NAME:
        case Timestamp.LOGICAL_NAME:
          assertEquals(ElasticsearchSinkConnectorConstants.DATE_TYPE, type);
          return;
        case Decimal.LOGICAL_NAME:
          assertEquals(ElasticsearchSinkConnectorConstants.DOUBLE_TYPE, type);
          return;
      }
    }

    Schema.Type schemaType = schema.type();
    switch (schemaType) {
      case ARRAY:
        verifyMapping(schema.valueSchema(), mapping);
        break;
      case MAP:
        Schema newSchema = DataConverter.preProcessSchema(schema);
        verifyMapping(newSchema, mapping);
        break;
      case STRUCT:
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
        for (Field field: schema.fields()) {
          verifyMapping(field.schema(), (Map<String, Object>) properties.get(field.name()));
        }
        break;
      default:
        assertEquals(ElasticsearchSinkConnectorConstants.TYPES.get(schemaType), type);
    }
  }
}
