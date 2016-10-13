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

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DataConverterTest {

  @Test
  public void primitivePassThrough() {
    assertEquals(Schema.INT64_SCHEMA, DataConverter.preProcessSchema(Schema.INT64_SCHEMA));
    assertEquals(Schema.OPTIONAL_INT64_SCHEMA, DataConverter.preProcessSchema(Schema.OPTIONAL_INT64_SCHEMA));
  }

  @Test
  public void decimal() {
    Schema origSchema = Decimal.schema(2);
    Schema preProcessedSchema = DataConverter.preProcessSchema(origSchema);
    assertEquals(Schema.FLOAT64_SCHEMA, preProcessedSchema);
    assertEquals(0.02, DataConverter.preProcessValue(new BigDecimal("0.02"), origSchema, preProcessedSchema));
  }

  @Test
  public void array() {
    Schema origSchema = SchemaBuilder.array(Decimal.schema(2)).schema();
    Schema preProcessedSchema = DataConverter.preProcessSchema(origSchema);
    assertEquals(SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build(), preProcessedSchema);
    assertEquals(
        Arrays.asList(0.02, 0.42),
        DataConverter.preProcessValue(Arrays.asList(new BigDecimal("0.02"), new BigDecimal("0.42")), origSchema, preProcessedSchema)
    );
  }

  @Test
  public void map() {
    Schema origSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Decimal.schema(2)).build();
    Schema preProcessedSchema = DataConverter.preProcessSchema(origSchema);
    assertEquals(
        SchemaBuilder.array(
            SchemaBuilder.struct().name(Schema.STRING_SCHEMA.type().name() + "-" + Decimal.LOGICAL_NAME)
                .field(ElasticsearchSinkConnectorConstants.MAP_KEY, Schema.STRING_SCHEMA)
                .field(ElasticsearchSinkConnectorConstants.MAP_VALUE, Schema.FLOAT64_SCHEMA)
                .build()
        ).build(),
        preProcessedSchema
    );

    Map<Object, Object> origValue = new HashMap<>();
    origValue.put("field1", new BigDecimal("0.02"));
    origValue.put("field2", new BigDecimal("0.42"));
    assertEquals(
        new HashSet<>(Arrays.asList(
            new Struct(preProcessedSchema.valueSchema())
                .put(ElasticsearchSinkConnectorConstants.MAP_KEY, "field1")
                .put(ElasticsearchSinkConnectorConstants.MAP_VALUE, 0.02),
            new Struct(preProcessedSchema.valueSchema())
                .put(ElasticsearchSinkConnectorConstants.MAP_KEY, "field2")
                .put(ElasticsearchSinkConnectorConstants.MAP_VALUE, 0.42)
        )),
        new HashSet<>((List) DataConverter.preProcessValue(origValue, origSchema, preProcessedSchema))
    );
  }

  @Test
  public void struct() {
    Schema origSchema = SchemaBuilder.struct().name("struct").field("decimal", Decimal.schema(2)).schema();
    Schema preProcessedSchema = DataConverter.preProcessSchema(origSchema);
    assertEquals(
        SchemaBuilder.struct().name("struct").field("decimal", Schema.FLOAT64_SCHEMA).build(),
        preProcessedSchema
    );
    assertEquals(
        new Struct(preProcessedSchema).put("decimal", 0.02),
        DataConverter.preProcessValue(new Struct(origSchema).put("decimal", new BigDecimal("0.02")), origSchema, preProcessedSchema)
    );
  }

}
