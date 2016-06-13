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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DataConverterTest {

  private static final int SCALE = 2;
  private static final Schema DECIMAL_SCHEMA = Decimal.schema(SCALE);

  @Test
  public void testPreProcessSchema() {
    SchemaBuilder builder;
    Schema schema;
    Schema newSchema;

    newSchema = DataConverter.preProcessSchema(DECIMAL_SCHEMA);
    assertEquals(Schema.FLOAT64_SCHEMA, newSchema);

    builder = SchemaBuilder.array(Decimal.schema(SCALE));
    schema = builder.build();
    newSchema = DataConverter.preProcessSchema(schema);
    assertEquals(SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build(), newSchema);

    builder = SchemaBuilder.map(Schema.STRING_SCHEMA, DECIMAL_SCHEMA);
    schema = builder.build();
    newSchema = DataConverter.preProcessSchema(schema);
    String keyName = Schema.STRING_SCHEMA.type().name();
    String valueName = Decimal.LOGICAL_NAME;
    Schema expectedSchema = SchemaBuilder.array(SchemaBuilder.struct().name(keyName + "-" + valueName)
        .field(ElasticsearchSinkConnectorConstants.MAP_KEY, Schema.STRING_SCHEMA)
        .field(ElasticsearchSinkConnectorConstants.MAP_VALUE, Schema.FLOAT64_SCHEMA)
        .build()).build();
    assertEquals(expectedSchema, newSchema);

    builder = SchemaBuilder.struct().name("struct")
        .field("decimal", DECIMAL_SCHEMA);
    schema = builder.schema();
    newSchema = DataConverter.preProcessSchema(schema);
    expectedSchema = SchemaBuilder.struct().name("struct")
        .field("decimal", Schema.FLOAT64_SCHEMA)
        .build();
    assertEquals(expectedSchema, newSchema);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testPreProcessValue() {
    double expectedValue = 0.02;
    byte[] bytes = ByteBuffer.allocate(4).putInt(2).array();
    BigDecimal value = new BigDecimal(new BigInteger(bytes), SCALE);

    SchemaBuilder builder;
    Schema schema;
    Schema newSchema;

    newSchema = DataConverter.preProcessSchema(DECIMAL_SCHEMA);

    Object newValue = DataConverter.preProcessValue(value, DECIMAL_SCHEMA, newSchema);
    assertEquals(expectedValue, newValue);

    builder = SchemaBuilder.array(DECIMAL_SCHEMA);
    schema = builder.build();
    newSchema = DataConverter.preProcessSchema(schema);
    ArrayList<Object> values = new ArrayList<>();
    values.add(value);
    newValue = DataConverter.preProcessValue(values, schema, newSchema);
    List<Object> result = (List<Object>) newValue;
    for (Object element: result) {
      assertEquals(expectedValue, element);
    }

    builder = SchemaBuilder.map(Schema.STRING_SCHEMA, DECIMAL_SCHEMA);
    schema = builder.build();
    newSchema = DataConverter.preProcessSchema(schema);

    Map<Object, Object> original = new HashMap<>();
    original.put("field1", value);
    original.put("field2", value);

    List<Struct> structs = new LinkedList<>();
    Struct expected = new Struct(newSchema.valueSchema());
    expected.put(ElasticsearchSinkConnectorConstants.MAP_KEY, "field1");
    expected.put(ElasticsearchSinkConnectorConstants.MAP_VALUE, 0.02);
    structs.add(expected);

    expected = new Struct(newSchema.valueSchema());
    expected.put(ElasticsearchSinkConnectorConstants.MAP_KEY, "field2");
    expected.put(ElasticsearchSinkConnectorConstants.MAP_VALUE, 0.02);
    structs.add(expected);

    assertEquals(new HashSet<>(structs), new HashSet<>((List<?>) DataConverter.preProcessValue(original, schema, newSchema)));
  }
}
