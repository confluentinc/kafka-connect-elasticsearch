package io.confluent.connect.elasticsearch_2_4;

import static org.junit.Assert.assertEquals;

import com.google.gson.JsonObject;
import io.confluent.connect.elasticsearch_2_4.DataConverter.BehaviorOnNullValues;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

public class TestUtils {


  public static Schema createSchema() {
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

  private static Schema createInnerSchema() {
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
  public static void verifyMapping(ElasticsearchClient client, Schema schema, JsonObject mapping) throws Exception {
    String schemaName = schema.name();
    Object type = mapping.get("type");
    if (schemaName != null) {
      switch (schemaName) {
        case Date.LOGICAL_NAME:
        case Time.LOGICAL_NAME:
        case Timestamp.LOGICAL_NAME:
          assertEquals("\"" + ElasticsearchSinkConnectorConstants.DATE_TYPE + "\"",
              type.toString());
          return;
        case Decimal.LOGICAL_NAME:
          assertEquals("\"" + ElasticsearchSinkConnectorConstants.DOUBLE_TYPE + "\"",
              type.toString());
          return;
      }
    }

    DataConverter converter = new DataConverter(true, BehaviorOnNullValues.IGNORE);
    Schema.Type schemaType = schema.type();
    switch (schemaType) {
      case ARRAY:
        verifyMapping(client, schema.valueSchema(), mapping);
        break;
      case MAP:
        Schema newSchema = converter.preProcessSchema(schema);
        JsonObject mapProperties = mapping.get("properties").getAsJsonObject();
        verifyMapping(client, newSchema.keySchema(),
            mapProperties.get(ElasticsearchSinkConnectorConstants.MAP_KEY).getAsJsonObject());
        verifyMapping(client, newSchema.valueSchema(),
            mapProperties.get(ElasticsearchSinkConnectorConstants.MAP_VALUE).getAsJsonObject());
        break;
      case STRUCT:
        JsonObject properties = mapping.get("properties").getAsJsonObject();
        for (Field field : schema.fields()) {
          verifyMapping(client, field.schema(), properties.get(field.name()).getAsJsonObject());
        }
        break;
      default:
        assertEquals("\"" + Mapping.getElasticsearchType(client, schemaType) + "\"",
            type.toString());
    }
  }
}
