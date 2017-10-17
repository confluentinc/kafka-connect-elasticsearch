package io.confluent.connect.elasticsearch;

import com.google.gson.JsonObject;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.nio.ByteBuffer;

public class MappingWithDefaultValuesTest extends ElasticsearchSinkTestBase {

  private static final String INDEX = "kafka-connect";
  private static final String TYPE = "kafka-connect-type";

  @Test
  public void testDefaultValues() throws Exception {
    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(1);

    createIndex(INDEX);
    Schema schema = createSchema();
    Mapping.createMapping(client, INDEX, TYPE, schema);

    JsonObject mapping = Mapping.getMapping(client, INDEX, TYPE);
    verifyResult(mapping);
  }

  private void verifyResult(JsonObject mapping) {
    verifyBoolean(mapping);
    verifyInt8(mapping);
    verifyInt16(mapping);
    verifyInt32(mapping);
    verifyInt64(mapping);
    verifyFloat32(mapping);
    verifyFloat64(mapping);
    verifyString(mapping);
    verifyDate(mapping);
  }

  private void verifyDate(JsonObject mapping) {
    assertEquals(mapping.get("properties").getAsJsonObject()
        .get("date").getAsJsonObject()
        .get("null_value").getAsLong(), 42);
  }

  private void verifyString(JsonObject mapping) {
    assertEquals(mapping.get("properties").getAsJsonObject()
        .get("string").getAsJsonObject()
        .get("null_value").getAsString(), "foo");
  }

  private void verifyFloat64(JsonObject mapping) {
    assertEquals(mapping.get("properties").getAsJsonObject()
        .get("float64").getAsJsonObject()
        .get("null_value").getAsDouble(), 42d, 1);
  }

  private void verifyFloat32(JsonObject mapping) {
    assertEquals(mapping.get("properties").getAsJsonObject()
        .get("float32").getAsJsonObject()
        .get("null_value").getAsFloat(), 42f, 1);
  }

  private void verifyInt64(JsonObject mapping) {
    assertEquals(mapping.get("properties").getAsJsonObject()
        .get("int64").getAsJsonObject()
        .get("null_value").getAsLong(), 42L);
  }

  private void verifyInt32(JsonObject mapping) {
    assertEquals(mapping.get("properties").getAsJsonObject()
        .get("int32").getAsJsonObject()
        .get("null_value").getAsInt(), 42);
  }

  private void verifyInt16(JsonObject mapping) {
    assertEquals(mapping.get("properties").getAsJsonObject()
        .get("int16").getAsJsonObject()
        .get("null_value").getAsShort(), (short) 42);
  }

  private void verifyInt8(JsonObject mapping) {
    assertEquals(mapping.get("properties").getAsJsonObject()
        .get("int8").getAsJsonObject()
        .get("null_value").getAsByte(), (byte) 42);
  }

  private void verifyBoolean(JsonObject mapping) {
    assertEquals(mapping.get("properties").getAsJsonObject()
        .get("boolean").getAsJsonObject()
        .get("null_value").getAsBoolean(), false);
  }

  protected Schema createSchema() {
    return SchemaBuilder.struct().name("record")
        .field("boolean", SchemaBuilder.bool().defaultValue(false).build())
        .field("bytes", SchemaBuilder.bytes().defaultValue(new byte[]{1}))
        .field("byte_buffer", SchemaBuilder.bytes().defaultValue(ByteBuffer.wrap("bar".getBytes())))
        .field("int8", SchemaBuilder.int8().defaultValue((byte) 42).build())
        .field("int16", SchemaBuilder.int16().defaultValue((short) 42).build())
        .field("int32", SchemaBuilder.int32().defaultValue(42).build())
        .field("int64", SchemaBuilder.int64().defaultValue(42L).build())
        .field("float32", SchemaBuilder.float32().defaultValue(42.0f).build())
        .field("float64", SchemaBuilder.float64().defaultValue(42.0d).build())
        .field("string", SchemaBuilder.string().defaultValue("foo").build())
        .field("date", Date.builder().defaultValue(new java.util.Date(42)).build())
        .build();
  }
}
