package io.confluent.connect.elasticsearch_2_4;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.elasticsearch_2_4.type.mapping.DefaultTypeMapper;
import io.confluent.connect.elasticsearch_2_4.type.mapping.TypeMapper;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.TYPE_MAPPER_FIELD;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.TYPE_MAPPER_TYPE;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class TypeMapperTest {

  private Map<String, String> props;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    props = ElasticsearchSinkConnectorConfigTest.addNecessaryProps(props);
  }

  @Test
  public void testDefaultTypeMapperGetTypeForDataStream() throws Exception {
    props.put(TYPE_NAME_CONFIG, "my-type");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    TypeMapper mapper = new DefaultTypeMapper();
    mapper.configure(config);
    String type = mapper.getType("topic-name",null);
    assertEquals("my-type", type);
  }

  @Test
  public void testValueTypeMapperConfig() throws Exception {
    props.put(TYPE_MAPPER_TYPE, "io.confluent.connect.elasticsearch_2_4.type.mapping.ValueTypeMapper");
    props.put(TYPE_MAPPER_FIELD, "top.mid.low");

    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    String json = "{\"top\": { \"mid\": { \"low\": \"my-type\" } } }";

    ObjectMapper mapper = new ObjectMapper();
    JsonNode value = mapper.readTree(json);

    assertEquals(config.getTypeMapper().getClass().getName(), "io.confluent.connect.elasticsearch_2_4.type.mapping.ValueTypeMapper");

    assertEquals(config.getTypeMapper().getType("topic-name", value), "my-type");
  }

  @Test
  public void testValueTypeMapperConfigFailure() throws JsonProcessingException {
    props.put(TYPE_MAPPER_TYPE, "io.confluent.connect.elasticsearch_2_4.type.mapping.ValueTypeMapper");
    props.put(TYPE_MAPPER_FIELD, "top.mid.low");

    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    String json = "{\"top\": { \"mid\": \"my-type\" } }";

    ObjectMapper mapper = new ObjectMapper();
    JsonNode value = mapper.readTree(json);

    assertEquals(config.getTypeMapper().getClass().getName(), "io.confluent.connect.elasticsearch_2_4.type.mapping.ValueTypeMapper");

    Exception exception = assertThrows(Exception.class, () -> config.getTypeMapper().getType("topic-name", value));

    assertEquals("Unable to determine type name for path top.mid.low for value {\"top\":{\"mid\":\"my-type\"}}", exception.getMessage());
  }

}
