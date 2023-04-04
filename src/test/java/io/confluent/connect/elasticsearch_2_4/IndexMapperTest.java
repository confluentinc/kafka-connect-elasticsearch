package io.confluent.connect.elasticsearch_2_4;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.elasticsearch_2_4.index.mapping.DefaultIndexMapper;
import io.confluent.connect.elasticsearch_2_4.index.mapping.IndexMapper;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.INDEX_MAPPER_FIELD;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.INDEX_MAPPER_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class IndexMapperTest {

  private Map<String, String> props;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    props = ElasticsearchSinkConnectorConfigTest.addNecessaryProps(props);
  }

  @Test
  public void testDefaultIndexMapperGetIndexForDataStream() throws Exception {
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    IndexMapper indexMapper = new DefaultIndexMapper();
    indexMapper.configure(config);
    String indexName = indexMapper.getIndex("topic-name",null);
    assertEquals(indexName, "topic-name");
  }

  @Test
  public void testDefaultIndexMapperGetIndex() throws Exception {
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    IndexMapper indexMapper = new DefaultIndexMapper();
    indexMapper.configure(config);
    String indexName = indexMapper.getIndex("topic-name",null);
    assertEquals(indexName, "topic-name");
  }

  @Test
  public void testValueIndexMapperConfig() throws Exception {
    props.put(INDEX_MAPPER_TYPE, "io.confluent.connect.elasticsearch_2_4.index.mapping.ValueIndexMapper");
    props.put(INDEX_MAPPER_FIELD, "top.mid.low");

    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    String json = "{\"top\": { \"mid\": { \"low\": \"my-index\" } } }";

    ObjectMapper mapper = new ObjectMapper();
    JsonNode value = mapper.readTree(json);

    assertEquals(config.getIndexMapper().getClass().getName(), "io.confluent.connect.elasticsearch_2_4.index.mapping.ValueIndexMapper");

    assertEquals(config.getIndexMapper().getIndex("topic-name", value), "my-index");
  }

  @Test
  public void testValueIndexMapperConfigFailure() throws JsonProcessingException {
    props.put(INDEX_MAPPER_TYPE, "io.confluent.connect.elasticsearch_2_4.index.mapping.ValueIndexMapper");
    props.put(INDEX_MAPPER_FIELD, "top.mid.low");

    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    String json = "{\"top\": { \"mid\": \"my-index\" } }";

    ObjectMapper mapper = new ObjectMapper();
    JsonNode value = mapper.readTree(json);

    assertEquals(config.getIndexMapper().getClass().getName(), "io.confluent.connect.elasticsearch_2_4.index.mapping.ValueIndexMapper");

    Exception exception = assertThrows(Exception.class, () -> config.getIndexMapper().getIndex("topic-name", value));

    assertEquals("Unable to determine index name for path top.mid.low for value {\"top\":{\"mid\":\"my-index\"}}", exception.getMessage());
  }

}
