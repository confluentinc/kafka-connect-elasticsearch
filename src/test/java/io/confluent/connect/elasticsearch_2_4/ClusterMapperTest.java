package io.confluent.connect.elasticsearch_2_4;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.elasticsearch_2_4.cluster.mapping.ClusterMapper;
import io.confluent.connect.elasticsearch_2_4.cluster.mapping.DefaultClusterMapper;
import io.confluent.connect.elasticsearch_2_4.index.mapping.DefaultIndexMapper;
import io.confluent.connect.elasticsearch_2_4.index.mapping.IndexMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.CLUSTER_MAPPER_FIELD;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.CLUSTER_MAPPER_TYPE;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.INDEX_MAPPER_FIELD;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.INDEX_MAPPER_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ClusterMapperTest {

  private Map<String, String> props;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    props = ElasticsearchSinkConnectorConfigTest.addNecessaryProps(props);
  }

  @Test
  public void testDefaultClusterMapperGetCluster() throws Exception {
    String url = "http://localhost:9200";
    props.put(CONNECTION_URL_CONFIG, url);
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    ClusterMapper clusterMapper = new DefaultClusterMapper();
    clusterMapper.configure(config);
    Set<String> cluster = clusterMapper.getCluster("topic-name",null);
    assertEquals(cluster.toArray()[0], url);
  }

  @Test
  public void testDefaultClusterMapperGetMultiUrlCluster() throws Exception {
    String url = "http://localhost:9200";
    String url2 = "http://localhost:9201";
    props.put(CONNECTION_URL_CONFIG, url + "," + url2);
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    ClusterMapper clusterMapper = new DefaultClusterMapper();
    clusterMapper.configure(config);
    Set<String> cluster = clusterMapper.getCluster("topic-name",null);
    assertEquals(cluster.size(), 2);
    Assert.assertArrayEquals(cluster.toArray(), new String[]{url, url2});
    assertEquals(cluster.toArray()[0], url);
  }

  @Test
  public void testValueClusterMapperConfig() throws Exception {
    props.put(CLUSTER_MAPPER_TYPE, "io.confluent.connect.elasticsearch_2_4.cluster.mapping.ValueClusterMapper");
    props.put(CLUSTER_MAPPER_FIELD, "top.mid.low");

    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    String json = "{\"top\": { \"mid\": { \"low\": \"my-cluster\" } } }";

    ObjectMapper mapper = new ObjectMapper();
    JsonNode value = mapper.readTree(json);

    assertEquals(config.getClusterMapper().getClass().getName(), "io.confluent.connect.elasticsearch_2_4.cluster.mapping.ValueClusterMapper");

    assertEquals(config.getClusterMapper().getCluster("topic-name", value).toArray()[0], "my-cluster");
  }

  @Test
  public void testValueIndexMapperConfigFailure() throws JsonProcessingException {
    props.put(CLUSTER_MAPPER_TYPE, "io.confluent.connect.elasticsearch_2_4.cluster.mapping.ValueClusterMapper");
    props.put(CLUSTER_MAPPER_FIELD, "top.mid.low");

    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    String json = "{\"top\": { \"mid\": \"my-cluster\" } }";

    ObjectMapper mapper = new ObjectMapper();
    JsonNode value = mapper.readTree(json);

    assertEquals(config.getClusterMapper().getClass().getName(), "io.confluent.connect.elasticsearch_2_4.cluster.mapping.ValueClusterMapper");

    Exception exception = assertThrows(Exception.class, () -> config.getClusterMapper().getCluster("topic-name", value));

    assertEquals("Unable to determine cluster name for path top.mid.low for value {\"top\":{\"mid\":\"my-cluster\"}}", exception.getMessage());
  }

}
