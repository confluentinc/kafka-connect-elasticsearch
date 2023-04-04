package io.confluent.connect.elasticsearch_2_4;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.elasticsearch_2_4.cluster.mapping.ClusterMapper;
import io.confluent.connect.elasticsearch_2_4.cluster.mapping.DefaultClusterMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.CLUSTER_MAPPER_FIELD;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.CLUSTER_MAPPER_TYPE;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.CLUSTER_MAP_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
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
    String cluster = clusterMapper.getName(null);
    assertEquals(cluster, "default");
  }

  @Test
  public void testDefaultClusterMapperGetMultiUrlCluster() throws Exception {
    String url = "http://localhost:9200";
    String url2 = "http://localhost:9201";
    props.put(CONNECTION_URL_CONFIG, url + "," + url2);
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    ClusterMapper clusterMapper = new DefaultClusterMapper();
    clusterMapper.configure(config);
    String cluster = clusterMapper.getName(null);
    assertEquals(cluster, "default");
  }

  @Test
  public void testValueClusterMapperConfig() throws Exception {
    props.put(CLUSTER_MAPPER_TYPE, "io.confluent.connect.elasticsearch_2_4.cluster.mapping.ValueClusterMapper");
    props.put(CLUSTER_MAPPER_FIELD, "top.mid.low");
    props.put(CLUSTER_MAP_CONFIG, "my-cluster->http://localhost:9200,http://localhost:9201");

    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    String json = "{\"top\": { \"mid\": { \"low\": \"my-cluster\" } } }";

    ObjectMapper mapper = new ObjectMapper();
    JsonNode value = mapper.readTree(json);

    assertEquals(config.getClusterMapper().getClass().getName(), "io.confluent.connect.elasticsearch_2_4.cluster.mapping.ValueClusterMapper");

    assertEquals(config.getClusterMapper().getName(value), "my-cluster");
  }

  @Test
  public void testValueClusterMapperComplexConfig() throws Exception {
    props.put(CLUSTER_MAPPER_TYPE, "io.confluent.connect.elasticsearch_2_4.cluster.mapping.ValueClusterMapper");
    props.put(CLUSTER_MAPPER_FIELD, "top.mid.low");
    props.put(CLUSTER_MAP_CONFIG, "my-cluster->http://localhost:9200,http://localhost:9201;my-cluster-2->http://localhost:9202,http://localhost:9203");

    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    ObjectMapper mapper = new ObjectMapper();

    String json = "{\"top\": { \"mid\": { \"low\": \"my-cluster\" } } }";

    JsonNode value = mapper.readTree(json);

    assertEquals(config.getClusterMapper().getName(value), "my-cluster");

    String json2 = "{\"top\": { \"mid\": { \"low\": \"my-cluster-2\" } } }";

    JsonNode value2 = mapper.readTree(json2);

    assertEquals(config.getClusterMapper().getName(value2), "my-cluster-2");
  }

  @Test
  public void testValueClusterMapperGetConfig() throws Exception {
    props.put(CLUSTER_MAPPER_TYPE, "io.confluent.connect.elasticsearch_2_4.cluster.mapping.ValueClusterMapper");
    props.put(CLUSTER_MAPPER_FIELD, "top.mid.low");
    props.put(CLUSTER_MAP_CONFIG, "my-cluster->http://localhost:9200,http://localhost:9201;my-cluster-2->http://localhost:9202,http://localhost:9203");

    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    Assert.assertArrayEquals(new String[]{"http://localhost:9200","http://localhost:9201"}, config.getClusterMapper().getClusterUrl("my-cluster").toArray());

    Assert.assertArrayEquals(new String[]{"http://localhost:9202","http://localhost:9203"}, config.getClusterMapper().getClusterUrl("my-cluster-2").toArray());
  }

  @Test
  public void testValueClusterMapperGetAllClusterConfig() throws Exception {
    props.put(CLUSTER_MAPPER_TYPE, "io.confluent.connect.elasticsearch_2_4.cluster.mapping.ValueClusterMapper");
    props.put(CLUSTER_MAPPER_FIELD, "top.mid.low");
    props.put(CLUSTER_MAP_CONFIG, "my-cluster->http://localhost:9200,http://localhost:9201;my-cluster-2->http://localhost:9202,http://localhost:9203");

    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    Map<String, Set<String>> clusters = config.getClusterMapper().getAllClusters();

    Assert.assertArrayEquals(new String[]{"http://localhost:9200","http://localhost:9201"}, clusters.get("my-cluster").toArray());

    Assert.assertArrayEquals(new String[]{"http://localhost:9202","http://localhost:9203"}, clusters.get("my-cluster-2").toArray());
  }

  @Test
  public void testValueIndexMapperConfigFailure() throws JsonProcessingException {
    props.put(CLUSTER_MAPPER_TYPE, "io.confluent.connect.elasticsearch_2_4.cluster.mapping.ValueClusterMapper");
    props.put(CLUSTER_MAPPER_FIELD, "top.mid.low");
    props.put(CLUSTER_MAP_CONFIG, "my-cluster->http://localhost:9200");

    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    String json = "{\"top\": { \"mid\": \"my-cluster\" } }";

    ObjectMapper mapper = new ObjectMapper();
    JsonNode value = mapper.readTree(json);

    assertEquals(config.getClusterMapper().getClass().getName(), "io.confluent.connect.elasticsearch_2_4.cluster.mapping.ValueClusterMapper");

    Exception exception = assertThrows(Exception.class, () -> config.getClusterMapper().getName(value));

    assertEquals("Unable to determine cluster name for path top.mid.low for value {\"top\":{\"mid\":\"my-cluster\"}}", exception.getMessage());
  }

}
