package io.confluent.connect.elasticsearch;

import io.confluent.connect.elasticsearch.index.mapping.DefaultIndexMapper;
import io.confluent.connect.elasticsearch.index.mapping.IndexMapper;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_DATASET_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_TYPE_CONFIG;
import static org.junit.Assert.assertEquals;

public class IndexMapperTest {

  private Map<String, String> props;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    props = ElasticsearchSinkConnectorConfigTest.addNecessaryProps(new HashMap<>());

  }

  @Test
  public void testDefaultIndexMapperGetIndexForDataStream(){
    props.put(DATA_STREAM_TYPE_CONFIG, "LOGS");
    props.put(DATA_STREAM_DATASET_CONFIG, "dataset");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    IndexMapper indexMapper = new DefaultIndexMapper();
    indexMapper.configure(config);
    String indexName = indexMapper.getIndex("topic-name",null);
    assertEquals(indexName, "logs-dataset-topic-name");
  }

  @Test
  public void testDefaultIndexMapperGetIndex(){
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    IndexMapper indexMapper = new DefaultIndexMapper();
    indexMapper.configure(config);
    String indexName = indexMapper.getIndex("topic-name",null);
    assertEquals(indexName, "topic-name");
  }
}
