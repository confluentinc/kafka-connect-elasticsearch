package io.confluent.connect.elasticsearch;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ElasticsearchSinkConnectorConfigTest {

  private Map<String, String> props;

  @Before
  public void setup() {
    props = new HashMap<>();
    props.put(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG, ElasticsearchSinkTestBase.TYPE);
    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "localhost");
    props.put(ElasticsearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true");
  }

  @Test
  public void testDefaultHttpTimeoutsConfig() {
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    Assert.assertEquals(
        config.getInt(ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG),
        (Integer) 3000
    );
    Assert.assertEquals(
        config.getInt(ElasticsearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG),
        (Integer) 1000
    );
  }

  @Test
  public void testSetHttpTimeoutsConfig() {
    props.put(ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG, "10000");
    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG, "15000");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    Assert.assertEquals(
        config.getInt(ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG),
        (Integer) 10000
    );
    Assert.assertEquals(
        config.getInt(ElasticsearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG),
        (Integer) 15000
    );
  }
}
