package io.confluent.connect.elasticsearch;

import org.apache.kafka.common.config.types.Password;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

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

  @Test
  public void testSslConfigs() {
    props.put("elastic.https.ssl.keystore.location", "/path");
    props.put("elastic.https.ssl.keystore.password", "opensesame");
    props.put("elastic.https.ssl.truststore.location", "/path2");
    props.put("elastic.https.ssl.truststore.password", "opensesame2");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    Map<String, Object> sslConfigs = config.sslConfigs();
    Assert.assertTrue(sslConfigs.size() > 0);
    Assert.assertEquals(
        new Password("opensesame"),
        sslConfigs.get("ssl.keystore.password")
    );
    Assert.assertEquals(
        new Password("opensesame2"),
        sslConfigs.get("ssl.truststore.password")
    );
    Assert.assertEquals("/path", sslConfigs.get("ssl.keystore.location"));
    Assert.assertEquals("/path2", sslConfigs.get("ssl.truststore.location"));
  }

  @Test
  public void testSecured() {
    props.put("connection.url", "http://host:9999");
    assertFalse(new ElasticsearchSinkConnectorConfig(props).secured());

    props.put("connection.url", "https://host:9999");
    assertFalse(new ElasticsearchSinkConnectorConfig(props).secured());

    props.put("connection.url", "http://host1:9992,https://host:9999");
    assertFalse(new ElasticsearchSinkConnectorConfig(props).secured());

    // Default behavior should be backwards compat
    props.put("connection.url", "host1:9992");
    assertFalse(new ElasticsearchSinkConnectorConfig(props).secured());

    props.put("elastic.security.protocol", SecurityProtocol.SSL.name());
    assertTrue(new ElasticsearchSinkConnectorConfig(props).secured());

    props.put("elastic.security.protocol", SecurityProtocol.PLAINTEXT.name());
    props.put("connection.url", "https://host:9999");
    assertFalse(new ElasticsearchSinkConnectorConfig(props).secured());
  }
}
