package io.confluent.connect.elasticsearch;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SECURITY_PROTOCOL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SSL_CONFIG_PREFIX;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ElasticsearchSinkConnectorConfigTest {

  private Map<String, String> props;

  @Before
  public void setup() {
    props = new HashMap<>();
    props.put(TYPE_NAME_CONFIG, ElasticsearchSinkTestBase.TYPE);
    props.put(CONNECTION_URL_CONFIG, "localhost");
    props.put(IGNORE_KEY_CONFIG, "true");
  }

  @Test
  public void testDefaultHttpTimeoutsConfig() {
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    assertEquals(config.readTimeoutMs(), 3000);
    assertEquals(config.connectionTimeoutMs(), 1000);
  }

  @Test
  public void testSetHttpTimeoutsConfig() {
    props.put(READ_TIMEOUT_MS_CONFIG, "10000");
    props.put(CONNECTION_TIMEOUT_MS_CONFIG, "15000");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    assertEquals(config.readTimeoutMs(), 10000);
    assertEquals(config.connectionTimeoutMs(), 15000);
  }

  @Test
  public void testSslConfigs() {
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/path");
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "opensesame");
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/path2");
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "opensesame2");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    Map<String, Object> sslConfigs = config.sslConfigs();
    assertTrue(sslConfigs.size() > 0);
    assertEquals(
        new Password("opensesame"),
        sslConfigs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)
    );
    assertEquals(
        new Password("opensesame2"),
        sslConfigs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
    );
    assertEquals("/path", sslConfigs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    assertEquals("/path2", sslConfigs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
  }

  @Test
  public void testSecured() {
    props.put(CONNECTION_URL_CONFIG, "http://host:9999");
    assertFalse(new ElasticsearchSinkConnectorConfig(props).secured());

    props.put(CONNECTION_URL_CONFIG, "https://host:9999");
    assertFalse(new ElasticsearchSinkConnectorConfig(props).secured());

    props.put(CONNECTION_URL_CONFIG, "http://host1:9992,https://host:9999");
    assertFalse(new ElasticsearchSinkConnectorConfig(props).secured());

    // Default behavior should be backwards compat
    props.put(CONNECTION_URL_CONFIG, "host1:9992");
    assertFalse(new ElasticsearchSinkConnectorConfig(props).secured());

    props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
    assertTrue(new ElasticsearchSinkConnectorConfig(props).secured());

    props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name());
    props.put(CONNECTION_URL_CONFIG, "https://host:9999");
    assertFalse(new ElasticsearchSinkConnectorConfig(props).secured());
  }
}
