package io.confluent.connect.elasticsearch;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.PROXY_HOST_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.PROXY_PASSWORD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.PROXY_PORT_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.PROXY_USERNAME_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.common.config.ConfigException;
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
    props.put("elastic.https.ssl.keystore.location", "/path");
    props.put("elastic.https.ssl.keystore.password", "opensesame");
    props.put("elastic.https.ssl.truststore.location", "/path2");
    props.put("elastic.https.ssl.truststore.password", "opensesame2");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    Map<String, Object> sslConfigs = config.sslConfigs();
    assertTrue(sslConfigs.size() > 0);
    assertEquals(
        new Password("opensesame"),
        sslConfigs.get("ssl.keystore.password")
    );
    assertEquals(
        new Password("opensesame2"),
        sslConfigs.get("ssl.truststore.password")
    );
    assertEquals("/path", sslConfigs.get("ssl.keystore.location"));
    assertEquals("/path2", sslConfigs.get("ssl.truststore.location"));
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

  @Test
  public void shouldAcceptValidBasicProxy() {
    props.put(PROXY_HOST_CONFIG, "proxy host");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    assertNotNull(config);
    assertTrue(config.isBasicProxyConfigured());
    assertFalse(config.isProxyWithAuthenticationConfigured());
  }

  @Test
  public void shouldAcceptValidProxyWithAuthentication() {
    props.put(PROXY_HOST_CONFIG, "proxy host");
    props.put(PROXY_USERNAME_CONFIG, "username");
    props.put(PROXY_PASSWORD_CONFIG, "password");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    assertNotNull(config);
    assertTrue(config.isBasicProxyConfigured());
    assertTrue(config.isProxyWithAuthenticationConfigured());
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowInvalidProxyPort() {
    props.put(PROXY_PORT_CONFIG, "-666");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowProxyCredentialsWithoutProxy() {
    props.put(PROXY_USERNAME_CONFIG, "username");
    props.put(PROXY_PASSWORD_CONFIG, "password");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowPartialProxyCredentials() {
    props.put(PROXY_HOST_CONFIG, "proxy host");
    props.put(PROXY_USERNAME_CONFIG, "username");
    new ElasticsearchSinkConnectorConfig(props);
  }
}
