package io.confluent.connect.elasticsearch;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.*;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SecurityProtocol;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.junit.Before;
import org.junit.Test;

public class ElasticsearchSinkConnectorConfigTest {

  private Map<String, String> props;

  @Before
  public void setup() {
    props = addNecessaryProps(new HashMap<>());
  }

  @Test
  public void testDefaultHttpTimeoutsConfig() {
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    assertEquals(config.readTimeoutMs(), 3000);
    assertEquals(config.connectionTimeoutMs(), 1000);
  }

  @Test
  public void testDefaultFlushSynchronously() {
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    assertEquals(false, config.flushSynchronously());
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
  public void shouldAllowValidChractersDataStreamNamespace() {
    props.put(DATA_STREAM_NAMESPACE_CONFIG, "a_valid.namespace123");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test
  public void shouldAllowValidChractersDataStreamDataset() {
    props.put(DATA_STREAM_DATASET_CONFIG, "a_valid.dataset123");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test
  public void shouldAllowValidDataStreamType() {
    props.put(DATA_STREAM_TYPE_CONFIG, "metrics");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test
  public void shouldAllowValidDataStreamTypeCaseInsensitive() {
    props.put(DATA_STREAM_TYPE_CONFIG, "mEtRICS");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowInvalidCaseDataStreamNamespace() {
    props.put(DATA_STREAM_NAMESPACE_CONFIG, "AN_INVALID.namespace123");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowInvalidCaseDataStreamDataset() {
    props.put(DATA_STREAM_DATASET_CONFIG, "AN_INVALID.dataset123");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowInvalidCharactersDataStreamNamespace() {
    props.put(DATA_STREAM_NAMESPACE_CONFIG, "not-valid?");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowInvalidCharactersDataStreamDataset() {
    props.put(DATA_STREAM_DATASET_CONFIG, "not-valid?");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test
  public void shouldAllowCustomDataStreamType() {
    props.put(DATA_STREAM_TYPE_CONFIG, "notLogOrMetrics");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowLongDataStreamNamespace() {
    props.put(DATA_STREAM_NAMESPACE_CONFIG, String.format("%d%100d", 1, 1));
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowLongDataStreamDataset() {
    props.put(DATA_STREAM_DATASET_CONFIG, String.format("%d%100d", 1, 1));
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowNullUrlList(){
    props.put(CONNECTION_URL_CONFIG, null);
    new ElasticsearchSinkConnectorConfig(props);
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
    assertFalse(new ElasticsearchSinkConnectorConfig(props).isSslEnabled());

    props.put(CONNECTION_URL_CONFIG, "https://host:9999");
    assertFalse(new ElasticsearchSinkConnectorConfig(props).isSslEnabled());

    props.put(CONNECTION_URL_CONFIG, "http://host1:9992,https://host:9999");
    assertFalse(new ElasticsearchSinkConnectorConfig(props).isSslEnabled());

    // Default behavior should be backwards compat
    props.put(CONNECTION_URL_CONFIG, "host1:9992");
    assertFalse(new ElasticsearchSinkConnectorConfig(props).isSslEnabled());

    props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
    assertTrue(new ElasticsearchSinkConnectorConfig(props).isSslEnabled());

    props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name());
    props.put(CONNECTION_URL_CONFIG, "https://host:9999");
    assertFalse(new ElasticsearchSinkConnectorConfig(props).isSslEnabled());
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
    props.put(PROXY_PORT_CONFIG, "1010");
    props.put(PROXY_USERNAME_CONFIG, "username");
    props.put(PROXY_PASSWORD_CONFIG, "password");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    assertNotNull(config);
    assertTrue(config.isBasicProxyConfigured());
    assertTrue(config.isProxyWithAuthenticationConfigured());
    assertEquals("proxy host", config.proxyHost());
    assertEquals(1010, config.proxyPort());
    assertEquals("username", config.proxyUsername());
    assertEquals("password", config.proxyPassword().value());
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowInvalidProxyPort() {
    props.put(PROXY_PORT_CONFIG, "-666");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowInvalidUrl() {
    props.put(CONNECTION_URL_CONFIG, ".com:/bbb/dfs,http://valid.com");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowInvalidSecurityProtocol() {
    props.put(SECURITY_PROTOCOL_CONFIG, "unsecure");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test
  public void shouldDisableHostnameVerification() {
    props.put(SSL_CONFIG_PREFIX + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    assertFalse(config.shouldDisableHostnameVerification());

    props.put(SSL_CONFIG_PREFIX + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    config = new ElasticsearchSinkConnectorConfig(props);
    assertTrue(config.shouldDisableHostnameVerification());

    props.put(SSL_CONFIG_PREFIX + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, null);
    config = new ElasticsearchSinkConnectorConfig(props);
    assertFalse(config.shouldDisableHostnameVerification());
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowInvalidExtensionKeytab() {
    props.put(KERBEROS_KEYTAB_PATH_CONFIG, "keytab.wrongextension");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowNonExistingKeytab() {
    props.put(KERBEROS_KEYTAB_PATH_CONFIG, "idontexist.keytab");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test
  public void shouldAllowValidKeytab() throws IOException {
    Path keytab = Files.createTempFile("iexist", ".keytab");
    props.put(KERBEROS_KEYTAB_PATH_CONFIG, keytab.toString());

    new ElasticsearchSinkConnectorConfig(props);

    keytab.toFile().delete();
  }

  @Test
  public void testDefaultMaxExternalResourceMappings() {
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    assertEquals(15, config.maxExternalResourceMappings());
  }

  @Test
  public void testCustomMaxExternalResourceMappings() {
    props.put(MAX_EXTERNAL_RESOURCE_MAPPINGS_CONFIG, "25");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    assertEquals(25, config.maxExternalResourceMappings());
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowZeroMaxExternalResourceMappings() {
    props.put(MAX_EXTERNAL_RESOURCE_MAPPINGS_CONFIG, "0");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowNegativeMaxExternalResourceMappings() {
    props.put(MAX_EXTERNAL_RESOURCE_MAPPINGS_CONFIG, "-1");
    new ElasticsearchSinkConnectorConfig(props);
  }

  @Test
  public void shouldAllowValidTopicToExternalResourceMapping() {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, "topic1:index1,topic2:index2");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

    assertNotNull(config);
    Map<String, String> mappings = config.getTopicToExternalResourceMap();
    assertEquals(2, mappings.size());
    assertEquals("index1", mappings.get("topic1"));
    assertEquals("index2", mappings.get("topic2"));
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowTooManyTopicToExternalResourceMappings() {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(MAX_EXTERNAL_RESOURCE_MAPPINGS_CONFIG, "2");
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, "topic1:index1,topic2:index2,topic3:index3");
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    // Trigger validation by calling the method
    config.getTopicToExternalResourceMap();
  }

  @Test(expected = ConfigException.class)
  public void shouldNotAllowTooManyTopicToExternalResourceMappingsWithDefaultLimit() {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    // Create 16 mappings (exceeds default limit of 15)
    StringBuilder mappings = new StringBuilder();
    for (int i = 1; i <= 16; i++) {
      if (i > 1) mappings.append(",");
      mappings.append("topic").append(i).append(":index").append(i);
    }
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, mappings.toString());
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
    // Trigger validation by calling the method
    config.getTopicToExternalResourceMap();
  }

  public static Map<String, String> addNecessaryProps(Map<String, String> props) {
    if (props == null) {
      props = new HashMap<>();
    }
    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost:8080");
    return props;
  }
}
