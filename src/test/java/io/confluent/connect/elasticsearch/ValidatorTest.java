/**
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.elasticsearch;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_DATASET_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_TYPE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_TOPICS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_TOPICS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.KERBEROS_KEYTAB_PATH_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.PROXY_HOST_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.PROXY_PASSWORD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.PROXY_USERNAME_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SECURITY_PROTOCOL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SSL_CONFIG_PREFIX;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.KERBEROS_PRINCIPAL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WRITE_METHOD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.EXTERNAL_RESOURCE_USAGE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.ExternalResourceUsage;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.INVALID_MAPPING_FORMAT_ERROR;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DUPLICATE_TOPIC_MAPPING_ERROR_FORMAT;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DUPLICATE_RESOURCE_MAPPING_ERROR_FORMAT;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.TOO_MANY_MAPPINGS_ERROR_FORMAT;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_EXTERNAL_RESOURCE_MAPPINGS_CONFIG;
import static io.confluent.connect.elasticsearch.Validator.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SecurityProtocol;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.SslConfigs;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;

public class ValidatorTest {

  private static final String DELETE_BEHAVIOR = "delete";
  private static final String UPSERT_METHOD = "upsert";
  private static final String TOPICS_CONFIG_KEY = "topics";
  private static final String TOPIC1 = "topic1";
  private static final String TOPIC2 = "topic2";
  private static final String TOPIC3 = "topic3";

  // Resource names
  private static final String INDEX1 = "index1";
  private static final String INDEX2 = "index2";
  private static final String ALIAS1 = "alias1";
  private static final String LOGS_TEST_1 = "logs-test-1";
  private static final String VALID_DATASET = "a_valid_dataset";

  private MainResponse mockInfoResponse;
  private Map<String, String> props;
  private RestHighLevelClient mockClient;
  private Validator validator;

  @Before
  public void setup() throws IOException {
    props = ElasticsearchSinkConnectorConfigTest.addNecessaryProps(new HashMap<>());

    mockClient = mock(RestHighLevelClient.class, Mockito.RETURNS_DEEP_STUBS);
    when(mockClient.ping(any(RequestOptions.class))).thenReturn(true);
    mockInfoResponse = mock(MainResponse.class, Mockito.RETURNS_DEEP_STUBS);
    when(mockClient.info(any(RequestOptions.class))).thenReturn(mockInfoResponse);
    when(mockInfoResponse.getVersion().getNumber()).thenReturn("7.9.3");
  }

  @Test
  public void testValidDefaultConfig() {
    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidIndividualConfigs() {
    validator = new Validator(new HashMap<>(), () -> mockClient);
    Config result = validator.validate();
    assertHasErrorMessage(result, CONNECTION_URL_CONFIG, "Missing required configuration");
  }

  @Test
  public void testValidUpsertDeleteOnDefaultConfig() {
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, DELETE_BEHAVIOR);
    props.put(WRITE_METHOD_CONFIG, UPSERT_METHOD);
    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidCredentials() {
    props.put(CONNECTION_USERNAME_CONFIG, "username");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, CONNECTION_USERNAME_CONFIG, "must be set");
    assertHasErrorMessage(result, CONNECTION_PASSWORD_CONFIG, "must be set");
    props.remove(CONNECTION_USERNAME_CONFIG);

    props.put(CONNECTION_PASSWORD_CONFIG, "password");
    validator = new Validator(props);
    result = validator.validate();
    assertHasErrorMessage(result, CONNECTION_USERNAME_CONFIG, "must be set");
    assertHasErrorMessage(result, CONNECTION_PASSWORD_CONFIG, "must be set");
  }

  @Test
  public void testClientThrowsElasticsearchStatusException() throws IOException {
    when(mockClient.ping(any(RequestOptions.class))).thenThrow(new ElasticsearchStatusException("Deleted resource.", RestStatus.GONE));
    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertHasErrorMessage(result, CONNECTION_URL_CONFIG, "Could not connect to Elasticsearch. Error message: Deleted resource.");
  }

  @Test
  public void testValidCredentials() {
    // username and password not set
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);

    // both set
    props.put(CONNECTION_USERNAME_CONFIG, "username");
    props.put(CONNECTION_PASSWORD_CONFIG, "password");
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidMissingOneDataStreamConfig() {
    props.put(DATA_STREAM_DATASET_CONFIG, VALID_DATASET);
    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertHasErrorMessage(result, DATA_STREAM_DATASET_CONFIG, "must be set");
    assertHasErrorMessage(result, DATA_STREAM_TYPE_CONFIG, "must be set");
  }

  @Test
  public void testInvalidUpsertOnValidDataStreamConfigs() {
    props.put(DATA_STREAM_DATASET_CONFIG, VALID_DATASET);
    props.put(DATA_STREAM_TYPE_CONFIG, "logs");
    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertNoErrors(result);

    props.put(WRITE_METHOD_CONFIG, UPSERT_METHOD);
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertHasErrorMessage(result, WRITE_METHOD_CONFIG, UPSERT_NOT_ALLOWED_WITH_DATASTREAM_ERROR);
  }

  @Test
  public void testInvalidDeleteOnValidDataStreamConfigs() {
    props.put(DATA_STREAM_DATASET_CONFIG, VALID_DATASET);
    props.put(DATA_STREAM_TYPE_CONFIG, "logs");
    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertNoErrors(result);

    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, DELETE_BEHAVIOR);
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertHasErrorMessage(result, BEHAVIOR_ON_NULL_VALUES_CONFIG, DELETE_NOT_ALLOWED_WITH_DATASTREAM_ERROR);
  }

  @Test
  public void testInvalidIgnoreConfigs() {
    props.put(IGNORE_KEY_CONFIG, "true");
    props.put(IGNORE_KEY_TOPICS_CONFIG, "some,topics");
    props.put(IGNORE_SCHEMA_CONFIG, "true");
    props.put(IGNORE_SCHEMA_TOPICS_CONFIG, "some,other,topics");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, IGNORE_KEY_CONFIG, "is true");
    assertHasErrorMessage(result, IGNORE_KEY_TOPICS_CONFIG, "is true");
    assertHasErrorMessage(result, IGNORE_SCHEMA_CONFIG, "is true");
    assertHasErrorMessage(result, IGNORE_SCHEMA_TOPICS_CONFIG, "is true");
  }

  @Test
  public void testValidIgnoreConfigs() {
    // topics configs not set
    props.put(IGNORE_KEY_CONFIG, "true");
    props.put(IGNORE_SCHEMA_CONFIG, "true");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);

    // ignore configs are false
    props.put(IGNORE_KEY_CONFIG, "false");
    props.put(IGNORE_KEY_TOPICS_CONFIG, "some,topics");
    props.put(IGNORE_SCHEMA_CONFIG, "false");
    props.put(IGNORE_SCHEMA_TOPICS_CONFIG, "some,other,topics");
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidKerberos() throws IOException {
    props.put(KERBEROS_PRINCIPAL_CONFIG, "principal");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, KERBEROS_PRINCIPAL_CONFIG, "must be set");
    assertHasErrorMessage(result, KERBEROS_KEYTAB_PATH_CONFIG, "must be set");

    // proxy
    Path keytab = Files.createTempFile("es", ".keytab");
    props.put(KERBEROS_PRINCIPAL_CONFIG, "principal");
    props.put(KERBEROS_KEYTAB_PATH_CONFIG, keytab.toString());
    props.put(PROXY_HOST_CONFIG, "proxy.com");
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertHasErrorMessage(result, KERBEROS_PRINCIPAL_CONFIG, "not supported with proxy settings");
    assertHasErrorMessage(result, KERBEROS_KEYTAB_PATH_CONFIG, "not supported with proxy settings");
    assertHasErrorMessage(result, PROXY_HOST_CONFIG, "not supported with proxy settings");

    // basic credentials
    props.remove(PROXY_HOST_CONFIG);
    props.put(CONNECTION_USERNAME_CONFIG, "username");
    props.put(CONNECTION_PASSWORD_CONFIG, "password");
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertHasErrorMessage(result, KERBEROS_PRINCIPAL_CONFIG, "Either only Kerberos");
    assertHasErrorMessage(result, KERBEROS_KEYTAB_PATH_CONFIG, "Either only Kerberos");
    assertHasErrorMessage(result, CONNECTION_USERNAME_CONFIG, "Either only Kerberos");
    assertHasErrorMessage(result, CONNECTION_PASSWORD_CONFIG, "Either only Kerberos");

    keytab.toFile().delete();
  }

  @Test
  public void testValidKerberos() throws IOException {
    // kerberos configs not set
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);

    // kerberos configs both false
    Path keytab = Files.createTempFile("es", ".keytab");
    props.put(KERBEROS_PRINCIPAL_CONFIG, "principal");
    props.put(KERBEROS_KEYTAB_PATH_CONFIG, keytab.toString());
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertNoErrors(result);
    keytab.toFile().delete();
  }

  @Test
  public void testInvalidLingerMs() {
    props.put(LINGER_MS_CONFIG, "1001");
    props.put(FLUSH_TIMEOUT_MS_CONFIG, "1000");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, LINGER_MS_CONFIG, "can not be larger than");
    assertHasErrorMessage(result, FLUSH_TIMEOUT_MS_CONFIG, "can not be larger than");
  }

  @Test
  public void testValidLingerMs() {
    props.put(LINGER_MS_CONFIG, "999");
    props.put(FLUSH_TIMEOUT_MS_CONFIG, "1000");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidMaxBufferedRecords() {
    props.put(MAX_BUFFERED_RECORDS_CONFIG, "1");
    props.put(BATCH_SIZE_CONFIG, "2");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "2");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, MAX_BUFFERED_RECORDS_CONFIG, "must be larger than or equal to");
    assertHasErrorMessage(result, BATCH_SIZE_CONFIG, "must be larger than or equal to");
    assertHasErrorMessage(result, MAX_IN_FLIGHT_REQUESTS_CONFIG, "must be larger than or equal to");
  }

  @Test
  public void testValidMaxBufferedRecords() {
    props.put(MAX_BUFFERED_RECORDS_CONFIG, "5");
    props.put(BATCH_SIZE_CONFIG, "2");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "2");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidProxy() {
    props.put(PROXY_HOST_CONFIG, "");
    props.put(PROXY_USERNAME_CONFIG, "username");
    props.put(PROXY_PASSWORD_CONFIG, "password");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, PROXY_HOST_CONFIG, " must be set to use");
    assertHasErrorMessage(result, PROXY_USERNAME_CONFIG, " must be set to use");
    assertHasErrorMessage(result, PROXY_PASSWORD_CONFIG, " must be set to use");

    props.remove(PROXY_USERNAME_CONFIG);

    props.put(PROXY_HOST_CONFIG, "proxy");
    props.put(PROXY_PASSWORD_CONFIG, "password");
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertHasErrorMessage(result, PROXY_USERNAME_CONFIG, "Either both or neither");
    assertHasErrorMessage(result, PROXY_PASSWORD_CONFIG, "Either both or neither");
  }

  @Test
  public void testValidProxy() {
    props.put(PROXY_HOST_CONFIG, "proxy");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);

    props.put(PROXY_HOST_CONFIG, "proxy");
    props.put(PROXY_USERNAME_CONFIG, "password");
    props.put(PROXY_PASSWORD_CONFIG, "password");
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidSsl() {
    // no SSL
    props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, SECURITY_PROTOCOL_CONFIG, "At least these SSL configs ");

    // SSL
    props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "a");
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "b");
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "c");
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "d");
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertHasErrorMessage(result, SECURITY_PROTOCOL_CONFIG, "to use SSL configs");
  }

  @Test
  public void testIncompatibleESVersionWithConnector() {
    validator = new Validator(props, () -> mockClient);
    when(mockInfoResponse.getVersion().getNumber()).thenReturn("6.0.0");
    Config result = validator.validate();
    assertHasErrorMessage(result, CONNECTION_URL_CONFIG, "not compatible with Elasticsearch");
  }

  @Test
  public void testCompatibleESVersionWithConnector() {
    validator = new Validator(props, () -> mockClient);
    String[] compatibleESVersions = {"7.0.0", "7.9.3", "7.10.0", "7.12.1", "8.0.0", "10.10.10"};
    for (String version : compatibleESVersions) {
      when(mockInfoResponse.getVersion().getNumber()).thenReturn(version);
      Config result = validator.validate();

      assertNoErrors(result);
    }
  }

  @Test
  public void testValidSsl() {
    // no SSL
    props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name());
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);

    // SSL
    props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "a");
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "b");
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "c");
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "d");
    validator = new Validator(props, () -> mockClient);

    result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testValidConnection() {
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidConnection() throws IOException {
    when(mockClient.ping(eq(RequestOptions.DEFAULT))).thenReturn(false);
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, CONNECTION_URL_CONFIG, "Could not connect to Elasticsearch.");
  }

  @Test
  public void testInvalidConnectionThrows() throws IOException {
    when(mockClient.ping(eq(RequestOptions.DEFAULT))).thenThrow(new IOException("i iz fake"));
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, CONNECTION_URL_CONFIG, "Could not connect to Elasticsearch.");
  }

  @Test
  public void testTimestampMappingDataStreamSet() {
    configureDataStream();
    props.put(DATA_STREAM_TIMESTAMP_CONFIG, "one, two, fields");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();

    assertNoErrors(result);
  }

  @Test
  public void testTimestampMappingDataStreamNotSet() {
    props.put(DATA_STREAM_TIMESTAMP_CONFIG, "one, two, fields");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();

    assertHasErrorMessage(result, DATA_STREAM_TIMESTAMP_CONFIG, TIMESTAMP_FIELD_NOT_ALLOWED_ERROR);
  }

  @Test
  public void testIncompatibleVersionDataStreamSet() {
    configureDataStream();
    validator = new Validator(props, () -> mockClient);
    when(mockInfoResponse.getVersion().getNumber()).thenReturn("7.8.1");

    Config result = validator.validate();
    assertHasErrorMessage(result, CONNECTION_URL_CONFIG, "not compatible with data streams");
    assertHasErrorMessage(result, DATA_STREAM_TYPE_CONFIG, "not compatible with data streams");
    assertHasErrorMessage(result, DATA_STREAM_DATASET_CONFIG, "not compatible with data streams");
  }

  @Test
  public void testIncompatibleVersionDataStreamNotSet() {
    validator = new Validator(props, () -> mockClient);
    String[] incompatibleESVersions = {"7.8.0", "7.7.1", "7.6.2", "7.2.0", "7.1.1", "7.0.0-rc2"};
    for (String version : incompatibleESVersions) {
      when(mockInfoResponse.getVersion().getNumber()).thenReturn(version);
      Config result = validator.validate();

      assertNoErrors(result);
    }
  }

  @Test
  public void testCompatibleVersionDataStreamNotSet() {
    validator = new Validator(props, () -> mockClient);
    String[] compatibleESVersions = {"7.9.0", "7.9.3", "7.9.3-amd64", "7.10.0", "7.10.2", "7.11.0", "7.11.2", "7.12.0", "7.12.1",
        "8.0.0", "10.10.10", "10.1.10", "10.1.1", "8.10.10"};
    for (String version : compatibleESVersions) {
      when(mockInfoResponse.getVersion().getNumber()).thenReturn(version);
      Config result = validator.validate();

      assertNoErrors(result);
    }
  }

  @Test
  public void testCompatibleVersionDataStreamSet() {
    configureDataStream();
    validator = new Validator(props, () -> mockClient);
    String[] compatibleESVersions = {"7.9.0", "7.9.3", "7.9.3-amd64", "7.10.0", "7.10.2", "7.11.0", "7.11.2", "7.12.0", "7.12.1",
        "8.0.0", "10.10.10", "10.1.10", "10.1.1", "8.10.10"};
    for (String version : compatibleESVersions) {
      when(mockInfoResponse.getVersion().getNumber()).thenReturn(version);
      Config result = validator.validate();

      assertNoErrors(result);
    }
  }

  @Test
  public void testValidResourceMappingConfig() throws IOException {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + INDEX1 + "," + TOPIC2 + ":" + INDEX2);
    props.put(TOPICS_CONFIG_KEY, TOPIC1 + "," + TOPIC2);
    when(mockClient.indices().exists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidResourceTypeWithoutMapping() {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, EXTERNAL_RESOURCE_USAGE_CONFIG, EXTERNAL_RESOURCE_CONFIG_TOGETHER_ERROR);
    assertHasErrorMessage(result, TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, EXTERNAL_RESOURCE_CONFIG_TOGETHER_ERROR);
  }

  @Test
  public void testInvalidMappingNoneResourceType() {
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + INDEX1);
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, EXTERNAL_RESOURCE_USAGE_CONFIG, EXTERNAL_RESOURCE_CONFIG_TOGETHER_ERROR);
    assertHasErrorMessage(result, TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, EXTERNAL_RESOURCE_CONFIG_TOGETHER_ERROR);
  }

  @Test
  public void testInvalidResourceMappingWithDataStreamConfigs() {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + INDEX1);
    props.put(DATA_STREAM_TYPE_CONFIG, "logs");
    props.put(DATA_STREAM_DATASET_CONFIG, "dataset");
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG,
            EXTERNAL_RESOURCE_DATA_STREAM_MUTUAL_EXCLUSIVITY_ERROR);
    assertHasErrorMessage(result, DATA_STREAM_TYPE_CONFIG,
            EXTERNAL_RESOURCE_DATA_STREAM_MUTUAL_EXCLUSIVITY_ERROR);
    assertHasErrorMessage(result, DATA_STREAM_DATASET_CONFIG,
            EXTERNAL_RESOURCE_DATA_STREAM_MUTUAL_EXCLUSIVITY_ERROR);
  }

  @Test
  public void testInvalidTopicMappingFormat() {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + INDEX1 + ":extra," + TOPIC2 + ":" + INDEX2);
    props.put(TOPICS_CONFIG_KEY, TOPIC1 + "," + TOPIC2);
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertHasErrorMessage(result, TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, INVALID_MAPPING_FORMAT_ERROR);
  }

  @Test
  public void testInvalidDuplicateTopicMapping() {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + INDEX1 + "," + TOPIC1 + ":" + INDEX2);
    props.put(TOPICS_CONFIG_KEY, TOPIC1);
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    String expectedMessage = String.format(DUPLICATE_TOPIC_MAPPING_ERROR_FORMAT, TOPIC1);
    assertHasErrorMessage(result, TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, expectedMessage);
  }

  @Test
  public void testInvalidDuplicateResourceMapping() {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + INDEX1 + "," + TOPIC2 + ":" + INDEX1);
    props.put(TOPICS_CONFIG_KEY, TOPIC1 + "," + TOPIC2);
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    String expectedMessage = String.format(DUPLICATE_RESOURCE_MAPPING_ERROR_FORMAT, INDEX1);
    assertHasErrorMessage(result, TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, expectedMessage);
  }

  @Test
  public void testInvalidUnmappedTopic() {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + INDEX1);
    props.put(TOPICS_CONFIG_KEY, TOPIC1 + "," + TOPIC2); // topic2 is not mapped
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    String expectedMessage = String.format(UNMAPPED_TOPIC_ERROR_FORMAT, TOPIC2);
    assertHasErrorMessage(result, TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, expectedMessage);
  }

  @Test
  public void testInvalidUnconfiguredTopic() {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + INDEX1 + "," + TOPIC2 + ":" + INDEX2);
    props.put(TOPICS_CONFIG_KEY, TOPIC1); // topic2 is mapped but not configured
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    String expectedMessage = String.format(UNCONFIGURED_TOPIC_ERROR_FORMAT, TOPIC2);
    assertHasErrorMessage(result, TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, expectedMessage);
  }

  @Test
  public void testValidMappingWithinLimit() throws IOException {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + INDEX1 + "," + TOPIC2 + ":" + INDEX2);
    props.put(TOPICS_CONFIG_KEY, TOPIC1 + "," + TOPIC2);
    props.put(MAX_EXTERNAL_RESOURCE_MAPPINGS_CONFIG, "5"); // Set limit higher than number of mappings
    when(mockClient.indices().exists(any(GetIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(true);
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidTooManyMappings() {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + INDEX1 + "," + TOPIC2 + ":" + INDEX2 + "," + TOPIC3 + ":index3");
    props.put(TOPICS_CONFIG_KEY, TOPIC1 + "," + TOPIC2 + "," + TOPIC3);
    props.put(MAX_EXTERNAL_RESOURCE_MAPPINGS_CONFIG, "2"); // Set limit lower than number of mappings
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    String expectedMessage = String.format(TOO_MANY_MAPPINGS_ERROR_FORMAT, 3, 2, MAX_EXTERNAL_RESOURCE_MAPPINGS_CONFIG);
    assertHasErrorMessage(result, TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, expectedMessage);
  }

  @Test
  public void testDefaultMappingLimit() {
    // Create a mapping string with 16 topics (exceeds default limit of 15)
    StringBuilder mappingBuilder = new StringBuilder();
    StringBuilder topicsBuilder = new StringBuilder();
    for (int i = 1; i <= 16; i++) {
      if (i > 1) {
        mappingBuilder.append(",");
        topicsBuilder.append(",");
      }
      mappingBuilder.append("topic").append(i).append(":index").append(i);
      topicsBuilder.append("topic").append(i);
    }

    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, mappingBuilder.toString());
    props.put(TOPICS_CONFIG_KEY, topicsBuilder.toString());
    // Don't set MAX_EXTERNAL_RESOURCE_MAPPINGS_CONFIG to test default limit
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    String expectedMessage = String.format(TOO_MANY_MAPPINGS_ERROR_FORMAT, 16, 15, MAX_EXTERNAL_RESOURCE_MAPPINGS_CONFIG);
    assertHasErrorMessage(result, TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, expectedMessage);
  }

  @Test
  public void testValidDataStreamResourceTypeWithTimestampField() throws IOException {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.DATASTREAM.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + LOGS_TEST_1);
    props.put(TOPICS_CONFIG_KEY, TOPIC1);
    props.put(DATA_STREAM_TIMESTAMP_CONFIG, "created_at");

    // Mock data stream exists call
    when(mockClient.indices().exists(any(GetIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(true);

    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidAliasDataStreamResourceTypeWithUpsert() throws IOException {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.ALIAS_DATASTREAM.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + ALIAS1);
    props.put(TOPICS_CONFIG_KEY, TOPIC1);
    props.put(WRITE_METHOD_CONFIG, UPSERT_METHOD);

    // Mock alias exists call
    when(mockClient.indices().existsAlias(any(GetAliasesRequest.class), any(RequestOptions.class)))
            .thenReturn(true);

    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertHasErrorMessage(result, WRITE_METHOD_CONFIG, UPSERT_NOT_ALLOWED_WITH_DATASTREAM_ERROR);
  }

  @Test
  public void testInvalidDataStreamResourceTypeWithDeleteBehavior() throws IOException {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.DATASTREAM.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + LOGS_TEST_1);
    props.put(TOPICS_CONFIG_KEY, TOPIC1);
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, DELETE_BEHAVIOR);

    // Mock data stream exists call
    when(mockClient.indices().exists(any(GetIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(true);

    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertHasErrorMessage(result, BEHAVIOR_ON_NULL_VALUES_CONFIG, DELETE_NOT_ALLOWED_WITH_DATASTREAM_ERROR);
  }

  @Test
  public void testInvalidIndexResourceTypeWithTimestampField() throws IOException {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + INDEX1);
    props.put(TOPICS_CONFIG_KEY, TOPIC1);
    props.put(DATA_STREAM_TIMESTAMP_CONFIG, "created_at");

    // Mock index exists call
    when(mockClient.indices().exists(any(GetIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(true);

    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertHasErrorMessage(result, DATA_STREAM_TIMESTAMP_CONFIG, TIMESTAMP_FIELD_NOT_ALLOWED_ERROR);
  }

  @Test
  public void testValidIndexResourceExists() throws IOException {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + INDEX1);
    props.put(TOPICS_CONFIG_KEY, TOPIC1);

    // Mock index exists call on the high-level client
    when(mockClient.indices().exists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);

    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidIndexResourceDoesNotExist() throws IOException {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + INDEX1);
    props.put(TOPICS_CONFIG_KEY, TOPIC1);

    // Mock index does not exist call on the high-level client
    when(mockClient.indices().exists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(false);

    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    String expectedMessage = String.format(RESOURCE_DOES_NOT_EXIST_ERROR_FORMAT, ExternalResourceUsage.INDEX.name().toLowerCase(), INDEX1);
    assertHasErrorMessage(result, TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, expectedMessage);
  }

  @Test
  public void testValidAliasResourceExists() throws IOException {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.ALIAS_INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + ALIAS1);
    props.put(TOPICS_CONFIG_KEY, TOPIC1);

    // Mock alias exists call on the high-level client
    when(mockClient.indices().existsAlias(any(GetAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(true);

    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidAliasResourceDoesNotExist() throws IOException {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.ALIAS_INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + ALIAS1);
    props.put(TOPICS_CONFIG_KEY, TOPIC1);

    // Mock alias does not exist call on the high-level client
    when(mockClient.indices().existsAlias(any(GetAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(false);

    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    String expectedMessage = String.format(RESOURCE_DOES_NOT_EXIST_ERROR_FORMAT, ExternalResourceUsage.ALIAS_INDEX.name().toLowerCase(), ALIAS1);
    assertHasErrorMessage(result, TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, expectedMessage);
  }

  @Test
  public void testResourceExistenceCheckFailure() throws IOException {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, TOPIC1 + ":" + INDEX1);
    props.put(TOPICS_CONFIG_KEY, TOPIC1);

    // Mock Elasticsearch exception on the high-level client
    when(mockClient.indices().exists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenThrow(new ElasticsearchStatusException("Index not found", RestStatus.NOT_FOUND));

    validator = new Validator(props, () -> mockClient);
    Config result = validator.validate();
    String expectedMessage = String.format(RESOURCE_EXISTENCE_CHECK_FAILED_ERROR_FORMAT, ExternalResourceUsage.INDEX.name().toLowerCase(), INDEX1, "Index not found");
    assertHasErrorMessage(result, TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, expectedMessage);
  }

  @Test
  public void testValidMappingWithWhitespace() throws IOException {
    props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
    props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, " " + TOPIC1 + " : " + INDEX1 + " , " + TOPIC2 + " : " + INDEX2 + " ");
    props.put(TOPICS_CONFIG_KEY, TOPIC1 + "," + TOPIC2);
    when(mockClient.indices().exists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);
    validator = new Validator(props, () -> mockClient);

    Config result = validator.validate();
    assertNoErrors(result);
  }

  private static void assertHasErrorMessage(Config config, String property, String msg) {
    for (ConfigValue configValue : config.configValues()) {
      if (configValue.name().equals(property)) {
        assertFalse(configValue.errorMessages().isEmpty());
        assertTrue(configValue.errorMessages().get(0).contains(msg));
      }
    }
  }

  private static void assertNoErrors(Config config) {
    config.configValues().forEach(c -> assertTrue(c.errorMessages().isEmpty()));
  }

  private void configureDataStream() {
    props.put(DATA_STREAM_DATASET_CONFIG, VALID_DATASET);
    props.put(DATA_STREAM_TYPE_CONFIG, "logs");
  }
}
