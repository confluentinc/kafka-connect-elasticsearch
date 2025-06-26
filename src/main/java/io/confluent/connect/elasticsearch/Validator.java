/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.SslConfigs;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SecurityProtocol;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnNullValues;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_DATASET_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_TYPE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DataStreamType;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_TOPICS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_TOPICS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.KERBEROS_KEYTAB_PATH_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.KERBEROS_PRINCIPAL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.PROXY_HOST_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.PROXY_PASSWORD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.PROXY_PORT_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.PROXY_USERNAME_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SECURITY_PROTOCOL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SSL_CONFIG_PREFIX;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WRITE_METHOD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WriteMethod;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.EXTERNAL_RESOURCE_USAGE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG;
import static io.confluent.connect.elasticsearch.ExternalResourceExistenceChecker.ExternalResourceExistenceStrategy;

public class Validator {

  private static final Logger log = LoggerFactory.getLogger(Validator.class);

  private static final String CONNECTOR_V11_COMPATIBLE_ES_VERSION = "7.0.0";
  private static final String DATA_STREAM_COMPATIBLE_ES_VERSION = "7.9.0";

  public static final String EXTERNAL_RESOURCE_CONFIG_TOGETHER_ERROR =
          String.format("Invalid configuration:"
          + " %s and %s must be configured together."
          + " Either both must be set, or both must be empty.",
          EXTERNAL_RESOURCE_USAGE_CONFIG, TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG);

  public static final String EXTERNAL_RESOURCE_DATA_STREAM_MUTUAL_EXCLUSIVITY_ERROR =
          String.format(
          "Resource mapping mode and data stream configs are mutually exclusive. "
          + "When using %s, data stream configs (%s, %s) must not be set.",
          TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG,
          DATA_STREAM_TYPE_CONFIG,
          DATA_STREAM_DATASET_CONFIG
  );

  public static final String UNMAPPED_TOPIC_ERROR_FORMAT = 
      "Topic '%s' is not mapped to any resource. "
      + "All configured topics must be mapped to a resource.";

  public static final String UNCONFIGURED_TOPIC_ERROR_FORMAT = 
      "Topic '%s' is mapped but not configured. "
      + "All mapped topics must be configured topics.";

  public static final String RESOURCE_DOES_NOT_EXIST_ERROR_FORMAT = 
      "%s '%s' does not exist in Elasticsearch";

  public static final String RESOURCE_EXISTENCE_CHECK_FAILED_ERROR_FORMAT = 
      "Failed to check if %s '%s' exists: %s";

  public static final String TIMESTAMP_FIELD_NOT_ALLOWED_ERROR = String.format(
      "Mapping a field to the '@timestamp' field is only necessary when working with data streams. "
      + "'%s' must not be set when not using data streams.", DATA_STREAM_TIMESTAMP_CONFIG);

  public static final String UPSERT_NOT_ALLOWED_WITH_DATASTREAM_ERROR = String.format(
          "Upserts are not supported with data streams. %s must not be %s when using data streams.",
          WRITE_METHOD_CONFIG, WriteMethod.UPSERT
  );

  public static final String DELETE_NOT_ALLOWED_WITH_DATASTREAM_ERROR = String.format(
          "Deletes are not supported with data streams. %s must not be %s when using data streams.",
          BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.DELETE
  );

  private ElasticsearchSinkConnectorConfig config;
  private Map<String, ConfigValue> values;
  private List<ConfigValue> validations;
  private ClientFactory clientFactory;
  private Map<String, String> topicToExternalResourceMap;

  public Validator(Map<String, String> props) {
    this(props, null);
  }

  // Exposed for testing
  protected Validator(Map<String, String> props, ClientFactory clientFactory) {
    try {
      this.config = new ElasticsearchSinkConnectorConfig(props);
    } catch (ConfigException e) {
      // some configs are invalid
    }

    this.clientFactory = clientFactory == null ? this::createClient : clientFactory;
    validations = ElasticsearchSinkConnectorConfig.CONFIG.validate(props);
    values = validations.stream().collect(Collectors.toMap(ConfigValue::name, Function.identity()));
  }

  public Config validate() {
    if (config == null) {
      // individual configs are invalid, no point in validating combinations
      return new Config(validations);
    }

    validateCredentials();
    validateResourceConfigs();
    validateDataStreamConfigs();
    validateDataStreamCompatibility();
    validateIgnoreConfigs();
    validateKerberos();
    validateLingerMs();
    validateMaxBufferedRecords();
    validateProxy();
    validateSsl();

    if (!hasErrors()) {
      // no point in connection validation if previous ones fails
      try (RestHighLevelClient client = clientFactory.client()) {
        validateConnection(client);
        validateVersion(client);
        validateResourceExists(client);
      } catch (IOException e) {
        log.warn("Closing the client failed.", e);
      } catch (Throwable e) {
        log.error("Failed to create client to verify connection. ", e);
        addErrorMessage(CONNECTION_URL_CONFIG, "Failed to create client to verify connection. "
            + e.getMessage());
      }
    }

    return new Config(validations);
  }

  private void validateCredentials() {
    boolean onlyOneSet = config.username() != null ^ config.password() != null;
    if (onlyOneSet) {
      String errorMessage = String.format(
          "Both '%s' and '%s' must be set.", CONNECTION_USERNAME_CONFIG, CONNECTION_PASSWORD_CONFIG
      );
      addErrorMessage(CONNECTION_USERNAME_CONFIG, errorMessage);
      addErrorMessage(CONNECTION_PASSWORD_CONFIG, errorMessage);
    }
  }

  /**
   * Ensures proper configuration based on external resource usage:
   * - When externalResourceUsage != DISABLED: topic mappings must be configured
   * - When externalResourceUsage == DISABLED: topic mappings must be empty
   * - Validates mutual exclusivity with legacy data stream configurations
   * - Ensures all configured topics have corresponding resource mappings
   */
  private void validateResourceConfigs() {
    boolean hasResourceMappings = !config.topicToExternalResourceMapping().isEmpty();

    // Validate that both are set together or both are empty
    if (config.isExternalResourceUsageEnabled() != hasResourceMappings) {
      addErrorMessage(EXTERNAL_RESOURCE_USAGE_CONFIG, EXTERNAL_RESOURCE_CONFIG_TOGETHER_ERROR);
      addErrorMessage(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG,
              EXTERNAL_RESOURCE_CONFIG_TOGETHER_ERROR);
      return;
    }

    // Skip resource mapping validations when both are empty
    if (!hasResourceMappings) {
      return;
    }

    // Validate that data stream configs are not set (mutual exclusivity)
    if (!config.dataStreamType().toUpperCase().equals(DataStreamType.NONE.name())
            || !config.dataStreamDataset().isEmpty()) {
      addErrorMessage(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG,
              EXTERNAL_RESOURCE_DATA_STREAM_MUTUAL_EXCLUSIVITY_ERROR);
      addErrorMessage(DATA_STREAM_TYPE_CONFIG,
              EXTERNAL_RESOURCE_DATA_STREAM_MUTUAL_EXCLUSIVITY_ERROR);
      addErrorMessage(DATA_STREAM_DATASET_CONFIG,
              EXTERNAL_RESOURCE_DATA_STREAM_MUTUAL_EXCLUSIVITY_ERROR);
      return;
    }

    // Parse topic-to-resource mappings
    try {
      topicToExternalResourceMap = config.getTopicToExternalResourceMap();
    } catch (ConfigException e) {
      addErrorMessage(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, e.getMessage());
      return;
    }

    // Validate all topics are mapped and configured
    validateAllTopicsMapped(new HashSet<>(topicToExternalResourceMap.keySet()));
  }

  /**
   * Validates that all configured Kafka topics have corresponding resource mappings.
   * Ensures no topics are left unmapped when using resource mapping configuration.
   * Also validates that all mapped topics are configured topics.
   */
  private void validateAllTopicsMapped(Set<String> mappedTopics) {
    String[] configuredTopics = config.getKafkaTopics();
    Set<String> configuredTopicSet = new HashSet<>(Arrays.asList(configuredTopics));

    // Check for missing mappings (configured topics not in mapped topics)
    for (String topic : configuredTopicSet) {
      if (!mappedTopics.contains(topic)) {
        String errorMessage = String.format(UNMAPPED_TOPIC_ERROR_FORMAT, topic);
        addErrorMessage(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, errorMessage);
        return;
      }
    }

    // Check for unconfigured mappings (mapped topics not in configured topics)
    for (String topic : mappedTopics) {
      if (!configuredTopicSet.contains(topic)) {
        String errorMessage = String.format(UNCONFIGURED_TOPIC_ERROR_FORMAT, topic);
        addErrorMessage(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, errorMessage);
        return;
      }
    }
  }

  /**
   * Ensures data stream type and dataset are configured together:
   * - Both must be set (for auto data stream creation)
   * - Both must be empty (for auto index creation/resource mapping)
   */
  private void validateDataStreamConfigs() {
    if (config.dataStreamType().toUpperCase().equals(DataStreamType.NONE.name())
            ^ config.dataStreamDataset().isEmpty()) {
      String errorMessage = String.format(
          "Either both or neither '%s' and '%s' must be set.",
          DATA_STREAM_DATASET_CONFIG,
          DATA_STREAM_TYPE_CONFIG
      );
      addErrorMessage(DATA_STREAM_TYPE_CONFIG, errorMessage);
      addErrorMessage(DATA_STREAM_DATASET_CONFIG, errorMessage);
    }
  }

  private void validateDataStreamCompatibility() {
    if (!config.isDataStream()) {
      // Validate timestamp field is not set for non-data stream configurations
      if (!config.dataStreamTimestampField().isEmpty()) {
        addErrorMessage(DATA_STREAM_TIMESTAMP_CONFIG, TIMESTAMP_FIELD_NOT_ALLOWED_ERROR);
      }
      return;
    }

    if (config.writeMethod() == WriteMethod.UPSERT) {
      addErrorMessage(WRITE_METHOD_CONFIG, UPSERT_NOT_ALLOWED_WITH_DATASTREAM_ERROR);
      return;
    }

    if (config.behaviorOnNullValues() == BehaviorOnNullValues.DELETE) {
      addErrorMessage(BEHAVIOR_ON_NULL_VALUES_CONFIG, DELETE_NOT_ALLOWED_WITH_DATASTREAM_ERROR);
    }
  }

  private void validateIgnoreConfigs() {
    if (config.ignoreKey() && !config.ignoreKeyTopics().isEmpty()) {
      String errorMessage = String.format(
          "'%s' can not be set if '%s' is true.", IGNORE_KEY_TOPICS_CONFIG, IGNORE_KEY_CONFIG
      );
      addErrorMessage(IGNORE_KEY_CONFIG, errorMessage);
      addErrorMessage(IGNORE_KEY_TOPICS_CONFIG, errorMessage);
    }

    if (config.ignoreSchema() && !config.ignoreSchemaTopics().isEmpty()) {
      String errorMessage = String.format(
          "'%s' can not be set if '%s' is true.", IGNORE_SCHEMA_TOPICS_CONFIG, IGNORE_SCHEMA_CONFIG
      );
      addErrorMessage(IGNORE_SCHEMA_CONFIG, errorMessage);
      addErrorMessage(IGNORE_SCHEMA_TOPICS_CONFIG, errorMessage);
    }
  }

  private void validateKerberos() {
    boolean onlyOneSet = config.kerberosUserPrincipal() != null ^ config.keytabPath() != null;
    if (onlyOneSet) {
      String errorMessage = String.format(
          "Either both or neither '%s' and '%s' must be set.",
          KERBEROS_PRINCIPAL_CONFIG,
          KERBEROS_KEYTAB_PATH_CONFIG
      );
      addErrorMessage(KERBEROS_PRINCIPAL_CONFIG, errorMessage);
      addErrorMessage(KERBEROS_KEYTAB_PATH_CONFIG, errorMessage);
    }

    if (config.isKerberosEnabled()) {
      // currently do not support Kerberos with regular auth
      if (config.isAuthenticatedConnection()) {
        String errorMessage = String.format(
            "Either only Kerberos (%s, %s) or connection credentials (%s, %s) must be set.",
            KERBEROS_PRINCIPAL_CONFIG,
            KERBEROS_KEYTAB_PATH_CONFIG,
            CONNECTION_USERNAME_CONFIG,
            CONNECTION_PASSWORD_CONFIG
        );
        addErrorMessage(KERBEROS_PRINCIPAL_CONFIG, errorMessage);
        addErrorMessage(KERBEROS_KEYTAB_PATH_CONFIG, errorMessage);
        addErrorMessage(CONNECTION_USERNAME_CONFIG, errorMessage);
        addErrorMessage(CONNECTION_PASSWORD_CONFIG, errorMessage);
      }

      // currently do not support Kerberos with proxy
      if (config.isBasicProxyConfigured()) {
        String errorMessage = String.format(
            "Kerberos (%s, %s) is not supported with proxy settings (%s).",
            KERBEROS_PRINCIPAL_CONFIG,
            KERBEROS_KEYTAB_PATH_CONFIG,
            PROXY_HOST_CONFIG
        );
        addErrorMessage(KERBEROS_PRINCIPAL_CONFIG, errorMessage);
        addErrorMessage(KERBEROS_KEYTAB_PATH_CONFIG, errorMessage);
        addErrorMessage(PROXY_HOST_CONFIG, errorMessage);
      }
    }

  }

  private void validateLingerMs() {
    if (config.lingerMs() > config.flushTimeoutMs()) {
      String errorMessage = String.format(
          "'%s' (%d) can not be larger than '%s' (%d).",
          LINGER_MS_CONFIG, config.lingerMs(), FLUSH_TIMEOUT_MS_CONFIG, config.flushTimeoutMs()
      );
      addErrorMessage(LINGER_MS_CONFIG, errorMessage);
      addErrorMessage(FLUSH_TIMEOUT_MS_CONFIG, errorMessage);
    }
  }

  private void validateMaxBufferedRecords() {
    if (config.maxBufferedRecords() < config.batchSize() * config.maxInFlightRequests()) {
      String errorMessage = String.format(
          "'%s' (%d) must be larger than or equal to '%s' (%d) x %s (%d).",
          MAX_BUFFERED_RECORDS_CONFIG, config.maxBufferedRecords(),
          BATCH_SIZE_CONFIG, config.batchSize(),
          MAX_IN_FLIGHT_REQUESTS_CONFIG, config.maxInFlightRequests()
      );

      addErrorMessage(MAX_BUFFERED_RECORDS_CONFIG, errorMessage);
      addErrorMessage(BATCH_SIZE_CONFIG, errorMessage);
      addErrorMessage(MAX_IN_FLIGHT_REQUESTS_CONFIG, errorMessage);
    }
  }

  private void validateProxy() {
    if (!config.isBasicProxyConfigured()) {
      if (!config.proxyUsername().isEmpty()) {
        String errorMessage = String.format(
            "'%s' must be set to use '%s'.", PROXY_HOST_CONFIG, PROXY_USERNAME_CONFIG
        );
        addErrorMessage(PROXY_USERNAME_CONFIG, errorMessage);
        addErrorMessage(PROXY_HOST_CONFIG, errorMessage);
      }

      if (config.proxyPassword() != null) {
        String errorMessage = String.format(
            "'%s' must be set to use '%s'.", PROXY_HOST_CONFIG, PROXY_PASSWORD_CONFIG
        );
        addErrorMessage(PROXY_PASSWORD_CONFIG, errorMessage);
        addErrorMessage(PROXY_HOST_CONFIG, errorMessage);
      }
    } else {
      boolean onlyOneSet = config.proxyUsername().isEmpty() ^ config.proxyPassword() == null;
      if (onlyOneSet) {
        String errorMessage = String.format(
            "Either both or neither '%s' and '%s' can be set.",
            PROXY_USERNAME_CONFIG,
            PROXY_PASSWORD_CONFIG
        );
        addErrorMessage(PROXY_USERNAME_CONFIG, errorMessage);
        addErrorMessage(PROXY_PASSWORD_CONFIG, errorMessage);
      }
    }
  }

  private void validateSsl() {
    Map<String, Object> sslConfigs = config.originalsWithPrefix(SSL_CONFIG_PREFIX);
    if (!config.isSslEnabled()) {
      if (!sslConfigs.isEmpty()) {
        String errorMessage = String.format(
            "'%s' must be set to '%s' to use SSL configs.",
            SECURITY_PROTOCOL_CONFIG,
            SecurityProtocol.SSL
        );
        addErrorMessage(SECURITY_PROTOCOL_CONFIG, errorMessage);
      }
    } else {
      if (sslConfigs.isEmpty()) {
        String errorMessage = String.format(
            "At least these SSL configs ('%s', '%s', '%s', and '%s') must be present for SSL"
                + " support. Otherwise set '%s' to '%s'.",
            SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            SECURITY_PROTOCOL_CONFIG,
            SecurityProtocol.PLAINTEXT
        );
        addErrorMessage(SECURITY_PROTOCOL_CONFIG, errorMessage);
      }
    }
  }

  private void validateVersion(RestHighLevelClient client) {
    MainResponse response;
    try {
      response = client.info(RequestOptions.DEFAULT);
    } catch (IOException | ElasticsearchStatusException e) {
      // Same error messages as from validating the connection for IOException.
      // Insufficient privileges to validate the version number if caught
      // ElasticsearchStatusException.
      return;
    }
    String esVersionNumber = response.getVersion().getNumber();
    if (config.isDataStream()
        && compareVersions(esVersionNumber, DATA_STREAM_COMPATIBLE_ES_VERSION) < 0) {
      String errorMessage = String.format(
          "Elasticsearch version %s is not compatible with data streams. Elasticsearch"
              + "version must be at least %s.",
          esVersionNumber,
          DATA_STREAM_COMPATIBLE_ES_VERSION
      );
      addErrorMessage(CONNECTION_URL_CONFIG, errorMessage);
      addErrorMessage(DATA_STREAM_TYPE_CONFIG, errorMessage);
      addErrorMessage(DATA_STREAM_DATASET_CONFIG, errorMessage);
    }
    if (compareVersions(esVersionNumber, CONNECTOR_V11_COMPATIBLE_ES_VERSION) < 0) {
      String errorMessage = String.format(
          "Connector version %s is not compatible with Elasticsearch version %s. Elasticsearch "
              + "version must be at least %s.",
          Version.getVersion(),
          esVersionNumber,
          CONNECTOR_V11_COMPATIBLE_ES_VERSION
      );
      addErrorMessage(CONNECTION_URL_CONFIG, errorMessage);
    }
  }

  /**
   * Validates that all mapped external resources exist in Elasticsearch.
   * Checks resource existence based on the configured external resource type.
   * Only validates when external resource usage is enabled.
   */
  private void validateResourceExists(RestHighLevelClient client) {
    if (!config.isExternalResourceUsageEnabled()) {
      return;
    }

    ExternalResourceExistenceStrategy existenceStrategy =
        ExternalResourceExistenceChecker.getExistenceStrategy(config.externalResourceUsage());
    for (Map.Entry<String, String> entry : topicToExternalResourceMap.entrySet()) {
      String resource = entry.getValue();
      try {
        boolean exists = existenceStrategy.exists(client, resource);
        if (!exists) {
          String errorMessage = String.format(
                  RESOURCE_DOES_NOT_EXIST_ERROR_FORMAT,
                  config.externalResourceUsage().name().toLowerCase(),
                  resource
          );
          addErrorMessage(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, errorMessage);
          return;
        }
      } catch (IOException | ElasticsearchStatusException e) {
        String errorMessage = String.format(
                RESOURCE_EXISTENCE_CHECK_FAILED_ERROR_FORMAT,
                config.externalResourceUsage().name().toLowerCase(),
                resource,
                e.getMessage()
        );
        addErrorMessage(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG, errorMessage);
        return;
      }
    }
  }

  /**
   * Compares <code>versionNumber</code> to <code>compatibleVersion</code>.
   *
   * @return  a negative integer, zero, or a positive integer if
   *          <code>versionNumber</code> is less than, equal to, or greater
   *          than <code>compatibleVersion</code>.
   */
  private int compareVersions(String versionNumber, String compatibleVersion) {
    String[] versionSplit = versionNumber.split("\\.");
    String[] compatibleSplit = compatibleVersion.split("\\.");

    for (int i = 0; i < Math.min(versionSplit.length, compatibleSplit.length); i++) {
      String versionSplitBeforeSuffix = versionSplit[i].split("-")[0];
      String compatibleSplitBeforeSuffix = compatibleSplit[i].split("-")[0];
      int comparison = Integer.compare(
          Integer.parseInt(versionSplitBeforeSuffix),
          Integer.parseInt(compatibleSplitBeforeSuffix)
      );
      if (comparison != 0) {
        return comparison;
      }
    }
    return versionSplit.length - compatibleSplit.length;
  }

  private void validateConnection(RestHighLevelClient client) {
    boolean successful;
    String exceptionMessage = "";
    try {
      successful = client.ping(RequestOptions.DEFAULT);
    } catch (ElasticsearchStatusException e) {
      switch (e.status()) {
        case FORBIDDEN:
          // ES is up, but user is not authorized to ping server
          successful = true;
          break;
        default:
          successful = false;
          exceptionMessage = String.format("Error message: %s", e.getMessage());
      }
    } catch (Exception e) {
      successful = false;
      exceptionMessage = String.format("Error message: %s", e.getMessage());
    }
    if (!successful) {
      String errorMessage = String.format(
          "Could not connect to Elasticsearch. %s",
          exceptionMessage
      );
      addErrorMessage(CONNECTION_URL_CONFIG, errorMessage);

      if (config.isAuthenticatedConnection()) {
        errorMessage = String.format(
            "Could not authenticate the user. Check the '%s' and '%s'. %s",
            CONNECTION_USERNAME_CONFIG,
            CONNECTION_PASSWORD_CONFIG,
            exceptionMessage
        );
        addErrorMessage(CONNECTION_USERNAME_CONFIG, errorMessage);
        addErrorMessage(CONNECTION_PASSWORD_CONFIG, errorMessage);
      }

      if (config.isSslEnabled()) {
        errorMessage = String.format(
            "Could not connect to Elasticsearch. Check your SSL settings.%s",
            exceptionMessage
        );

        addErrorMessage(SECURITY_PROTOCOL_CONFIG, errorMessage);
      }

      if (config.isKerberosEnabled()) {
        errorMessage = String.format(
            "Could not connect to Elasticsearch. Check your Kerberos settings. %s",
            exceptionMessage
        );

        addErrorMessage(KERBEROS_PRINCIPAL_CONFIG, errorMessage);
        addErrorMessage(KERBEROS_KEYTAB_PATH_CONFIG, errorMessage);
      }

      if (config.isBasicProxyConfigured()) {
        errorMessage = String.format(
            "Could not connect to Elasticsearch. Check your proxy settings. %s",
            exceptionMessage
        );
        addErrorMessage(PROXY_HOST_CONFIG, errorMessage);
        addErrorMessage(PROXY_PORT_CONFIG, errorMessage);

        if (config.isProxyWithAuthenticationConfigured()) {
          addErrorMessage(PROXY_USERNAME_CONFIG, errorMessage);
          addErrorMessage(PROXY_PASSWORD_CONFIG, errorMessage);
        }
      }
    }
  }

  private void addErrorMessage(String property, String error) {
    values.get(property).addErrorMessage(error);
  }

  private RestHighLevelClient createClient() {
    ConfigCallbackHandler configCallbackHandler = new ConfigCallbackHandler(config);
    return new RestHighLevelClient(
        RestClient
            .builder(
                config.connectionUrls()
                    .stream()
                    .map(HttpHost::create)
                    .collect(Collectors.toList())
                    .toArray(new HttpHost[config.connectionUrls().size()])
            )
            .setHttpClientConfigCallback(configCallbackHandler)
    );
  }

  private boolean hasErrors() {
    for (ConfigValue config : validations) {
      if (!config.errorMessages().isEmpty()) {
        return true;
      }
    }

    return false;
  }

  interface ClientFactory {
    RestHighLevelClient client();
  }
}
