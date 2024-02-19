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

public class Validator {

  private static final Logger log = LoggerFactory.getLogger(Validator.class);

  private static final String CONNECTOR_V11_COMPATIBLE_ES_VERSION = "7.0.0";
  private static final String DATA_STREAM_COMPATIBLE_ES_VERSION = "7.9.0";

  private ElasticsearchSinkConnectorConfig config;
  private Map<String, ConfigValue> values;
  private List<ConfigValue> validations;
  private ClientFactory clientFactory;

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
    validateDataStreamConfigs();
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

    if (config.isDataStream() && config.writeMethod() == WriteMethod.UPSERT) {
      String errorMessage = String.format(
          "Upserts are not supported with data streams. %s must not be %s if %s and %s are set.",
          WRITE_METHOD_CONFIG,
          WriteMethod.UPSERT,
          DATA_STREAM_TYPE_CONFIG,
          DATA_STREAM_DATASET_CONFIG
      );
      addErrorMessage(WRITE_METHOD_CONFIG, errorMessage);
    }

    if (config.isDataStream() && config.behaviorOnNullValues() == BehaviorOnNullValues.DELETE) {
      String errorMessage = String.format(
          "Deletes are not supported with data streams. %s must not be %s if %s and %s are set.",
          BEHAVIOR_ON_NULL_VALUES_CONFIG,
          BehaviorOnNullValues.DELETE,
          DATA_STREAM_TYPE_CONFIG,
          DATA_STREAM_DATASET_CONFIG
      );
      addErrorMessage(BEHAVIOR_ON_NULL_VALUES_CONFIG, errorMessage);
    }

    if (!config.isDataStream() && !config.dataStreamTimestampField().isEmpty()) {
      String errorMessage = String.format(
          "Mapping a field to the '@timestamp' field is only necessary for data streams. "
              + "%s must not be set if %s and %s are not set.",
          DATA_STREAM_TIMESTAMP_CONFIG,
          DATA_STREAM_TYPE_CONFIG,
          DATA_STREAM_DATASET_CONFIG
      );
      addErrorMessage(DATA_STREAM_TIMESTAMP_CONFIG, errorMessage);
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
