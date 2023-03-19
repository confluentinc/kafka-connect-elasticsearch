/*
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

package io.confluent.connect.elasticsearch_2_4;

import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.IGNORE_KEY_TOPICS_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_TOPICS_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.PROXY_HOST_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.PROXY_PASSWORD_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.PROXY_USERNAME_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.SECURITY_PROTOCOL_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.SSL_CONFIG_PREFIX;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.SslConfigs;

public class Validator {

  private ElasticsearchSinkConnectorConfig config;
  private Map<String, ConfigValue> values;
  private List<ConfigValue> validations;

  public Validator(Map<String, String> props) {
    try {
      this.config = new ElasticsearchSinkConnectorConfig(props);
    } catch (ConfigException e) {
      // some configs are invalid
    }

    validations = ElasticsearchSinkConnectorConfig.CONFIG.validate(props);
    values = validations.stream().collect(Collectors.toMap(ConfigValue::name, Function.identity()));
  }

  public Config validate() {
    if (config == null) {
      // individual configs are invalid, no point in validating combinations
      return new Config(validations);
    }

    validateCredentials();
    validateIgnoreConfigs();
    validateLingerMs();
    validateMaxBufferedRecords();
    validateProxy();
    validateSsl();

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
    if (!config.secured()) {
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

  private void addErrorMessage(String property, String error) {
    values.get(property).addErrorMessage(error);
  }
}
