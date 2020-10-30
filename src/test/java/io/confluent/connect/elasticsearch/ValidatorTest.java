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
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_TOPICS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_TOPICS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Before;
import org.junit.Test;

public class ValidatorTest {

  private Map<String, String> props;
  private Validator validator;

  @Before
  public void setup() {
    props = new HashMap<>();
    props.put(TYPE_NAME_CONFIG, "type");
    props.put(CONNECTION_URL_CONFIG, "localhost:8080");
  }

  @Test
  public void testInvalidCredentials() {
    props.put(CONNECTION_USERNAME_CONFIG, "username");
    validator = new Validator(props);

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
  public void testValidCredentials() {
    // username and password not set
    validator = new Validator(props);

    Config result = validator.validate();
    assertNoErrors(result);

    // both set
    props.put(CONNECTION_USERNAME_CONFIG, "username");
    props.put(CONNECTION_PASSWORD_CONFIG, "password");
    validator = new Validator(props);

    result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidIgnoreConfigs() {
    props.put(IGNORE_KEY_CONFIG, "true");
    props.put(IGNORE_KEY_TOPICS_CONFIG, "some,topics");
    props.put(IGNORE_SCHEMA_CONFIG, "true");
    props.put(IGNORE_SCHEMA_TOPICS_CONFIG, "some,other,topics");
    validator = new Validator(props);

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
    validator = new Validator(props);

    Config result = validator.validate();
    assertNoErrors(result);

    // ignore configs are false
    props.put(IGNORE_KEY_CONFIG, "false");
    props.put(IGNORE_KEY_TOPICS_CONFIG, "some,topics");
    props.put(IGNORE_SCHEMA_CONFIG, "false");
    props.put(IGNORE_SCHEMA_TOPICS_CONFIG, "some,other,topics");
    validator = new Validator(props);

    result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidLingerMs() {
    props.put(LINGER_MS_CONFIG, "2");
    props.put(FLUSH_TIMEOUT_MS_CONFIG, "1");
    validator = new Validator(props);

    Config result = validator.validate();
    assertHasErrorMessage(result, LINGER_MS_CONFIG, "can not be larger than");
    assertHasErrorMessage(result, FLUSH_TIMEOUT_MS_CONFIG, "can not be larger than");
  }

  @Test
  public void testValidLingerMs() {
    props.put(LINGER_MS_CONFIG, "1");
    props.put(FLUSH_TIMEOUT_MS_CONFIG, "2");
    validator = new Validator(props);

    Config result = validator.validate();
    assertNoErrors(result);
  }

  @Test
  public void testInvalidMaxBufferedRecords() {
    props.put(MAX_BUFFERED_RECORDS_CONFIG, "1");
    props.put(BATCH_SIZE_CONFIG, "2");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "2");
    validator = new Validator(props);

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
    validator = new Validator(props);

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
}
