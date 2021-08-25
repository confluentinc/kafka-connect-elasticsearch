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

package io.confluent.connect.elasticsearch.integration;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.test.TestUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.client.security.user.privileges.Role.Builder;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import io.confluent.connect.elasticsearch.helper.ElasticsearchHelperClient;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ElasticsearchConnectorBaseIT extends BaseConnectorIT {

  protected static final int NUM_RECORDS = 5;
  protected static final int TASKS_MAX = 1;
  protected static final String CONNECTOR_NAME = "es-connector";
  protected static final String TOPIC = "test";

  // User that has a minimal required and documented set of privileges
  public static final String ELASTIC_MINIMAL_PRIVILEGES_NAME = "frank";
  public static final String ELASTIC_MINIMAL_PRIVILEGES_PASSWORD = "WatermelonInEasterHay";

  private static final String ES_SINK_CONNECTOR_ROLE = "es_sink_connector_role";

  protected static ElasticsearchContainer container;

  protected ElasticsearchHelperClient helperClient;
  protected Map<String, String> props;

  @AfterClass
  public static void cleanupAfterAll() {
    container.close();
  }

  @Before
  public void setup() {
    startConnect();
    connect.kafka().createTopic(TOPIC);

    props = createProps();
    helperClient = container.getHelperClient(props);
  }

  @After
  public void cleanup() throws IOException {
    stopConnect();

    if (container.isRunning()) {
      if (helperClient != null) {
        try {
          helperClient.deleteIndex(TOPIC);
          helperClient.close();
        } catch (ConnectException e) {
          // Server is already down. No need to close
        }
      }
    }
  }

  protected Map<String, String> createProps() {
    Map<String, String> props = new HashMap<>();

    // generic configs
    props.put(CONNECTOR_CLASS_CONFIG, ElasticsearchSinkConnector.class.getName());
    props.put(TOPICS_CONFIG, TOPIC);
    props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put("value.converter." + SCHEMAS_ENABLE_CONFIG, "false");

    // connectors specific
    props.put(CONNECTION_URL_CONFIG, container.getConnectionUrl());
    props.put(IGNORE_KEY_CONFIG, "true");
    props.put(IGNORE_SCHEMA_CONFIG, "true");

    return props;
  }

  protected void runSimpleTest(Map<String, String> props) throws Exception {
    // start the connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    writeRecords(NUM_RECORDS);

    verifySearchResults(NUM_RECORDS);
  }


  protected void verifySearchResults(int numRecords) throws Exception {
    waitForRecords(numRecords);

    for (SearchHit hit : helperClient.search(TOPIC)) {
      int id = (Integer) hit.getSourceAsMap().get("doc_num");
      assertNotNull(id);
      assertTrue(id < numRecords);
      assertEquals(TOPIC, hit.getIndex());
    }
  }

  protected void waitForRecords(int numRecords) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> {
          try {
            return helperClient.getDocCount(TOPIC) == numRecords;
          } catch (ElasticsearchStatusException e) {
            if (e.getMessage().contains("index_not_found_exception")) {
              return false;
            }

            throw e;
          }
        },
        CONSUME_MAX_DURATION_MS,
        "Sufficient amount of document were not found in ES on time."
    );
  }

  protected void writeRecords(int numRecords) {
    for (int i = 0; i < numRecords; i++) {
      connect.kafka().produce(TOPIC, String.valueOf(i), String.format("{\"doc_num\":%d}", i));
    }
  }

  protected void writeRecordsFromStartIndex(int start, int numRecords) {
    for (int i  = start; i < start + numRecords; i++) {
      connect.kafka().produce(TOPIC, String.valueOf(i), String.format("{\"doc_num\":%d}", i));
    }
  }

  protected static Role getMinimalPrivilegesRole() {
    IndicesPrivileges.Builder indicesPrivilegesBuilder = IndicesPrivileges.builder();
    IndicesPrivileges indicesPrivileges = indicesPrivilegesBuilder
        .indices("*")
        .privileges("create_index", "read", "write", "view_index_metadata")
        .build();
    Builder builder = Role.builder();
    Role role = builder.name(ES_SINK_CONNECTOR_ROLE).indicesPrivileges(indicesPrivileges).build();
    return role;
  }

  protected static User getMinimalPrivilegesUser() {
        return new User(ELASTIC_MINIMAL_PRIVILEGES_NAME,
            Collections.singletonList(ES_SINK_CONNECTOR_ROLE));
  }

  protected static String getMinimalPrivilegesPassword() {
    return ELASTIC_MINIMAL_PRIVILEGES_PASSWORD;
  }
}
