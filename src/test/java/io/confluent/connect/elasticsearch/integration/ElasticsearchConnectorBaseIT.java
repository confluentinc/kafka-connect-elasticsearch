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

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.test.TestUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.client.security.user.privileges.Role.Builder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import io.confluent.connect.elasticsearch.helper.ElasticsearchHelperClient;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_DATASET_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_TYPE_CONFIG;
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

  public static final String ELASTIC_DATA_STREAM_MINIMAL_PRIVILEGES_NAME = "bob";
  public static final String ELASTIC_DS_MINIMAL_PRIVILEGES_PASSWORD = "PeachesInGeorgia";

  private static final String ES_SINK_CONNECTOR_ROLE = "es_sink_connector_role";
  private static final String ES_SINK_CONNECTOR_DS_ROLE = "es_sink_connector_ds_role";

  protected static ElasticsearchContainer container;

  protected boolean isDataStream;
  protected ElasticsearchHelperClient helperClient;
  protected Map<String, String> props;
  protected String index;

  @BeforeClass
  public static void setupBeforeAll() throws Exception {
    container = ElasticsearchContainer.fromSystemProperties();
    container.start();
  }

  @AfterClass
  public static void cleanupAfterAll() {
    container.close();
  }

  @Before
  public void setup() throws Exception {
    index = TOPIC;
    isDataStream = false;
    startConnect();
    connect.kafka().createTopic(TOPIC);

    props = createProps();
    helperClient = container.getHelperClient(props);
  }

  @After
  public void cleanup() throws Exception {
    stopConnect();

    if (container != null && container.isRunning()) {
      if (helperClient != null) {
        try {
          helperClient.deleteIndex(index, isDataStream);
          helperClient.close();
        } catch (ConnectException e) {
          // Server is already down. No need to close
        } catch (ElasticsearchStatusException e) {
          if (RestStatus.NOT_FOUND.equals(e.status())) {
            // index wasn't created, nothing to clean
          } else {
            throw e;
          }
        }
      }
    }
  }

  protected Map<String, String> createProps() {
    Map<String, String> props = new HashMap<>();

    // generic configs
    props.put(CONNECTOR_CLASS_CONFIG, ElasticsearchSinkConnector.class.getSimpleName());
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

  protected void assertConnectorFailsOnWriteRecords(Map<String, String> props, String trace) throws Exception {
    // start the connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    writeRecords(NUM_RECORDS);
    waitForConnectorToFail(CONNECTOR_NAME, 1, trace);
  }

  protected void setDataStream() {
    isDataStream = true;
    props.put(DATA_STREAM_TYPE_CONFIG, "logs");
    props.put(DATA_STREAM_DATASET_CONFIG, "dataset");
    index = "logs-dataset-" + TOPIC;
    props.put(CONNECTION_USERNAME_CONFIG, ELASTIC_DATA_STREAM_MINIMAL_PRIVILEGES_NAME);
    props.put(CONNECTION_PASSWORD_CONFIG, ELASTIC_DS_MINIMAL_PRIVILEGES_PASSWORD);
  }

  protected void setupFromContainer() {
    String address = container.getConnectionUrl();
    props.put(CONNECTION_URL_CONFIG, address);
    helperClient = new ElasticsearchHelperClient(
        props.get(CONNECTION_URL_CONFIG),
        new ElasticsearchSinkConnectorConfig(props),
        container.shouldStartClientInCompatibilityMode()
    );
  }

  protected void verifySearchResults(int numRecords) throws Exception {
    waitForRecords(numRecords);

    for (SearchHit hit : helperClient.search(index)) {
      int id = (Integer) hit.getSourceAsMap().get("doc_num");
      assertNotNull(id);
      assertTrue(id < numRecords);

      if (isDataStream) {
        assertTrue(hit.getIndex().contains(index));
      } else {
        assertEquals(index, hit.getIndex());
      }
    }
  }

  protected void waitForRecords(int numRecords) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> {
          try {
            return helperClient.getDocCount(index) == numRecords;
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
    writeRecordsFromStartIndex(0, numRecords);
  }

  protected void writeRecordsFromStartIndex(int start, int numRecords) {
    for (int i = start; i < start + numRecords; i++) {
      connect.kafka().produce(
          TOPIC,
          String.valueOf(i),
          String.format("{\"doc_num\":%d,\"@timestamp\":\"2021-04-28T11:11:22.%03dZ\"}", i, i)
      );
    }
  }

  protected static List<Role> getRoles() {
    List<Role> roles = new ArrayList<>();
    roles.add(getMinimalPrivilegesRole(false));
    roles.add(getMinimalPrivilegesRole(true));
    return roles;
  }

  protected static Map<User, String> getUsers() {
    Map<User, String> users = new HashMap<>();
    users.put(getMinimalPrivilegesUser(true), getMinimalPrivilegesPassword(true));
    users.put(getMinimalPrivilegesUser(false), getMinimalPrivilegesPassword(false));
    return users;
  }

  private static Role getMinimalPrivilegesRole(boolean forDataStream) {
    IndicesPrivileges.Builder indicesPrivilegesBuilder = IndicesPrivileges.builder();
    IndicesPrivileges indicesPrivileges = indicesPrivilegesBuilder
        .indices("*")
        .privileges("create_index", "read", "write", "view_index_metadata")
        .build();
    // Historically (i.e. ES the previous test base version 7.9.3), ES_SINK_CONNECTOR_ROLE would not require the
    // "monitor" cluster privilege.  However, this has changed for 7.16.3, although leaving the surrounding
    // logic in place in the case that future ES versions or tests wish to diverge the permissions.
    return Role.builder()
        .name(forDataStream ? ES_SINK_CONNECTOR_DS_ROLE : ES_SINK_CONNECTOR_ROLE)
        .indicesPrivileges(indicesPrivileges)
        .clusterPrivileges("monitor")
        .build();
  }

  private static User getMinimalPrivilegesUser(boolean forDataStream) {
    return new User(forDataStream ? ELASTIC_DATA_STREAM_MINIMAL_PRIVILEGES_NAME : ELASTIC_MINIMAL_PRIVILEGES_NAME,
        Collections.singletonList(forDataStream ? ES_SINK_CONNECTOR_DS_ROLE : ES_SINK_CONNECTOR_ROLE));
  }

  private static String getMinimalPrivilegesPassword(boolean forDataStream) {
    return forDataStream ? ELASTIC_DS_MINIMAL_PRIVILEGES_PASSWORD : ELASTIC_MINIMAL_PRIVILEGES_PASSWORD;
  }
}
