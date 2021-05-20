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

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_DATASET_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.DATA_STREAM_TYPE_CONFIG;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import io.confluent.connect.elasticsearch.helper.ElasticsearchHelperClient;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.test.TestUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

public class ElasticsearchConnectorBaseIT extends BaseConnectorIT {

  protected static final int NUM_RECORDS = 5;
  protected static final int TASKS_MAX = 1;
  protected static final String CONNECTOR_NAME = "es-connector";
  protected static final String TOPIC = "test";

  protected static ElasticsearchContainer container;

  protected boolean isDataStream;
  protected ElasticsearchHelperClient helperClient;
  protected Map<String, String> props;
  protected String index;

  @AfterClass
  public static void cleanupAfterAll() {
    container.close();
  }

  @Before
  public void setup() {
    index = TOPIC;
    isDataStream = false;

    startConnect();
    connect.kafka().createTopic(TOPIC);

    props = createProps();
    helperClient = new ElasticsearchHelperClient(
        container.getConnectionUrl(),
        new ElasticsearchSinkConnectorConfig(props)
    );
  }

  @After
  public void cleanup() throws IOException {
    stopConnect();

    if (helperClient != null) {
      helperClient.deleteIndex(index, isDataStream);
      helperClient.close();
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

  protected void setDataStream() {
    isDataStream = true;
    props.put(DATA_STREAM_TYPE_CONFIG, "logs");
    props.put(DATA_STREAM_DATASET_CONFIG, "dataset");
    index = "logs-dataset-" + TOPIC;
  }

  protected void setupFromContainer() {
    String address = container.getConnectionUrl();
    props.put(CONNECTION_URL_CONFIG, address);
    helperClient = new ElasticsearchHelperClient(
        props.get(CONNECTION_URL_CONFIG),
        new ElasticsearchSinkConnectorConfig(props)
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
    for (int i  = start; i < start + numRecords; i++) {
      connect.kafka().produce(
          TOPIC,
          String.valueOf(i),
          String.format("{\"doc_num\":%d,\"@timestamp\":\"2021-04-28T11:11:22.%03dZ\"}", i, i)
      );
    }
  }
}
