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
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.test.TestUtils;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.search.SearchHit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.*;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class ElasticsearchConnectorIT extends ElasticsearchConnectorBaseIT {
  // TODO: test compatibility
  
  private static final String TOPIC_1 = "users-topic";
  private static final String TOPIC_2 = "orders-topic";
  private static final String INDEX_1 = "users-index-1";
  private static final String INDEX_2 = "users-index-2";
  private static final String INDEX_3 = "orders-index-1";
  private static final String INDEX_4 = "orders-index-2";
  // Our data stream names follow the pattern "logs-{dataset}-{namespace}" to leverage the default "logs-*-*" template.
  // If you need to use custom data stream names, ensure the corresponding index template exists first.
  private static final String DATA_STREAM_1 = "logs-users-1";
  private static final String DATA_STREAM_2 = "logs-users-2";
  private static final String DATA_STREAM_3 = "logs-orders-1";
  private static final String DATA_STREAM_4 = "logs-orders-2";
  private static final String ALIAS_1 = "users-alias";
  private static final String ALIAS_2 = "orders-alias";

  // Constants for string formats to avoid duplication
  private static final String TOPIC_RESOURCE_MAPPING_FORMAT = "%s:%s,%s:%s";
  private static final String TOPICS_LIST_FORMAT = "%s,%s";

  // Constants for record types to avoid duplication
  private static final String USERS_RECORD_TYPE = "users";
  private static final String ORDERS_RECORD_TYPE = "orders";

  @BeforeClass
  public static void setupBeforeAll() {
    Map<User, String> users = getUsers();
    List<Role> roles = getRoles();
    container = ElasticsearchContainer.fromSystemProperties().withBasicAuth(users, roles);
    container.start();
  }

  @Override
  public void setup() throws Exception {
    if (!container.isRunning()) {
      setupBeforeAll();
    }
    super.setup();
  }

  @Override
  public void cleanup() throws Exception {
    // Clean up all resources created by tests before calling super cleanup
    if (container != null && container.isRunning() && helperClient != null) {
      // Clean up aliases, indices, and data streams
      String[] aliases = {ALIAS_1, ALIAS_2};
      String[] indices = {INDEX_1, INDEX_2, INDEX_3, INDEX_4};
      String[] dataStreams = {DATA_STREAM_1, DATA_STREAM_2, DATA_STREAM_3, DATA_STREAM_4};

      // Delete aliases and indices (isDataStream = false)
      for (String alias : aliases) {
        safeDeleteIndex(alias, false);
      }
      for (String index : indices) {
        safeDeleteIndex(index, false);
      }

      // Delete data streams (isDataStream = true)
      for (String dataStream : dataStreams) {
        safeDeleteIndex(dataStream, true);
      }
    }

    // Call parent cleanup
    super.cleanup();
  }

  private void safeDeleteIndex(String name, boolean isDataStream) {
    try {
      helperClient.deleteIndex(name, isDataStream);
    } catch (Exception e) {
      // Ignore if resource doesn't exist - this is expected during cleanup
    }
  }

  @Override
  protected Map<String, String> createProps() {
    props = super.createProps();
    props.put(CONNECTION_USERNAME_CONFIG, ELASTIC_MINIMAL_PRIVILEGES_NAME);
    props.put(CONNECTION_PASSWORD_CONFIG, ELASTIC_MINIMAL_PRIVILEGES_PASSWORD);
    return props;
  }

  /**
   * Verify that mapping errors when an index has strict mapping is handled correctly
   */
  @Test
  public void testStrictMappings() throws Exception {
    helperClient.createIndex(TOPIC, "{ \"dynamic\" : \"strict\", " +
        " \"properties\": { \"longProp\": { \"type\": \"long\" } } } }");

    props.put(ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG, "1");
    props.put(ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG, "1");
    props.put(ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "2");
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    connect.kafka().produce(TOPIC, "key1", "{\"longProp\":1}");
    connect.kafka().produce(TOPIC, "key2", "{\"any-prop\":1}");
    connect.kafka().produce(TOPIC, "key3", "{\"any-prop\":1}");
    connect.kafka().produce(TOPIC, "key4", "{\"any-prop\":1}");

    await().atMost(Duration.ofMinutes(1)).untilAsserted(() ->
        assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).state())
            .isEqualTo("FAILED"));

    // The framework commits offsets right before failing the task, verify the failed record's
    // offset is not included
    assertThat(getConnectorOffset(CONNECTOR_NAME, TOPIC, 0)).isLessThanOrEqualTo(1);
  }

  private long getConnectorOffset(String connectorName, String topic, int partition)
      throws Exception {
    String cGroupName = "connect-" + connectorName;
    ListConsumerGroupOffsetsResult offsetsResult = connect.kafka().createAdminClient()
        .listConsumerGroupOffsets(cGroupName);
    OffsetAndMetadata offsetAndMetadata = offsetsResult.partitionsToOffsetAndMetadata().get()
        .get(new TopicPartition(topic, partition));
    return offsetAndMetadata == null ? 0 : offsetAndMetadata.offset();
  }

  @Test
  public void testBatchByByteSize() throws Exception {
    // Based on the size of the topic, key, and value strings in JSON format.
    int approximateRecordByteSize = 60;
    props.put(BULK_SIZE_BYTES_CONFIG, Integer.toString(approximateRecordByteSize * 2));
    props.put(LINGER_MS_CONFIG, "180000");

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    writeRecords(3);
    // Only 2 records fit in 1 batch. The other record is sent once another record is written.
    verifySearchResults(2);

    writeRecords(1);
    verifySearchResults(4);
  }

  @Test
  public void testStopESContainer() throws Exception {
    props.put(ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG, "2");
    props.put(ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG, "1");
    props.put(ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG,
        Integer.toString(NUM_RECORDS - 1));

    // run connector and write
    runSimpleTest(props);

    // stop ES, for all following requests to fail with "connection refused"
    container.stop();

    // try to write some more
    writeRecords(NUM_RECORDS);

    // Connector should fail since the server is down
    await().atMost(Duration.ofMinutes(1)).untilAsserted(() ->
        assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).state())
            .isEqualTo("FAILED"));

    assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).trace())
        .contains("'java.net.ConnectException: Connection refused' after 3 attempt(s)");
  }

  @Test
  public void testChangeConfigsAndRestart() throws Exception {
    // run connector and write
    runSimpleTest(props);

    // restart
    props.put(BATCH_SIZE_CONFIG, "10");
    props.put(LINGER_MS_CONFIG, "1000");
    connect.configureConnector(CONNECTOR_NAME, props);

    // write some more
    writeRecords(NUM_RECORDS);
    verifySearchResults(NUM_RECORDS * 2);
  }

  @Test
  public void testDelete() throws Exception {
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, ElasticsearchSinkConnectorConfig.BehaviorOnNullValues.DELETE.name());
    props.put(IGNORE_KEY_CONFIG, "false");
    runSimpleTest(props);

    // should have 5 records at this point
    // try deleting last one
    int lastRecord = NUM_RECORDS - 1;
    connect.kafka().produce(TOPIC, String.valueOf(lastRecord), null);

    // should have one less records
    verifySearchResults(NUM_RECORDS - 1);
  }

  @Test
  public void testHappyPath() throws Exception {
    runSimpleTest(props);
  }

  @Test
  public void testHappyPathDataStream() throws Exception {
    setDataStream();

    runSimpleTest(props);

    if (container.esMajorVersion() == 8) {
      assertEquals(index, helperClient.getDataStreamWithJavaAPIClient(index).name());
    } else {
      assertEquals(index, helperClient.getDataStream(index).getName());
    }
  }

  @Test
  public void testNullValue() throws Exception {
    runSimpleTest(props);

    // should have 5 records at this point
    // try writing null value
    connect.kafka().produce(TOPIC, String.valueOf(NUM_RECORDS), null);

    // should still have 5 records
    verifySearchResults(NUM_RECORDS);
  }

  /*
   * Currently writing primitives to ES fails because ES expects a JSON document and the connector
   * does not wrap primitives in any way into a JSON document.
   */
  @Test
  public void testPrimitive() throws Exception {
    props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    for (int i = 0; i < NUM_RECORDS; i++) {
      connect.kafka().produce(TOPIC, String.valueOf(i), String.valueOf(i));
    }

    waitForRecords(0);
  }

  @Test
  public void testUpsert() throws Exception {
    props.put(WRITE_METHOD_CONFIG, ElasticsearchSinkConnectorConfig.WriteMethod.UPSERT.toString());
    props.put(IGNORE_KEY_CONFIG, "false");
    runSimpleTest(props);

    // should have 10 records at this point
    // try updating last one
    int lastRecord = NUM_RECORDS - 1;
    connect.kafka().produce(TOPIC, String.valueOf(lastRecord), String.format("{\"doc_num\":%d}", 0));
    writeRecordsFromStartIndex(NUM_RECORDS, NUM_RECORDS);

    // should have double number of records
    verifySearchResults(NUM_RECORDS * 2);

    for (SearchHit hit : helperClient.search(TOPIC)) {
      if (Integer.parseInt(hit.getId()) == lastRecord) {
        // last record should be updated
        int docNum = (Integer) hit.getSourceAsMap().get("doc_num");
        assertEquals(0, docNum);
      }
    }
  }

  private void testBackwardsCompatibilityDataStreamVersionHelper(
      String version
  ) throws Exception {
    // since we require older ES container we close the current one and set it back up
    container.close();
    container = ElasticsearchContainer.withESVersion(version);
    container.start();
    setupFromContainer();

    runSimpleTest(props);

    helperClient = null;
    container.close();
    setupBeforeAll();
  }

  // Disabled backward compatibility tests due to cgroupv2 issues with older ES versions
  //  @Test
  public void testBackwardsCompatibilityDataStream() throws Exception {
    testBackwardsCompatibilityDataStreamVersionHelper("7.0.1");
  }

  //  @Test
  public void testBackwardsCompatibilityDataStream2() throws Exception {
    testBackwardsCompatibilityDataStreamVersionHelper("7.9.3");
  }

  //  @Test
  public void testBackwardsCompatibility() throws Exception {
    testBackwardsCompatibilityDataStreamVersionHelper("7.16.3");
  }

  @Test
  public void testRoutingSmtSynchronousMode() throws Exception {
    index = addRoutingSmt("YYYYMM", "route-it-to-here-${topic}-at-${timestamp}");
    props.put(FLUSH_SYNCHRONOUSLY_CONFIG, "true");
    runSimpleTest(props);
    waitForCommittedOffsets(CONNECTOR_NAME, TOPIC, 0, NUM_RECORDS);
  }

  @Test
  public void testRoutingSmtAsynchronousMode() throws Exception {
    index = addRoutingSmt("YYYYMM", "route-it-to-here-${topic}-at-${timestamp}");
    props.put(FLUSH_SYNCHRONOUSLY_CONFIG, "false");
    assertConnectorFailsOnWriteRecords(props, "Connector doesn't support topic mutating SMTs");
  }

  @Test
  public void testReconfigureToUseRoutingSMT() throws Exception {
    props.put(FLUSH_SYNCHRONOUSLY_CONFIG, "false");
    // run a connector without a routing SMT in asynchronous mode
    runSimpleTest(props);
    // reconfigure connector to use a routing SMT in synchronous mode
    props.put(FLUSH_SYNCHRONOUSLY_CONFIG, "true");
    index = addRoutingSmt("YYYYMM", "route-it-to-here-${topic}-at-${timestamp}");
    runSimpleTest(props);
    waitForCommittedOffsets(CONNECTOR_NAME, TOPIC, 0, NUM_RECORDS * 2);
    // reconfigure connector to use a routing SMT in asynchronous mode
    props.put(FLUSH_SYNCHRONOUSLY_CONFIG, "false");
    assertConnectorFailsOnWriteRecords(props, "Connector doesn't support topic mutating SMTs");
  }

  public void waitForCommittedOffsets(String connectorName, String topicName, int partition, int expectedOffset) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> expectedOffset == getConnectorOffset(connectorName, topicName, partition),
        CONNECTOR_COMMIT_DURATION_MS,
        "Connector tasks did not commit offsets in time."
    );
  }

  private String addRoutingSmt(String timestampFormat, String topicFormat) {
    SimpleDateFormat formatter = new SimpleDateFormat(timestampFormat);
    Date date = new Date(System.currentTimeMillis());
    props.put("transforms", "TimestampRouter");
    props.put("transforms.TimestampRouter.type", "org.apache.kafka.connect.transforms.TimestampRouter");
    props.put("transforms.TimestampRouter.topic.format", topicFormat);
    props.put("transforms.TimestampRouter.timestamp.format", timestampFormat);
    return topicFormat.replace("${topic}", TOPIC).replace("${timestamp}", formatter.format(date));
  }

  @Test
  public void testResourceMappingMultipleTopicsToIndices() throws Exception {
    setupResourceConfigs(ExternalResourceUsage.INDEX);

    // Write records to each topic
    writeRecordsToTopic(TOPIC_1, USERS_RECORD_TYPE, 3);
    writeRecordsToTopic(TOPIC_2, ORDERS_RECORD_TYPE, 3);

    // Wait for records to be processed
    await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> {
      assertThat(helperClient.search(INDEX_1)).hasSize(3);
      assertThat(helperClient.search(INDEX_3)).hasSize(3);
    });
  }

  @Test
  public void testResourceMappingMultipleTopicsToDataStreams() throws Exception {
    setupResourceConfigs(ExternalResourceUsage.DATASTREAM);

    // Write records to each topic
    writeRecordsToTopic(TOPIC_1, USERS_RECORD_TYPE, 3);
    writeRecordsToTopic(TOPIC_2, ORDERS_RECORD_TYPE, 3);

    // Wait for records to be processed
    await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> {
      assertThat(helperClient.search(DATA_STREAM_1)).hasSize(3);
      assertThat(helperClient.search(DATA_STREAM_3)).hasSize(3);
    });
  }

  @Test
  public void testMultiTopicToMultiAliasWithRollover() throws Exception {
    setupResourceConfigs(ExternalResourceUsage.ALIAS_INDEX);

    // Write records to each topic
    writeRecordsToTopic(TOPIC_1, USERS_RECORD_TYPE, 3);
    writeRecordsToTopic(TOPIC_2, ORDERS_RECORD_TYPE, 3);

    // Wait for records to be processed
    await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> {
      assertThat(helperClient.search(INDEX_1)).hasSize(3);
      assertThat(helperClient.search(INDEX_2)).hasSize(0);
      assertThat(helperClient.search(INDEX_3)).hasSize(3);
      assertThat(helperClient.search(INDEX_4)).hasSize(0);
    });

    helperClient.updateAlias(INDEX_1, INDEX_2, ALIAS_1, INDEX_2);
    helperClient.updateAlias(INDEX_3, INDEX_4, ALIAS_2, INDEX_4);

    // Write more records
    writeRecordsToTopic(TOPIC_1, USERS_RECORD_TYPE, 2);
    writeRecordsToTopic(TOPIC_2, ORDERS_RECORD_TYPE, 2);

    // Wait for records to be processed
    await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> {
      assertThat(helperClient.search(INDEX_1)).hasSize(3);
      assertThat(helperClient.search(INDEX_2)).hasSize(2);
      assertThat(helperClient.search(INDEX_3)).hasSize(3);
      assertThat(helperClient.search(INDEX_4)).hasSize(2);
    });
  }

  @Test
  public void testMultiTopicToMultiDataStreamAliasWithRollover() throws Exception {
    setupResourceConfigs(ExternalResourceUsage.ALIAS_DATASTREAM);

    // Write records to each topic
    writeRecordsToTopic(TOPIC_1, USERS_RECORD_TYPE, 3);
    writeRecordsToTopic(TOPIC_2, ORDERS_RECORD_TYPE, 3);

    // Wait for records to be processed
    await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> {
      assertThat(helperClient.search(DATA_STREAM_1)).hasSize(3);
      assertThat(helperClient.search(DATA_STREAM_2)).hasSize(0);
      assertThat(helperClient.search(DATA_STREAM_3)).hasSize(3);
      assertThat(helperClient.search(DATA_STREAM_4)).hasSize(0);
    });

    // Perform rollover - switch write index to the second data stream
    helperClient.updateAlias(DATA_STREAM_1, DATA_STREAM_2, ALIAS_1, DATA_STREAM_2);
    helperClient.updateAlias(DATA_STREAM_3, DATA_STREAM_4, ALIAS_2, DATA_STREAM_4);

    // Write more records
    writeRecordsToTopic(TOPIC_1, USERS_RECORD_TYPE, 2);
    writeRecordsToTopic(TOPIC_2, ORDERS_RECORD_TYPE, 2);

    // Wait for records to be processed
    await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> {
      assertThat(helperClient.search(DATA_STREAM_1)).hasSize(3);
      assertThat(helperClient.search(DATA_STREAM_2)).hasSize(2);
      assertThat(helperClient.search(DATA_STREAM_3)).hasSize(3);
      assertThat(helperClient.search(DATA_STREAM_4)).hasSize(2);
    });
  }

  // Helper methods for writing different types of records
  private void writeRecordsToTopic(String topic, String recordType, int numRecords) {
    for (int i = 0; i < numRecords; i++) {
      String record;
      switch (recordType) {
        case USERS_RECORD_TYPE:
          record = String.format("{\"user_id\":\"user_%d\",\"name\":\"User %d\",\"email\":\"user%d@example.com\"}", i, i, i);
          break;
        case ORDERS_RECORD_TYPE:
          record = String.format("{\"order_id\":\"order_%d\",\"user_id\":\"user_%d\",\"amount\":%.2f}", i, i, 100.0 + i * 10);
          break;
        default:
          record = String.format("{\"id\":%d,\"data\":\"test_data_%d\"}", i, i);
      }
      connect.kafka().produce(topic, String.valueOf(i), record);
    }
  }

  // Helper methods for verifying data
  private void setupResourceConfigs(ExternalResourceUsage resourceType) throws IOException, InterruptedException {
    connect.kafka().createTopic(TOPIC_1);
    connect.kafka().createTopic(TOPIC_2);

    switch (resourceType) {
      case INDEX:
        props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.INDEX.name());
        props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG,
            String.format(TOPIC_RESOURCE_MAPPING_FORMAT, TOPIC_1, INDEX_1, TOPIC_2, INDEX_3));
        props.put(TOPICS_CONFIG, String.format(TOPICS_LIST_FORMAT, TOPIC_1, TOPIC_2));
        helperClient.createIndexesWithoutMapping(INDEX_1, INDEX_3);
        break;
      case DATASTREAM:
        props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.DATASTREAM.name());
        props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG,
            String.format(TOPIC_RESOURCE_MAPPING_FORMAT, TOPIC_1, DATA_STREAM_1, TOPIC_2, DATA_STREAM_3));
        props.put(TOPICS_CONFIG, String.format(TOPICS_LIST_FORMAT, TOPIC_1, TOPIC_2));
        helperClient.createDataStreams(DATA_STREAM_1, DATA_STREAM_3);
        break;
      case ALIAS_INDEX:
        props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.ALIAS_INDEX.name());
        props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG,
            String.format(TOPIC_RESOURCE_MAPPING_FORMAT, TOPIC_1, ALIAS_1, TOPIC_2, ALIAS_2));
        props.put(TOPICS_CONFIG, String.format(TOPICS_LIST_FORMAT, TOPIC_1, TOPIC_2));
        helperClient.createIndexesWithoutMapping(INDEX_1, INDEX_2, INDEX_3, INDEX_4);
        helperClient.updateAlias(INDEX_1, INDEX_2, ALIAS_1, INDEX_1);
        helperClient.updateAlias(INDEX_3, INDEX_4, ALIAS_2, INDEX_3);
        break;
      case ALIAS_DATASTREAM:
        props.put(EXTERNAL_RESOURCE_USAGE_CONFIG, ExternalResourceUsage.ALIAS_DATASTREAM.name());
        props.put(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG,
            String.format(TOPIC_RESOURCE_MAPPING_FORMAT, TOPIC_1, ALIAS_1, TOPIC_2, ALIAS_2));
        props.put(TOPICS_CONFIG, String.format(TOPICS_LIST_FORMAT, TOPIC_1, TOPIC_2));
        helperClient.createDataStreams(DATA_STREAM_1, DATA_STREAM_2, DATA_STREAM_3, DATA_STREAM_4);
        helperClient.updateAlias(DATA_STREAM_1, DATA_STREAM_2, ALIAS_1, DATA_STREAM_1);
        helperClient.updateAlias(DATA_STREAM_3, DATA_STREAM_4, ALIAS_2, DATA_STREAM_3);
        break;
    }
    // Start the connector
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);
  }
}
