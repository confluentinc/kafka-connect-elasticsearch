/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.connect.elasticsearch.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.elasticsearch.common.Strings;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;

@Category(IntegrationTest.class)
public class ElasticSearchSinkConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(ElasticSearchSinkConnectorIT.class);

  private static final int NUM_RECORDS = 10000;
  private Map<String, String> props;
  private JsonConverter jsonConverter = new JsonConverter();

  @Before
  public void setup() {
    startConnect();
    connect.kafka().createTopic(KAFKA_TOPIC, 1);
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put(JsonConverterConfig.SCHEMAS_CACHE_SIZE_CONFIG, "100");
    converterConfig.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
    converterConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
    jsonConverter.configure(converterConfig);
    startEScontainer();
    props = getSinkConnectorProperties();
    super.setUp(props);
  }

  @After
  public void close() {
    stopConnect();
    stopESContainer();
  }

  @Test
  public void testSuccess() throws InterruptedException, IOException {
    sendTestDataToKafka(0, NUM_RECORDS);
    ConsumerRecords<byte[], byte[]> totalRecords = connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());

    // Configure Connector and wait some specific time to start the connector.
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, Integer.valueOf(MAX_TASKS));

    // Wait Connector to write data into elasticsearch
    waitForConnectorToWriteDataIntoES(
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        NUM_RECORDS);
    assertRecordsCountAndContent(NUM_RECORDS, NUM_RECORDS, KAFKA_TOPIC);
  }

  @Test
  public void testForElasticSearchServerUnavailability() throws InterruptedException, IOException {
    // Pause all processes that is running  within target container('es-container')
    startPumbaPauseContainer();
    sendTestDataToKafka(0, NUM_RECORDS);
    ConsumerRecords<byte[], byte[]> totalRecords = connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());

    // Configure Connector and wait some specific time to start the connector.
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, Integer.valueOf(MAX_TASKS));

    // Wait Connector to write data into elasticsearch
    waitForConnectorToWriteDataIntoES(
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        NUM_RECORDS);
    pumbaPauseContainer.close();
    assertRecordsCountAndContent(NUM_RECORDS, NUM_RECORDS, KAFKA_TOPIC);
  }

  @Test
  public void testForElasticSearchServerDelay() throws InterruptedException, IOException {
    // Delay egress traffic for specified container('es-container')
    startPumbaDelayContainer();
    sendTestDataToKafka(0, NUM_RECORDS);
    ConsumerRecords<byte[], byte[]> totalRecords = connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());

    // Configure Connector and wait some specific time to start the connector.
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, Integer.valueOf(MAX_TASKS));

    // Wait Connector to write data into elasticsearch
    waitForConnectorToWriteDataIntoES(
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        NUM_RECORDS);
    pumbaDelayContainer.close();
    assertRecordsCountAndContent(NUM_RECORDS, NUM_RECORDS, KAFKA_TOPIC);
  }

  @Test
  public void testCredentialChange() throws InterruptedException, IOException {
    int numRecords = 50;
    sendTestDataToKafka(0, numRecords);
    ConsumerRecords<byte[], byte[]> totalRecords = connect.kafka().consume(
        numRecords,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());

    // Configure Connector and wait some specific time to start the connector.
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, Integer.valueOf(MAX_TASKS));

    // Wait Connector to write data into elasticsearch
    waitForConnectorToWriteDataIntoES(
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        numRecords);
    assertRecordsCountAndContent(numRecords, numRecords, KAFKA_TOPIC);

    int status = changeESPassword("elastic", "elastic1");
    Assert.assertEquals(200, status);

    sendTestDataToKafka(numRecords, numRecords);
    totalRecords = connect.kafka().consume(
        numRecords * 2,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());
    // Refresh ES client with new credential at test level to perform search operation
    refreshESClient();
    // Second batch of records should not be entertained due to credential change
    assertRecordsCountAndContent(numRecords * 2, numRecords, KAFKA_TOPIC);
    // Task must fail after credential change.
    waitForConnectorTaskToFail();
  }

  @Test
  public void testPermissionChange() throws InterruptedException, IOException {
    // Two batch of records to be processed
    int numRecords = 50;
    int status = createOrUpdateUserWithPermission(
        "test_user",
        "test_password",
        "superuser");
    Assert.assertEquals(200, status);

    // Run the connector with test user
    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG, "test_user");
    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG, "test_password");
    sendTestDataToKafka(0, numRecords);
    ConsumerRecords<byte[], byte[]> totalRecords = connect.kafka().consume(
        numRecords,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());

    // Configure Connector and wait some specific time to start the connector.
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, Integer.valueOf(MAX_TASKS));

    // Wait Connector to write data into elasticsearch
    waitForConnectorToWriteDataIntoES(
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        numRecords);
    assertRecordsCountAndContent(numRecords, numRecords, KAFKA_TOPIC);

    // Update the 'test_user' with read permissions
    status = createOrUpdateUserWithPermission(
        "test_user",
        "test_password",
        "monitoring_user");
    Assert.assertEquals(200, status);

    sendTestDataToKafka(numRecords, numRecords);
    totalRecords = connect.kafka().consume(
        numRecords * 2,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());

    // Second batch of records should not be entertained due to change in permissions
    assertRecordsCountAndContent(numRecords * 2, numRecords, KAFKA_TOPIC);
    // Task must fail after permission change.
    waitForConnectorTaskToFail();
  }

  private int createOrUpdateUserWithPermission(String username, String password, String role) throws IOException {
    try (CloseableHttpClient client = HttpClients.createDefault()) {
      String url = container.getConnectionUrl() + "/_xpack/security/user/" + username + "?pretty";
      String userPermission = (Strings.isNullOrEmpty(role)) ? "[]" : "[\"" + role + "\"]";
      String body = "{\"password\" : \"" + password + "\",\"roles\" : " + userPermission + "}";
      String authStr = "elastic:elastic";
      String authHeader = Base64.getEncoder().encodeToString((authStr).getBytes());
      HttpPost httpPost = new HttpPost(url);
      StringEntity requestEntity = new StringEntity(
          body,
          ContentType.APPLICATION_JSON);
      httpPost.setEntity(requestEntity);
      httpPost.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + authHeader);
      httpPost.setHeader("Content-type", "application/json");
      HttpResponse response = client.execute(httpPost);
      return response.getStatusLine().getStatusCode();
    }
  }

  private int changeESPassword(String oldPassword, String newPassword) throws IOException {
    try (CloseableHttpClient client = HttpClients.createDefault()) {
      String url = container.getConnectionUrl() + "/_security/user/elastic/_password?pretty";
      String body = "{\"password\" : \"" + newPassword + "\"}";
      String authStr = "elastic:" + oldPassword;
      String authHeader = Base64.getEncoder().encodeToString((authStr).getBytes());
      HttpPost httpPost = new HttpPost(url);
      StringEntity requestEntity = new StringEntity(
          body,
          ContentType.APPLICATION_JSON);
      httpPost.setEntity(requestEntity);
      httpPost.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + authHeader);
      httpPost.setHeader("Content-type", "application/json");
      HttpResponse response = client.execute(httpPost);
      return response.getStatusLine().getStatusCode();
    }
  }

  private void refreshESClient() {
    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG, "elastic");
    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG, "elastic1");
    super.setUp(props);
  }

  private void sendTestDataToKafka(int startIndex, int numRecords) {
    for (int i = startIndex; i < startIndex + numRecords; i++) {
      String value = getTestKafkaRecord(KAFKA_TOPIC, SCHEMA, i);
      connect.kafka().produce(KAFKA_TOPIC, null, value);
    }
  }

  private String getTestKafkaRecord(String topic, Schema schema, int i) {
    final Struct struct = new Struct(schema)
        .put("userId", i)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("message", "ElasticSearch message");
    byte[] raw = jsonConverter.fromConnectData(topic, schema, struct);
    return new String(raw, StandardCharsets.UTF_8);
  }

  private Map<String, String> getSinkConnectorProperties() {
    props = new HashMap<>();
    props.put(SinkConnectorConfig.TOPICS_CONFIG, KAFKA_TOPIC);
    props.put(CONNECTOR_CLASS_CONFIG, ElasticsearchSinkConnector.class.getName());
    props.put(TASKS_MAX_CONFIG, MAX_TASKS);
    props.put(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG, "kafka-connect");
    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, container.getConnectionUrl());
    props.put(ElasticsearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true");
    props.put(ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG, "10");
    props.put("value.converter", JsonConverter.class.getName());
    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG, "elastic");
    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG, "elastic");
    return props;
  }
}
