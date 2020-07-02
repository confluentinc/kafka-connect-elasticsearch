/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.connect.elasticsearch.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.*;

@Category(IntegrationTest.class)
public class ElasticsearchSinkConnectorIT extends BaseConnectorIT {

  private static final String KAFKA_TOPIC = "topic";

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkConnectorIT.class);

  private static final String CONNECTOR_NAME = "elasticsearch-sink-connector";
  private static final int NUM_RECORDS = 100;
  private static final int MAX_TASKS = 1;

  private final ElasticsearchIntegrationTestBase util = new ElasticsearchIntegrationTestBase();

  private final JsonConverter converterWithSchemaEnabled = new JsonConverter();

  public static DockerComposeContainer pumbaContainer;
  private void startPumbaContainer() {
    pumbaContainer =
        new DockerComposeContainer(new File("src/test/docker/configA/pumba-docker-compose.yml"));
    pumbaContainer.start();
  }

  @Before
  public void setup() throws IOException {
    Map<String, String> config = new HashMap<>();
    config.put(JsonConverterConfig.SCHEMAS_CACHE_SIZE_CONFIG, "100");
    config.put(ConverterConfig.TYPE_CONFIG, ConverterType.KEY.getName());
    converterWithSchemaEnabled.configure(config);
    converterWithSchemaEnabled.close();
    startEScontainer();
    startConnect();
    setUp();
  }

  @After
  public void close() {
    stopConnect();
    stopEScontainer();
  }

  @Test
  public void testElasticsearchConnectorSuccess() throws Throwable {
    connect.kafka().createTopic(KAFKA_TOPIC, 1);
    sendTestDataToKafka();
    ConsumerRecords<byte[], byte[]> totalRecords = connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());

    // Configure Connector and wait some specific time to start the connector.
    connect.configureConnector(CONNECTOR_NAME, getSinkConnectorProperties());
    waitForConnectorToStart(CONNECTOR_NAME, Integer.valueOf(MAX_TASKS));

    // Wait Connector to write data into elastic-search
    waitConnectorToWriteDataIntoElasticsearch(
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        NUM_RECORDS);
    verifySearchResults(totalRecords, KAFKA_TOPIC);

    sendTestDataToKafka();
    totalRecords = connect.kafka().consume(
        NUM_RECORDS * 2,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    // Wait Connector to write data into elastic-search
    waitConnectorToWriteDataIntoElasticsearch(
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        NUM_RECORDS * 2);
    verifySearchResults(totalRecords, KAFKA_TOPIC);
  }

  @Test
  public void testElasticsearchConnectorUnavailable() throws Throwable {
    startPumbaContainer();
    connect.kafka().createTopic(KAFKA_TOPIC, 1);
    sendTestDataToKafka();
    ConsumerRecords<byte[], byte[]> totalRecords = connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());

    // Configure Connector and wait some specific time to start the connector.
    connect.configureConnector(CONNECTOR_NAME, getSinkConnectorProperties());
    waitForConnectorToStart(CONNECTOR_NAME, Integer.valueOf(MAX_TASKS));

    // Wait Connector to write data into elastic-search
    waitConnectorToWriteDataIntoElasticsearch(
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        NUM_RECORDS);
    verifySearchResults(totalRecords, KAFKA_TOPIC);

    sendTestDataToKafka();
    totalRecords = connect.kafka().consume(
        NUM_RECORDS * 2,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    // Wait Connector to write data into elastic-search
    waitConnectorToWriteDataIntoElasticsearch(
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        NUM_RECORDS * 2);
    verifySearchResults(totalRecords, KAFKA_TOPIC);
    pumbaContainer.close();
  }

  private void sendTestDataToKafka() {
    String key = "key";
    Schema schema = util.createSchema();
    Struct record = util.createRecord(schema);
    byte[] value = converterWithSchemaEnabled.fromConnectData(KAFKA_TOPIC, record.schema(), record);

    produceRecordsToKafkaTopic(key, value, NUM_RECORDS);
  }

  private void produceRecordsToKafkaTopic(String key, byte[] value, long numberOfRecords) {
    // Send records to Kafka
    for (int i = 0; i < numberOfRecords; i++) {
      String valueString = new String(value, StandardCharsets.UTF_8);
      //log.debug("Sending message {} with topic {} to Kafka broker {}", kafkaTopic, valueString);
      connect.kafka().produce(KAFKA_TOPIC, key, valueString);
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private Map<String, String> getSinkConnectorProperties() {
    Map<String, String> props = new HashMap<>();
    props.put(SinkConnectorConfig.TOPICS_CONFIG, KAFKA_TOPIC);
    props.put(CONNECTOR_CLASS_CONFIG, ElasticsearchSinkConnector.class.getName());
    props.put(TASKS_MAX_CONFIG, Integer.toString(MAX_TASKS));
    props.put(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG, "kafka-connect");
    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, container.getConnectionUrl());
    props.put(ElasticsearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true");
    props.put(ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG, "10");
    props.put("value.converter", JsonConverter.class.getName());
    return props;
  }
}
