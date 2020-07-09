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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;

@Category(IntegrationTest.class)
public class ElasticSearchSinkConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(ElasticSearchSinkConnectorIT.class);

  private static final int NUM_RECORDS = 10000;
  Map<String, String> props;

  @Before
  public void setup() throws IOException {
    startConnect();
    connect.kafka().createTopic(KAFKA_TOPIC, 1);
    Map<String, String> config = new HashMap<>();
    config.put(JsonConverterConfig.SCHEMAS_CACHE_SIZE_CONFIG, "100");
    config.put(ConverterConfig.TYPE_CONFIG, ConverterType.KEY.getName());
    startEScontainer();
    props = getSinkConnectorProperties();
    setUp();
  }

  @After
  public void close() {
    stopConnect();
    stopEScontainer();
  }

  @Test
  public void testSuccess() throws Throwable {
    sendTestDataToKafka(NUM_RECORDS);
    ConsumerRecords<byte[], byte[]> totalRecords = connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());

    // Configure Connector and wait some specific time to start the connector.
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, Integer.valueOf(MAX_TASKS));

    // Wait Connector to write data into elastic-search
    waitConnectorToWriteDataIntoElasticsearch(
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        NUM_RECORDS);
    assertRecordsCountAndContent(NUM_RECORDS, KAFKA_TOPIC);
  }

  @Test
  public void testForElasticSearchServerUnavailability() throws Throwable {
    startPumbaPauseContainer();
    sendTestDataToKafka(NUM_RECORDS);
    ConsumerRecords<byte[], byte[]> totalRecords = connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());

    // Configure Connector and wait some specific time to start the connector.
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, Integer.valueOf(MAX_TASKS));

    // Wait Connector to write data into elastic-search
    waitConnectorToWriteDataIntoElasticsearch(
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        NUM_RECORDS);
    assertRecordsCountAndContent(NUM_RECORDS, KAFKA_TOPIC);
    pumbaPauseContainer.close();
  }

  @Test
  public void testForElasticSearchServerDelay() throws Throwable {
    startPumbaDelayContainer();
    sendTestDataToKafka(NUM_RECORDS);
    ConsumerRecords<byte[], byte[]> totalRecords = connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());

    // Configure Connector and wait some specific time to start the connector.
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, Integer.valueOf(MAX_TASKS));

    // Wait Connector to write data into elastic-search
    waitConnectorToWriteDataIntoElasticsearch(
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        NUM_RECORDS);
    assertRecordsCountAndContent(NUM_RECORDS, KAFKA_TOPIC);
    pumbaDelayContainer.close();
  }

  private void sendTestDataToKafka(int numRecords) throws InterruptedException {
    for (int i = 0; i < numRecords; i++) {
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
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, String> config = new HashMap<>();
    config.put(JsonConverterConfig.SCHEMAS_CACHE_SIZE_CONFIG, "100");
    config.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
    config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
    jsonConverter.configure(config);
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
    return props;
  }
}
