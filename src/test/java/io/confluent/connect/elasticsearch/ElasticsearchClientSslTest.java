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

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SECURITY_PROTOCOL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SSL_CONFIG_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.ElasticsearchStatusException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SecurityProtocol;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import io.confluent.connect.elasticsearch.helper.ElasticsearchHelperClient;
import org.apache.kafka.test.TestUtils;

public class ElasticsearchClientSslTest extends ElasticsearchClientTest {

  private static final String INDEX = "index";
  private static final String ELASTIC_SUPERUSER_NAME = "elastic";
  private static final String ELASTIC_SUPERUSER_PASSWORD = "elastic";
  private static final String TOPIC = "index";
  private static final String DATA_STREAM_TYPE = "logs";
  private static final String DATA_STREAM_DATASET = "dataset";

  private static ElasticsearchContainer container;

  private DataConverter converter;
  private ElasticsearchHelperClient helperClient;
  private ElasticsearchSinkConnectorConfig config;
  private Map<String, String> props;
  private String index;
  private OffsetTracker offsetTracker;

  @BeforeClass
  public static void setupBeforeAll() {
    container = ElasticsearchContainer.fromSystemProperties().withSslEnabled(true);
    container.start();
  }

  @AfterClass
  public static void cleanupAfterAll() {
    container.close();
  }

  @Override
  @Before
  public void setup() {
    index = TOPIC;
    props = ElasticsearchSinkConnectorConfigTest.addNecessaryProps(new HashMap<>());
    String address = container.getConnectionUrl(false);
    props.put(CONNECTION_URL_CONFIG, address);
    props.put(CONNECTION_USERNAME_CONFIG, ELASTIC_SUPERUSER_NAME);
    props.put(CONNECTION_PASSWORD_CONFIG, ELASTIC_SUPERUSER_PASSWORD);
    props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, container.getKeystorePath());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, container.getKeystorePassword());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, container.getTruststorePath());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, container.getTruststorePassword());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEY_PASSWORD_CONFIG, container.getKeyPassword());
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);
    helperClient = new ElasticsearchHelperClient(address, config,
        container.shouldStartClientInCompatibilityMode());
    helperClient.waitForConnection(30000);
    offsetTracker = mock(OffsetTracker.class);
  }

  @Override
  @After
  public void cleanup() throws IOException {
    super.cleanup();
  }

  @Test
  public void testSsl() throws Exception {
    ElasticsearchClient client = new ElasticsearchClient(config, null, () -> offsetTracker.updateOffsets());
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    client.flush();

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(index));
    client.close();
  }

  private static Schema schema() {
    return SchemaBuilder
        .struct()
        .name("record")
        .field("offset", SchemaBuilder.int32().defaultValue(0).build())
        .field("another", SchemaBuilder.int32().defaultValue(0).build())
        .build();
  }

  private static SinkRecord sinkRecord(int offset) {
    return sinkRecord("key", offset);
  }

  private static SinkRecord sinkRecord(String key, int offset) {
    Struct value = new Struct(schema()).put("offset", offset).put("another", offset + 1);
    return sinkRecord(key, schema(), value, offset);
  }

  private static SinkRecord sinkRecord(String key, Schema schema, Struct value, int offset) {
    return new SinkRecord(
        TOPIC,
        0,
        Schema.STRING_SCHEMA,
        key,
        schema,
        value,
        offset,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME
    );
  }

  private void waitUntilRecordsInES(int expectedRecords) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> {
          try {
            return helperClient.getDocCount(index) == expectedRecords;
          } catch (ElasticsearchStatusException e) {
            if (e.getMessage().contains("index_not_found_exception")) {
              return false;
            }
            throw e;
          }
        },
        TimeUnit.MINUTES.toMillis(1),
        String.format("Could not find expected documents (%d) in time.", expectedRecords)
    );
  }

  private void writeRecord(SinkRecord record, ElasticsearchClient client) {
    client.index(record, converter.convertRecord(record, createIndexName(record.topic())),
            new AsyncOffsetTracker.AsyncOffsetState(record.kafkaOffset()));
  }

  private String createIndexName(String name) {
    return config.isDataStream()
        ? String.format("%s-%s-%s", DATA_STREAM_TYPE, DATA_STREAM_DATASET, name)
        : name;
  }
} 