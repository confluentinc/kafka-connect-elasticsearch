/*
 * Copyright 2025 Confluent Inc.
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

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.ElasticsearchStatusException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import io.confluent.connect.elasticsearch.helper.ElasticsearchHelperClient;
import org.apache.kafka.test.TestUtils;

public abstract class ElasticsearchClientTestBase {

  protected static final String INDEX = "index";
  protected static final String TOPIC = "index";
  protected static final String DATA_STREAM_TYPE = "logs";
  protected static final String DATA_STREAM_DATASET = "dataset";

  protected static ElasticsearchContainer container;
  protected DataConverter converter;
  protected ElasticsearchHelperClient helperClient;
  protected ElasticsearchSinkConnectorConfig config;
  protected Map<String, String> props;
  protected String index;
  protected OffsetTracker offsetTracker;

  @Before
  public void setup() {
    index = TOPIC;
    props = ElasticsearchSinkConnectorConfigTest.addNecessaryProps(new HashMap<>());
    props.put(CONNECTION_URL_CONFIG, container.getConnectionUrl());
    props.put(IGNORE_KEY_CONFIG, "true");
    props.put(LINGER_MS_CONFIG, "1000");
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);
    helperClient = new ElasticsearchHelperClient(container.getConnectionUrl(), config,
        container.shouldStartClientInCompatibilityMode());
    helperClient.waitForConnection(30000);
    offsetTracker = mock(OffsetTracker.class);
  }

  @After
  public void cleanup() throws IOException {
    if (helperClient != null && helperClient.indexExists(index)){
      helperClient.deleteIndex(index, config.isDataStream());
    }
  }

  @AfterClass
  public static void cleanupAfterAll() {
    container.close();
  }

  protected Schema schema() {
    return SchemaBuilder
        .struct()
        .name("record")
        .field("offset", SchemaBuilder.int32().defaultValue(0).build())
        .field("another", SchemaBuilder.int32().defaultValue(0).build())
        .build();
  }

  protected SinkRecord sinkRecord(int offset) {
    return sinkRecord("key", offset);
  }

  protected SinkRecord sinkRecord(String key, int offset) {
    Struct value = new Struct(schema()).put("offset", offset).put("another", offset + 1);
    return sinkRecord(key, schema(), value, offset);
  }

  protected SinkRecord sinkRecord(String key, Schema schema, Struct value, int offset) {
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

  protected void waitUntilRecordsInES(int expectedRecords) throws InterruptedException {
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

  protected void writeRecord(SinkRecord record, ElasticsearchClient client) {
    client.index(record, converter.convertRecord(record, createIndexName(record.topic())),
            new AsyncOffsetTracker.AsyncOffsetState(record.kafkaOffset()));
  }

  protected String createIndexName(String name) {
    return config.isDataStream()
        ? String.format("%s-%s-%s", DATA_STREAM_TYPE, DATA_STREAM_DATASET, name)
        : name;
  }
} 