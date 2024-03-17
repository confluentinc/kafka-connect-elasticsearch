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

import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.confluent.kafka.schemaregistry.ClusterTestHarness.KAFKASTORE_TOPIC;

@RunWith(Parameterized.class)
public class ElasticsearchConnectorDataFormatIT extends ElasticsearchConnectorBaseIT {

  protected void startSchemaRegistry() throws Exception {
    int port = findAvailableOpenPort();
    restApp = new RestApp(port, null, connect.kafka().bootstrapServers(),
        KAFKASTORE_TOPIC, CompatibilityLevel.NONE.name, true, new Properties());
    restApp.start();
  }

  protected void stopSchemaRegistry() throws Exception {
    restApp.stop();
  }

  protected void waitForSchemaRegistryToStart() throws InterruptedException {
    TestUtils.waitForCondition(
        () -> restApp.restServer.isRunning(),
        CONNECTOR_STARTUP_DURATION_MS,
        "Schema-registry server did not start in time."
    );
  }

  private Converter converter;
  private Class<? extends Converter> converterClass;

  @Override
  public void setup() throws Exception {
    super.setup();
    startSchemaRegistry();
  }

  @Override
  public void cleanup() throws Exception {
    super.cleanup();
    stopSchemaRegistry();}

  @Parameters
  public static List<Class<? extends Converter>> data() {
    return Arrays.asList(JsonSchemaConverter.class, ProtobufConverter.class, AvroConverter.class);
  }

  public ElasticsearchConnectorDataFormatIT(Class<? extends Converter> converter) throws Exception {
    this.converterClass = converter;
    this.converter = converterClass.getConstructor().newInstance();
  }

  @Test
  public void testHappyPathDataFormat() throws Exception {
    // configure configs and converter with schema-registry addr
    props.put("value.converter", converterClass.getSimpleName());
    props.put("value.converter.schema.registry.url", restApp.restServer.getURI().toString());
    props.put("value.converter.scrub.invalid.names", "true");
    converter.configure(Collections.singletonMap(
            "schema.registry.url", restApp.restServer.getURI().toString()
        ), false
    );

    // start the connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for schema-registry to spin up
    waitForSchemaRegistryToStart();

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // run test
    writeRecords(NUM_RECORDS);
    verifySearchResults(NUM_RECORDS);
  }

  @Override
  protected void writeRecords(int numRecords) {
    writeRecordsFromIndex(0, numRecords, converter);
  }

  protected void writeRecordsFromIndex(int start, int numRecords, Converter converter) {
    // get defined schema for the test
    Schema schema = getRecordSchema();

    // configure producer with default properties
    KafkaProducer<byte[], byte[]> producer = configureProducer();

    List<SchemaAndValue> recordsList = getRecords(schema, start, numRecords);

    // produce records into topic
    produceRecords(producer, converter, recordsList, TOPIC);
  }

  private Integer findAvailableOpenPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  protected List<SchemaAndValue> getRecords(Schema schema, int start, int numRecords) {
    List<SchemaAndValue> recordList = new ArrayList<>();
    for (int i = start; i < start + numRecords; i++) {
      Struct struct = new Struct(schema);
      struct.put("doc_num", i);
      SchemaAndValue schemaAndValue = new SchemaAndValue(schema, struct);
      recordList.add(schemaAndValue);
    }
    return recordList;
  }

  protected void produceRecords(
      KafkaProducer<byte[], byte[]> producer,
      Converter converter,
      List<SchemaAndValue> recordsList,
      String topic
  ) {
    for (int i = 0; i < recordsList.size(); i++) {
      SchemaAndValue schemaAndValue = recordsList.get(i);
      byte[] convertedStruct = converter.fromConnectData(topic, schemaAndValue.schema(), schemaAndValue.value());
      ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(topic, 0, String.valueOf(i).getBytes(), convertedStruct);
      try {
        producer.send(msg).get(TimeUnit.SECONDS.toMillis(120), TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        throw new KafkaException("Could not produce message: " + msg, e);
      }
    }
  }

  protected KafkaProducer<byte[], byte[]> configureProducer() {
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    return new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer());
  }

  protected Schema getRecordSchema() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.field("doc_num", Schema.INT32_SCHEMA);
    return schemaBuilder.build();
  }
}
