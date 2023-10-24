/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.connect.elasticsearch_2_4;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import io.confluent.connect.elasticsearch_2_4.index.mapping.DefaultIndexMapper;
import io.confluent.connect.elasticsearch_2_4.jest.JestElasticsearchClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static io.confluent.connect.elasticsearch_2_4.DataConverter.BehaviorOnNullValues;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG;

public class ElasticsearchSinkTestBase extends ESIntegTestCase {

  protected static final String TYPE = "kafka-connect";

  protected static final String TOPIC = "topic";
  protected static final int PARTITION = 12;
  protected static final int PARTITION2 = 13;
  protected static final int PARTITION3 = 14;
  protected static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);
  protected static final TopicPartition TOPIC_PARTITION2 = new TopicPartition(TOPIC, PARTITION2);
  protected static final TopicPartition TOPIC_PARTITION3 = new TopicPartition(TOPIC, PARTITION3);

  protected ElasticsearchClient client;
  protected HashMap<String, String> props;
  private DataConverter converter;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    converter = new DataConverter(true, BehaviorOnNullValues.IGNORE);
    props = new HashMap<>();
    props.put(CONNECTION_URL_CONFIG, "http://localhost:" + getPort());
    props.put(TYPE_NAME_CONFIG, ElasticsearchSinkTestBase.TYPE);
    props.put(IGNORE_KEY_CONFIG, "true");

    client = new JestElasticsearchClient(props, null);
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    if (client != null) {
      client.close();
    }
    client = null;
  }

  protected int getPort() {
    assertTrue("There should be at least 1 HTTP endpoint exposed in the test cluster",
               cluster().httpAddresses().length > 0);
    return cluster().httpAddresses()[0].getPort();
  }

  protected Struct createRecord(Schema schema) {
    Struct struct = new Struct(schema);
    struct.put("user", "Liquan");
    struct.put("message", "trying out Elastic Search.");
    return struct;
  }

  protected Schema createSchema() {
    return SchemaBuilder.struct().name("record")
        .field("user", Schema.STRING_SCHEMA)
        .field("message", Schema.STRING_SCHEMA)
        .build();
  }

  protected Schema createOtherSchema() {
    return SchemaBuilder.struct().name("record")
        .field("user", Schema.INT32_SCHEMA)
        .build();
  }

  protected Struct createOtherRecord(Schema schema) {
    Struct struct = new Struct(schema);
    struct.put("user", 10);
    return struct;
  }

  protected void verifySearchResults(Collection<SinkRecord> records, boolean ignoreKey, boolean ignoreSchema) throws IOException {
    JsonArray allHits = new JsonArray();
    Collection<String> indices = new HashSet<>();
    for (SinkRecord record: records) {
      indices.add(DefaultIndexMapper.convertTopicToIndexName(record.topic()));
    }
    for (String index: indices) {
      final JsonObject result = client.search("", index, null);

      final JsonArray rawHits = result.getAsJsonObject("hits").getAsJsonArray("hits");
      allHits.addAll(rawHits);
    }
    assertEquals(records.size(), allHits.size());

    Map<String, String> hits = new HashMap<>();
    for (int i = 0; i < allHits.size(); ++i) {
      final JsonObject hitData = allHits.get(i).getAsJsonObject();
      final String id = hitData.get("_id").getAsString();
      final String source = hitData.get("_source").getAsJsonObject().toString();
      hits.put(id, source);
    }

    for (Object record : records) {
      if (record instanceof SinkRecord) {
        String index = DefaultIndexMapper.convertTopicToIndexName(((SinkRecord) record).topic());
        IndexableRecord indexableRecord = converter.convertRecord((SinkRecord) record, index, TYPE, "", "", ignoreKey, ignoreSchema);
        assertEquals(indexableRecord.payload, hits.get(indexableRecord.key.id));
      } else {
        assertEquals(record, hits.get("key"));
      }
    }
  }

  /* For ES 2.x */
  @Override
  protected Settings nodeSettings(int nodeOrdinal) {
    return Settings.settingsBuilder()
        .put(super.nodeSettings(nodeOrdinal))
        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
        .put(Node.HTTP_ENABLED, true)
        .build();
  }
}
