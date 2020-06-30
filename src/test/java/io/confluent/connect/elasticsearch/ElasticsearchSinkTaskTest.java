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

package io.confluent.connect.elasticsearch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertEquals;


@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ElasticsearchSinkTaskTest extends ElasticsearchSinkTestBase {

  private static final String TOPIC_IN_CAPS = "AnotherTopicInCaps";
  private static final int PARTITION_113 = 113;
  private static final TopicPartition TOPIC_IN_CAPS_PARTITION = new TopicPartition(TOPIC_IN_CAPS, PARTITION_113);

  private static final String UNSEEN_TOPIC = "UnseenTopic";
  private static final int PARTITION_114 = 114;
  private static final TopicPartition UNSEEN_TOPIC_PARTITION = new TopicPartition(UNSEEN_TOPIC, PARTITION_114);


  private Map<String, String> createProps() {
    Map<String, String> props = new HashMap<>();
    props.put(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG, TYPE);
    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "localhost");
    props.put(ElasticsearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true");
    props.put(ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG, "3000");
    return props;
  }

  @Test
  public void testPutAndFlush() throws Exception {
    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(3);
    Map<String, String> props = createProps();

    ElasticsearchSinkTask task = new ElasticsearchSinkTask();
    task.start(props, client);
    task.open(new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3)));

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 0);
    records.add(sinkRecord);

    sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1);
    records.add(sinkRecord);

    task.put(records);
    task.flush(null);

    refresh();

    verifySearchResults(records, true, false);
  }

  @Test
  public void testVersionedDeletes() throws Exception {
    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(3);
    int numOfRecords = 100;
    Map<String, String> props = createProps();
    props.put(ElasticsearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, Integer.toString(numOfRecords));
    props.put(ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG, "1");
    props.put(ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG, "1");
    props.put(ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "delete");

    ElasticsearchSinkTask task = new ElasticsearchSinkTask();
    task.start(props, client);
    task.open(new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3)));

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Collection<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < numOfRecords - 1 ; i++) {
      if (i % 2 == 0) {
        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, null, i);
        records.add(sinkRecord);
      } else {
        record.put("message", Integer.toString(i));
        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, i);
        records.add(sinkRecord);
      }
    }
    record.put("message", Integer.toString(numOfRecords));
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, numOfRecords);
    records.add(sinkRecord);

    task.put(records);
    task.flush(null);

    refresh();

    final JsonObject result = client.search("", TOPIC, null);
    final JsonArray rawHits = result.getAsJsonObject("hits").getAsJsonArray("hits");
    assertEquals(1, rawHits.size());
    String message = rawHits
            .get(0).getAsJsonObject()
            .get("_source").getAsJsonObject()
            .get("message").getAsString();
    assertEquals("100", message);
  }

  @Test
  public void testCreateAndWriteToIndexForTopicWithUppercaseCharacters() {
    // We should as well test that writing a record with a previously un seen record will create
    // an index following the required elasticsearch requirements of lowercasing.
    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(3);
    Map<String, String> props = createProps();

    ElasticsearchSinkTask task = new ElasticsearchSinkTask();

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    SinkRecord sinkRecord = new SinkRecord(TOPIC_IN_CAPS,
        PARTITION_113,
        Schema.STRING_SCHEMA,
        key,
        schema,
        record,
        0 );

    try {
      task.start(props, client);
      task.open(new HashSet<>(Collections.singletonList(TOPIC_IN_CAPS_PARTITION)));
      task.put(Collections.singleton(sinkRecord));
    } catch (Exception ex) {
      fail("A topic name not in lowercase can not be used as index name in Elasticsearch");
    } finally {
      task.stop();
    }
  }

  @Test
  public void testCreateAndWriteToIndexNotCreatedAtStartTime() {
    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(3);
    Map<String, String> props = createProps();

    props.put(ElasticsearchSinkConnectorConfig.AUTO_CREATE_INDICES_AT_START_CONFIG, "false");

    ElasticsearchSinkTask task = new ElasticsearchSinkTask();

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    SinkRecord sinkRecord = new SinkRecord(UNSEEN_TOPIC,
        PARTITION_114,
        Schema.STRING_SCHEMA,
        key,
        schema,
        record,
        0 );

    task.start(props, client);
    task.open(new HashSet<>(Collections.singletonList(TOPIC_IN_CAPS_PARTITION)));
    task.put(Collections.singleton(sinkRecord));
    task.stop();

    assertTrue(UNSEEN_TOPIC + " index created without errors ",
        verifyIndexExist(cluster, UNSEEN_TOPIC.toLowerCase()));

  }

  private boolean verifyIndexExist(InternalTestCluster cluster, String ... indices) {
    ActionFuture<IndicesExistsResponse> action = cluster
        .client()
        .admin()
        .indices()
        .exists(new IndicesExistsRequest(indices));

    return action.actionGet().isExists();
  }
}
