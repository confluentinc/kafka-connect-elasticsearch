///*
// * Copyright 2018 Confluent Inc.
// *
// * Licensed under the Confluent Community License (the "License"); you may not use
// * this file except in compliance with the License.  You may obtain a copy of the
// * License at
// *
// * http://www.confluent.io/confluent-community-license
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations under the License.
// */
//
//package io.confluent.connect.elasticsearch_2_4;
//
//import static org.mockito.Mockito.mock;
//
//import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
//
//import org.apache.kafka.common.TopicPartition;
//import org.apache.kafka.connect.data.Schema;
//import org.apache.kafka.connect.data.Struct;
//import org.apache.kafka.connect.sink.SinkRecord;
//import org.apache.kafka.connect.sink.SinkTaskContext;
//import org.elasticsearch.action.ActionFuture;
//import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
//import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
//import org.elasticsearch.test.ESIntegTestCase;
//import org.elasticsearch.test.InternalTestCluster;
//import org.junit.Test;
//
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Collection;
//import java.util.ArrayList;
//import java.util.Collections;
//
//
//@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
//public class ElasticsearchSinkTaskTest extends ElasticsearchSinkTestBase {
//
//  private static final String TOPIC_IN_CAPS = "AnotherTopicInCaps";
//  private static final int PARTITION_113 = 113;
//  private static final TopicPartition TOPIC_IN_CAPS_PARTITION = new TopicPartition(TOPIC_IN_CAPS, PARTITION_113);
//
//  private static final String UNSEEN_TOPIC = "UnseenTopic";
//  private static final int PARTITION_114 = 114;
//  private static final TopicPartition UNSEEN_TOPIC_PARTITION = new TopicPartition(UNSEEN_TOPIC, PARTITION_114);
//
//
//  private Map<String, String> createProps() {
//    Map<String, String> props = new HashMap<>();
//    props.put(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG, TYPE);
//    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost:19300");
//    props.put(ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG, "true");
//    props.put(ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG, "3000");
//    return props;
//  }
//
//  @Test
//  public void testPutAndFlush() throws Exception {
//    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
//    cluster.ensureAtLeastNumDataNodes(3);
//    Map<String, String> props = createProps();
//
//    ElasticsearchSinkTask task = new ElasticsearchSinkTask();
//    task.initialize(mock(SinkTaskContext.class));
//    task.start(props);
//    task.open(new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3)));
//
//    String key = "key";
//    Schema schema = createSchema();
//    Struct record = createRecord(schema);
//
//    Collection<SinkRecord> records = new ArrayList<>();
//    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 0);
//    records.add(sinkRecord);
//
//    sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1);
//    records.add(sinkRecord);
//
//    task.put(records);
//    task.flush(null);
//
//    refresh();
//
//    verifySearchResults(records, true, false);
//  }
//
//  @Test
//  public void testCreateAndWriteToIndexForTopicWithUppercaseCharacters() {
//    // We should as well test that writing a record with a previously un seen record will create
//    // an index following the required elasticsearch requirements of lowercasing.
//    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
//    cluster.ensureAtLeastNumDataNodes(3);
//    Map<String, String> props = createProps();
//
//    ElasticsearchSinkTask task = new ElasticsearchSinkTask();
//    task.initialize(mock(SinkTaskContext.class));
//
//    String key = "key";
//    Schema schema = createSchema();
//    Struct record = createRecord(schema);
//
//    SinkRecord sinkRecord = new SinkRecord(TOPIC_IN_CAPS,
//        PARTITION_113,
//        Schema.STRING_SCHEMA,
//        key,
//        schema,
//        record,
//        0 );
//
//    try {
//      task.start(props);
//      task.open(new HashSet<>(Collections.singletonList(TOPIC_IN_CAPS_PARTITION)));
//      task.put(Collections.singleton(sinkRecord));
//    } catch (Exception ex) {
//      fail("A topic name not in lowercase can not be used as index name in Elasticsearch");
//    } finally {
//      task.stop();
//    }
//  }
//
//  @Test
//  public void testCreateAndWriteToIndexNotCreatedAtStartTime() {
//    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
//    cluster.ensureAtLeastNumDataNodes(3);
//    Map<String, String> props = createProps();
//
//    props.put(ElasticsearchSinkConnectorConfig.CREATE_INDICES_AT_START_CONFIG, "false");
//
//    ElasticsearchSinkTask task = new ElasticsearchSinkTask();
//    task.initialize(mock(SinkTaskContext.class));
//
//    String key = "key";
//    Schema schema = createSchema();
//    Struct record = createRecord(schema);
//
//    SinkRecord sinkRecord = new SinkRecord(UNSEEN_TOPIC,
//        PARTITION_114,
//        Schema.STRING_SCHEMA,
//        key,
//        schema,
//        record,
//        0 );
//
//    task.start(props);
//    task.open(new HashSet<>(Collections.singletonList(TOPIC_IN_CAPS_PARTITION)));
//    task.put(Collections.singleton(sinkRecord));
//    task.stop();
//
//    assertTrue(UNSEEN_TOPIC + " index created without errors ",
//        verifyIndexExist(cluster, UNSEEN_TOPIC.toLowerCase()));
//
//  }
//
//  @Test
//  public void testStopThenFlushDoesNotThrow() {
//    ElasticsearchSinkTask task = new ElasticsearchSinkTask();
//    task.initialize(mock(SinkTaskContext.class));
//    task.start(createProps());
//    task.stop();
//    task.flush(new HashMap<>());
//  }
//
//  private boolean verifyIndexExist(InternalTestCluster cluster, String ... indices) {
//    ActionFuture<IndicesExistsResponse> action = cluster
//        .client()
//        .admin()
//        .indices()
//        .exists(new IndicesExistsRequest(indices));
//
//    return action.actionGet().isExists();
//  }
//}
