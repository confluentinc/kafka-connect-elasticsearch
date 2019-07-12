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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import java.util.Arrays;
import org.junit.Test;


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
    return props;
  }

  @Test
  public void testPutAndFlush() throws Exception {
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

    client.refresh();

    verifySearchResults(records, true, false);
  }

  @Test
  public void testCreateAndWriteToIndexForTopicWithUppercaseCharacters() {
    // We should as well test that writing a record with a previously un seen record will create
    // an index following the required elasticsearch requirements of lowercasing.
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

    assertFalse(UNSEEN_TOPIC + " index created without errors ",
        verifyIndexExist(UNSEEN_TOPIC.toLowerCase()));

  }

  private boolean verifyIndexExist(String index) {
    try {
       client.getMapping(index, TYPE);
       return true;
    } catch (Exception ex){
      return false;
    }
  }
}
