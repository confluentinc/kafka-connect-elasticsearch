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

package io.confluent.connect.elasticsearch.integration;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.confluent.connect.elasticsearch.ElasticsearchSinkTask;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.MDC;

import java.util.*;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class ElasticsearchSinkTaskDeletesIT extends ElasticsearchIntegrationTestBase {

  private ElasticsearchSinkTask task = new ElasticsearchSinkTask();
  private int numOfRecords = 100;

  @Before
  public void beforeEach() {
    MDC.put("connector.context", "[MyConnector|task1] ");
    Map<String, String> props = createProps();
    task.start(props, client);
  }

  @After
  public void afterEach() {
    if (task != null) {
      task.stop();
    }
    MDC.remove("connector.context");
  }

  private Map<String, String> createProps() {
    Map<String, String> props = new HashMap<>();
    props.put(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG, TYPE);
    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, container.getConnectionUrl());
    props.put(ElasticsearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "false");
    props.put(ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, Integer.toString(numOfRecords));
    props.put(ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG, "1");
    props.put(ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG, "1");
    props.put(ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "delete");
    return props;
  }

  @Test
  public void testPutAndFlush() throws Exception {

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

    client.refresh();

    final JsonObject result = client.search("", TOPIC, null);
    final JsonArray rawHits = result.getAsJsonObject("hits").getAsJsonArray("hits");
    assertEquals(1, rawHits.size());
    String message = rawHits
            .get(0).getAsJsonObject()
            .get("_source").getAsJsonObject()
            .get("message").getAsString();
    assertEquals("100", message);
  }
}
