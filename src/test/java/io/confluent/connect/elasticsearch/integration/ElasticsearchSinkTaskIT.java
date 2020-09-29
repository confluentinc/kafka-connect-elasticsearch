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

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.confluent.connect.elasticsearch.ElasticsearchSinkTask;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.MDC;

@Category(IntegrationTest.class)
public class ElasticsearchSinkTaskIT extends ElasticsearchIntegrationTestBase {

  private ElasticsearchSinkTask task = new ElasticsearchSinkTask();

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
    props.put(ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG, "true");
    return props;
  }

  @Test
  public void testPutAndFlush() throws Exception {

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
}
