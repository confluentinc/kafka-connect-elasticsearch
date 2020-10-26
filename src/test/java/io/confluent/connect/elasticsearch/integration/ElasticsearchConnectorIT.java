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

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static org.junit.Assert.assertNotNull;

import com.google.gson.JsonObject;
import io.confluent.connect.elasticsearch.DataConverter.BehaviorOnNullValues;
import io.confluent.connect.elasticsearch.Mapping;
import io.confluent.connect.elasticsearch.TestUtils;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.test.IntegrationTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTest.class)
public class ElasticsearchConnectorIT extends ElasticsearchConnectorBaseIT {

  private static Logger log = LoggerFactory.getLogger(ElasticsearchConnectorIT.class);

  @BeforeClass
  public static void setupBeforeAll() {
    container = ElasticsearchContainer.fromSystemProperties();
    container.start();
  }

  @Test
  public void testDelete() throws Exception {
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.DELETE.name());
    props.put(IGNORE_KEY_CONFIG, "false");
    runSimpleTest(props);

    // should have 10 records at this point
    // try deleting last one
    int lastRecord = 9;
    connect.kafka().produce(TOPIC, String.valueOf(lastRecord), null);

    // should have one less records
    verifySearchResults(9);
  }

  @Test
  public void testHappyPath() throws Exception {
    runSimpleTest(props);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMapping() throws Exception {
    client.createIndices(Collections.singleton(TOPIC));
    Schema schema = TestUtils.createSchema();
    Mapping.createMapping(client, TOPIC, TYPE, schema);

    JsonObject mapping = Mapping.getMapping(client, TOPIC, TYPE);
    assertNotNull(mapping);
    TestUtils.verifyMapping(client, schema, mapping);
  }
}
