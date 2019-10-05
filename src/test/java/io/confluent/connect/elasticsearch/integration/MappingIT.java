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

import static org.junit.Assert.assertNotNull;

import com.google.gson.JsonObject;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.Mapping;
import io.confluent.connect.elasticsearch.TestUtils;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class MappingIT extends ElasticsearchIntegrationTestBase {

  private static final String INDEX = "kafka-connect";
  private static final String TYPE = "kafka-connect-type";

  @Test(expected = ConnectException.class)
  @SuppressWarnings("unchecked")
  public void testMapping() throws Exception {

    client.createIndices(Collections.singleton(INDEX));
    Schema schema = TestUtils.createSchema();
    Mapping.createMapping(client, INDEX, TYPE, schema);

    JsonObject mapping = Mapping.getMapping(client, INDEX, TYPE);
    assertNotNull(mapping);
    TestUtils.verifyMapping(client, schema, mapping);

    Mapping.getMapping(client, INDEX, "another-mapping-type");
  }

}
