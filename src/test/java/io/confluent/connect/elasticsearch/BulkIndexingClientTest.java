/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.elasticsearch;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.searchbox.core.Bulk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BulkIndexingClientTest {

  @Test
  public void testSetParameters() throws Exception {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("pipeline", "pipeline_id_1");

    BulkIndexingClient bulkIndexingClient = new BulkIndexingClient(null, parameters);
    Key key = new Key("index", "test_type", "1");
    String payload = "{\"hello\": true}";
    IndexableRecord record = new IndexableRecord(key, payload, 1L);

    Bulk bulk = bulkIndexingClient.bulkRequest(Collections.singletonList(record));
    Object[] pipeline = bulk.getParameter("pipeline").toArray();

    assertEquals(pipeline.length, 1);
    assertEquals("pipeline_id_1", pipeline[0]);
    String uri = bulk.getURI();
    assertTrue(uri.contains("pipeline=pipeline_id_1"));
  }
}
