/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.elasticsearch.jest;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.confluent.connect.elasticsearch.ElasticsearchClient;
import io.confluent.connect.elasticsearch.IndexableRecord;
import io.confluent.connect.elasticsearch.Key;
import io.confluent.connect.elasticsearch.Mapping;
import io.confluent.connect.elasticsearch.bulk.BulkRequest;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.cluster.NodesInfo;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.equalTo;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JestElasticsearchClientTest {

  private static final String INDEX = "index";
  private static final String KEY = "key";
  private static final String TYPE = "type";
  private static final String QUERY = "query";

  private JestClient jestClient;
  private NodesInfo info;

  @Before
  public void setUp() throws Exception {
    jestClient = mock(JestClient.class);
    info = new NodesInfo.Builder().addCleanApiParameter("version").build();
    JsonObject nodeRoot = new JsonObject();
    nodeRoot.addProperty("version", "1.0");
    JsonObject nodesRoot = new JsonObject();
    nodesRoot.add("localhost", nodeRoot);
    JsonObject nodesInfo = new JsonObject();
    nodesInfo.add("nodes", nodesRoot);
    JestResult result = new JestResult(new Gson());
    result.setJsonObject(nodesInfo);
    when(jestClient.execute(info)).thenReturn(result);
  }

  @Test
  public void getsVersion() {
    JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
    assertThat(client.getVersion(), is(equalTo(ElasticsearchClient.Version.ES_V1)));
  }

  @Test
  public void createsIndices() throws Exception {
    JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
    JestResult failure = new JestResult(new Gson());
    failure.setSucceeded(false);
    JestResult success = new JestResult(new Gson());
    success.setSucceeded(true);
    IndicesExists indicesExists = new IndicesExists.Builder(INDEX).build();
    when(jestClient.execute(indicesExists)).thenReturn(failure);
    when(jestClient.execute(argThat(isCreateIndexForTestIndex()))).thenReturn(success);

    Set<String> indices = new HashSet<>();
    indices.add(INDEX);
    client.createIndices(indices);
    InOrder inOrder = inOrder(jestClient);
    inOrder.verify(jestClient).execute(info);
    inOrder.verify(jestClient).execute(indicesExists);
    inOrder.verify(jestClient).execute(argThat(isCreateIndexForTestIndex()));
  }

  private ArgumentMatcher<CreateIndex> isCreateIndexForTestIndex() {
    return new ArgumentMatcher<CreateIndex>() {
      @Override
      public boolean matches(CreateIndex createIndex) {
        // check the URI as the equals method on CreateIndex doesn't work
        return createIndex.getURI().equals(INDEX);
      }
    };
  }

  @Test(expected = ConnectException.class)
  public void createIndicesAndFails() throws Exception {
    JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
    JestResult failure = new JestResult(new Gson());
    failure.setSucceeded(false);
    IndicesExists indicesExists = new IndicesExists.Builder(INDEX).build();
    when(jestClient.execute(indicesExists)).thenReturn(failure);
    when(jestClient.execute(argThat(isCreateIndexForTestIndex()))).thenReturn(failure);

    Set<String> indices = new HashSet<>();
    indices.add(INDEX);
    client.createIndices(indices);
  }

  @Test
  public void createsMapping() throws Exception {
    JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
    JestResult success = new JestResult(new Gson());
    success.setSucceeded(true);
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set(TYPE, Mapping.inferMapping(client, Schema.STRING_SCHEMA));
    PutMapping putMapping = new PutMapping.Builder(INDEX, TYPE,obj.toString()).build();
    when(jestClient.execute(putMapping)).thenReturn(success);

    client.createMapping(INDEX, TYPE, Schema.STRING_SCHEMA);
    verify(jestClient).execute(putMapping);
  }

  @Test(expected = ConnectException.class)
  public void createsMappingAndFails() throws Exception {
    JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
    JestResult failure = new JestResult(new Gson());
    failure.setSucceeded(false);
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set(TYPE, Mapping.inferMapping(client, Schema.STRING_SCHEMA));
    PutMapping putMapping = new PutMapping.Builder(INDEX, TYPE, obj.toString()).build();
    when(jestClient.execute(putMapping)).thenReturn(failure);

    client.createMapping(INDEX, TYPE, Schema.STRING_SCHEMA);
  }

  @Test
  public void getsMapping() throws Exception {
    JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
    JsonObject mapping = new JsonObject();
    JsonObject mappings = new JsonObject();
    mappings.add(TYPE, mapping);
    JsonObject indexRoot = new JsonObject();
    indexRoot.add("mappings", mappings);
    JsonObject root = new JsonObject();
    root.add(INDEX, indexRoot);
    JestResult result = new JestResult(new Gson());
    result.setJsonObject(root);
    GetMapping getMapping = new GetMapping.Builder().addIndex(INDEX).addType(TYPE).build();
    when(jestClient.execute(getMapping)).thenReturn(result);

    assertThat(client.getMapping(INDEX, TYPE), is(equalTo(mapping)));
  }

  @Test
  public void executesBulk() throws Exception {
    JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
    IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
    List<IndexableRecord> records = new ArrayList<>();
    records.add(record);
    BulkRequest request = client.createBulkRequest(records);
    BulkResult success = new BulkResult(new Gson());
    success.setSucceeded(true);
    when(jestClient.execute(((JestBulkRequest) request).getBulk())).thenReturn(success);

    assertThat(client.executeBulk(request).isSucceeded(), is(equalTo(true)));
  }

  @Test
  public void executesBulkAndFails() throws Exception {
    JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
    IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), null, 0L);
    List<IndexableRecord> records = new ArrayList<>();
    records.add(record);
    BulkRequest request = client.createBulkRequest(records);
    BulkResult failure = new BulkResult(new Gson());
    failure.setSucceeded(false);
    when(jestClient.execute(((JestBulkRequest) request).getBulk())).thenReturn(failure);

    assertThat(client.executeBulk(request).isSucceeded(), is(equalTo(false)));
  }

  @Test
  public void executesBulkAndFailsWithParseError() throws Exception {
    JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
    IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
    List<IndexableRecord> records = new ArrayList<>();
    records.add(record);
    BulkRequest request = client.createBulkRequest(records);
    BulkResult failure = createBulkResultFailure(JestElasticsearchClient.MAPPER_PARSE_EXCEPTION);
    when(jestClient.execute(((JestBulkRequest) request).getBulk())).thenReturn(failure);

    assertThat(client.executeBulk(request).isSucceeded(), is(equalTo(false)));
  }

  @Test
  public void executesBulkAndFailsWithSomeOtherError() throws Exception {
    JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
    IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
    List<IndexableRecord> records = new ArrayList<>();
    records.add(record);
    BulkRequest request = client.createBulkRequest(records);
    BulkResult failure = createBulkResultFailure("some_random_exception");
    when(jestClient.execute(((JestBulkRequest) request).getBulk())).thenReturn(failure);

    assertThat(client.executeBulk(request).isSucceeded(), is(equalTo(false)));
  }

  @Test
  public void executesBulkAndSucceedsBecauseOnlyVersionConflicts() throws Exception {
    JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
    IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
    List<IndexableRecord> records = new ArrayList<>();
    records.add(record);
    BulkRequest request = client.createBulkRequest(records);
    BulkResult failure = createBulkResultFailure(JestElasticsearchClient.VERSION_CONFLICT_ENGINE_EXCEPTION);
    when(jestClient.execute(((JestBulkRequest) request).getBulk())).thenReturn(failure);

    assertThat(client.executeBulk(request).isSucceeded(), is(equalTo(true)));
  }

  @Test
  public void searches() throws Exception {
    JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
    Search search = new Search.Builder(QUERY).addIndex(INDEX).addType(TYPE).build();
    JsonObject queryResult = new JsonObject();
    SearchResult result = new SearchResult(new Gson());
    result.setJsonObject(queryResult);
    when(jestClient.execute(search)).thenReturn(result);

    assertThat(client.search(QUERY, INDEX, TYPE), is(equalTo(queryResult)));
  }

  @Test
  public void closes() {
    JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
    client.close();

    verify(jestClient).shutdownClient();
  }

  private BulkResult createBulkResultFailure(String exception) {
    BulkResult failure = new BulkResult(new Gson());
    failure.setSucceeded(false);
    JsonObject error = new JsonObject();
    error.addProperty("type", exception);
    JsonObject item = new JsonObject();
    item.addProperty("_index", INDEX);
    item.addProperty("_type", TYPE);
    item.addProperty("status", 0);
    item.add("error", error);
    JsonObject index = new JsonObject();
    index.add("index", item);
    JsonArray items = new JsonArray();
    items.add(index);
    JsonObject root = new JsonObject();
    root.add("items", items);
    failure.setJsonObject(root);
    return failure;
  }
}
