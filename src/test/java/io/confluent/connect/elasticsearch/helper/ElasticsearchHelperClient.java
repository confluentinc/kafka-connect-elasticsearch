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

package io.confluent.connect.elasticsearch.helper;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.GetDataStreamRequest.Builder;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.kafka.test.TestUtils;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateDataStreamRequest;
import org.elasticsearch.client.indices.DataStream;
import org.elasticsearch.client.indices.DeleteDataStreamRequest;
import org.elasticsearch.client.indices.GetDataStreamRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.client.security.PutRoleRequest;
import org.elasticsearch.client.security.PutRoleResponse;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.PutUserResponse;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import io.confluent.connect.elasticsearch.ConfigCallbackHandler;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;

public class ElasticsearchHelperClient {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchHelperClient.class);

  private final String url;
  private final ElasticsearchSinkConnectorConfig config;
  private RestHighLevelClient client;

  public ElasticsearchHelperClient(String url, ElasticsearchSinkConnectorConfig config,
      boolean compatibilityMode) {
    ConfigCallbackHandler configCallbackHandler = new ConfigCallbackHandler(config);
    this.url = url;
    this.config = config;
    this.client = new RestHighLevelClientBuilder(
        RestClient
            .builder(HttpHost.create(url))
            .setHttpClientConfigCallback(configCallbackHandler)
        .build()
        // compatibility mode should be true for 7.17 high level rest clients while talking to ES 8.
    ).setApiCompatibilityMode(compatibilityMode).build();
  }

  public ElasticsearchHelperClient(String url, ElasticsearchSinkConnectorConfig config) {
    this(url, config, false);
  }

  public ElasticsearchClient getNewJavaAPIClient() {
    ConfigCallbackHandler configCallbackHandler = new ConfigCallbackHandler(config);
    RestClient client = RestClient
        .builder(HttpHost.create(url))
        .setHttpClientConfigCallback(configCallbackHandler)
        .build();
    return new ElasticsearchClient(new RestClientTransport(
        client, new JacksonJsonpMapper()));
  }

  public void deleteIndex(String index, boolean isDataStream) throws IOException {
    if (isDataStream) {
      DeleteDataStreamRequest request = new DeleteDataStreamRequest(index);
      client.indices().deleteDataStream(request, RequestOptions.DEFAULT);
      return;
    }
    DeleteIndexRequest request = new DeleteIndexRequest(index);
    client.indices().delete(request, RequestOptions.DEFAULT);
  }

  public DataStream getDataStream(String dataStream) throws IOException {
    GetDataStreamRequest request = new GetDataStreamRequest(dataStream);
    List<DataStream> datastreams = client.indices()
        .getDataStream(request, RequestOptions.DEFAULT)
        .getDataStreams();
    return datastreams.size() == 0 ? null : datastreams.get(0);
  }

  public co.elastic.clients.elasticsearch.indices.DataStream getDataStreamWithJavaAPIClient(
      String dataStream
  ) throws IOException {
    List<co.elastic.clients.elasticsearch.indices.DataStream> dataStreams =
        getNewJavaAPIClient().indices().getDataStream(
            new Builder()
                .name(dataStream)
                .build()
        ).dataStreams();
    return dataStreams.size() == 0 ? null : dataStreams.get(0);
  }

  public long getDocCount(String index) throws IOException {
    CountRequest request = new CountRequest(index);
    return client.count(request, RequestOptions.DEFAULT).getCount();
  }

  public MappingMetadata getMapping(String index) throws IOException {
    GetMappingsRequest request = new GetMappingsRequest().indices(index);
    GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
    return response.mappings().get(index);
  }

  public boolean indexExists(String index) throws IOException {
    GetIndexRequest request = new GetIndexRequest(index);
    return client.indices().exists(request, RequestOptions.DEFAULT);
  }

  public void createIndex(String index, String jsonMappings) throws IOException {
    CreateIndexRequest createIndexRequest = new CreateIndexRequest(index).mapping(jsonMappings, XContentType.JSON);
    client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
  }

  public void createIndexesWithoutMapping(String... indexes) throws IOException {
    for (String index : indexes) {
      // Check if index exists and delete it first to avoid "already exists" error
      if (indexExists(index)) {
        deleteIndex(index, false);
      }
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
      client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }
  }

  public void createDataStreams(String... dataStreams) throws IOException {
    for (String dataStream : dataStreams) {
      // Check if data stream exists and delete it first to avoid "already exists" error
      if (indexExists(dataStream)) {
        deleteIndex(dataStream, true);
      }
      CreateDataStreamRequest createDataStreamRequest = new CreateDataStreamRequest(dataStream);
      client.indices().createDataStream(createDataStreamRequest, RequestOptions.DEFAULT);
    }
  }

  public void updateAlias(String index1, String index2, String alias, String writeIndex) throws IOException {
    IndicesAliasesRequest request = new IndicesAliasesRequest();
    
    // Add index1 to alias
    IndicesAliasesRequest.AliasActions addIndex1 =
            IndicesAliasesRequest.AliasActions.add()
                    .index(index1)
                    .alias(alias)
                    .writeIndex(index1.equals(writeIndex));

    // Add index2 to alias
    IndicesAliasesRequest.AliasActions addIndex2 =
            IndicesAliasesRequest.AliasActions.add()
                    .index(index2)
                    .alias(alias)
                    .writeIndex(index2.equals(writeIndex));

    request.addAliasAction(addIndex1);
    request.addAliasAction(addIndex2);
    client.indices().updateAliases(request, RequestOptions.DEFAULT);
  }

  public SearchHits search(String index) throws IOException {
    SearchRequest request = new SearchRequest(index);
    return client.search(request, RequestOptions.DEFAULT).getHits();
  }

  public void createRole(Role role) throws IOException {
    PutRoleRequest putRoleRequest = new PutRoleRequest(role, RefreshPolicy.IMMEDIATE);
    PutRoleResponse putRoleResponse = client.security().putRole(putRoleRequest, RequestOptions.DEFAULT);
    if (!putRoleResponse.isCreated()) {
      throw new RuntimeException(String.format("Failed to create a role %s", role.getName()));
    }
  }

  public void createUser(Entry<User, String> userToPassword) throws IOException {
    PutUserRequest putUserRequest = PutUserRequest.withPassword(
        userToPassword.getKey(),
        userToPassword.getValue().toCharArray(),
        true,
        RefreshPolicy.IMMEDIATE
    );
    PutUserResponse putUserResponse = client.security().putUser(putUserRequest, RequestOptions.DEFAULT);
    if (!putUserResponse.isCreated()) {
      throw new RuntimeException(String.format("Failed to create a user %s", userToPassword.getKey().getUsername()));
    }
  }

  public void waitForConnection(long timeMs) {
    try {
      TestUtils.retryOnExceptionWithTimeout(timeMs, () -> client.info(RequestOptions.DEFAULT));
    } catch (InterruptedException e) {
      // do nothing
    }
  }

  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      log.error("Error closing client.", e);
    }
  }
}
