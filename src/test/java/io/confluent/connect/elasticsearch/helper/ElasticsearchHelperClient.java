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
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.DataStream;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.elasticsearch.security.PutRoleRequest;
import co.elastic.clients.elasticsearch.security.PutRoleResponse;
import co.elastic.clients.elasticsearch.security.PutUserRequest;
import co.elastic.clients.elasticsearch.security.PutUserResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.kafka.test.TestUtils;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.elasticsearch.ConfigCallbackHandler;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;

public class ElasticsearchHelperClient {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchHelperClient.class);

  private final String url;
  private final ElasticsearchSinkConnectorConfig config;
  private ElasticsearchClient client;

  public ElasticsearchHelperClient(String url, ElasticsearchSinkConnectorConfig config) {
    ConfigCallbackHandler configCallbackHandler = new ConfigCallbackHandler(config);
    this.url = url;
    this.config = config;
    RestClient restClient = RestClient
        .builder(HttpHost.create(url))
        .setHttpClientConfigCallback(configCallbackHandler)
        .build();
    this.client = new ElasticsearchClient(
        new RestClientTransport(restClient, new JacksonJsonpMapper()));
  }

  public ElasticsearchClient getClient() {
    return client;
  }

  public void deleteIndex(String index, boolean isDataStream) throws IOException {
    if (isDataStream) {
      client.indices().deleteDataStream(d -> d.name(index));
      return;
    }
    client.indices().delete(d -> d.index(index));
  }

  public DataStream getDataStream(String dataStream) throws IOException {
    List<DataStream> dataStreams = client.indices()
        .getDataStream(d -> d.name(dataStream))
        .dataStreams();
    return dataStreams.size() == 0 ? null : dataStreams.get(0);
  }

  public long getDocCount(String index) throws IOException {
    return client.count(c -> c.index(index)).count();
  }

  public TypeMapping getMapping(String index) throws IOException {
    IndexMappingRecord record = client.indices()
        .getMapping(g -> g.index(index))
        .result()
        .get(index);
    return record == null ? null : record.mappings();
  }

  public boolean indexExists(String index) throws IOException {
    return client.indices().exists(e -> e.index(index)).value();
  }

  public void createIndex(String index, String jsonMappings) throws IOException {
    client.indices().create(c -> c
        .index(index)
        .mappings(m -> m.withJson(new StringReader(jsonMappings))));
  }

  public void createIndexesWithoutMapping(String... indexes) throws IOException {
    for (String index : indexes) {
      // Check if index exists and delete it first to avoid "already exists" error
      if (indexExists(index)) {
        deleteIndex(index, false);
      }
      client.indices().create(c -> c.index(index));
    }
  }

  public void createDataStreams(String... dataStreams) throws IOException {
    for (String dataStream : dataStreams) {
      // Check if data stream exists and delete it first to avoid "already exists" error
      if (indexExists(dataStream)) {
        deleteIndex(dataStream, true);
      }
      client.indices().createDataStream(c -> c.name(dataStream));
    }
  }

  public void updateAlias(String index1, String index2, String alias, String writeIndex)
      throws IOException {
    client.indices().updateAliases(u -> u
        .actions(a -> a.add(add -> add
            .index(index1)
            .alias(alias)
            .isWriteIndex(index1.equals(writeIndex))))
        .actions(a -> a.add(add -> add
            .index(index2)
            .alias(alias)
            .isWriteIndex(index2.equals(writeIndex)))));
  }

  public List<Hit<JsonData>> search(String index) throws IOException {
    return client.search(s -> s.index(index), JsonData.class).hits().hits();
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> sourceAsMap(Hit<JsonData> hit) {
    return hit.source() == null ? new HashMap<>() : hit.source().to(Map.class);
  }

  public void createRole(PutRoleRequest roleRequest) throws IOException {
    PutRoleResponse putRoleResponse = client.security().putRole(roleRequest);
    if (!putRoleResponse.role().created()) {
      throw new RuntimeException(
          String.format("Failed to create a role %s", roleRequest.name()));
    }
  }

  public void createUser(PutUserRequest userRequest) throws IOException {
    PutUserResponse putUserResponse = client.security().putUser(userRequest);
    if (!putUserResponse.created()) {
      throw new RuntimeException(
          String.format("Failed to create a user %s", userRequest.username()));
    }
  }

  public void waitForConnection(long timeMs) {
    try {
      TestUtils.retryOnExceptionWithTimeout(timeMs, () -> client.info());
    } catch (InterruptedException e) {
      // do nothing
    }
  }

  public void close() {
    try {
      // Closing the transport also closes the underlying low-level RestClient.
      client._transport().close();
    } catch (IOException e) {
      log.error("Error closing client.", e);
    }
  }
}
