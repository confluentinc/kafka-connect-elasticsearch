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
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.DataStream;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.elasticsearch.security.PutRoleResponse;
import co.elastic.clients.elasticsearch.security.PutUserResponse;
import co.elastic.clients.elasticsearch.security.RoleDescriptor;
import co.elastic.clients.elasticsearch.security.User;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.kafka.test.TestUtils;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.confluent.connect.elasticsearch.ConfigCallbackHandler;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;

public class ElasticsearchHelperClient {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchHelperClient.class);

  private final RestClient restClient;
  private final ElasticsearchClient client;

  public ElasticsearchHelperClient(String url, ElasticsearchSinkConnectorConfig config) {
    ConfigCallbackHandler configCallbackHandler = new ConfigCallbackHandler(config);
    this.restClient = RestClient
        .builder(HttpHost.create(url))
        .setHttpClientConfigCallback(configCallbackHandler)
        .build();
    this.client = new ElasticsearchClient(
        new RestClientTransport(restClient, new JacksonJsonpMapper()));
  }

  public void deleteIndex(String index, boolean isDataStream) throws IOException {
    if (isDataStream) {
      client.indices().deleteDataStream(r -> r.name(index));
      return;
    }
    client.indices().delete(r -> r.index(index));
  }

  public DataStream getDataStream(String dataStream) throws IOException {
    List<DataStream> dataStreams =
        client.indices().getDataStream(r -> r.name(dataStream)).dataStreams();
    return dataStreams.isEmpty() ? null : dataStreams.get(0);
  }

  public long getDocCount(String index) throws IOException {
    return client.count(r -> r.index(index)).count();
  }

  public IndexMappingRecord getMapping(String index) throws IOException {
    GetMappingResponse response = client.indices().getMapping(r -> r.index(index));
    return response.result().get(index);
  }

  public boolean indexExists(String index) throws IOException {
    return client.indices().exists(r -> r.index(index)).value();
  }

  public void createIndex(String index, String jsonMappings) throws IOException {
    client.indices().create(r -> r
        .index(index)
        .mappings(m -> m.withJson(new StringReader(jsonMappings))));
  }

  public void createIndexesWithoutMapping(String... indexes) throws IOException {
    for (String index : indexes) {
      // Check if index exists and delete it first to avoid "already exists" error
      if (indexExists(index)) {
        deleteIndex(index, false);
      }
      client.indices().create(r -> r.index(index));
    }
  }

  public void createDataStreams(String... dataStreams) throws IOException {
    for (String dataStream : dataStreams) {
      // Check if data stream exists and delete it first to avoid "already exists" error
      if (indexExists(dataStream)) {
        deleteIndex(dataStream, true);
      }
      client.indices().createDataStream(r -> r.name(dataStream));
    }
  }

  public void updateAlias(String index1, String index2, String alias, String writeIndex)
      throws IOException {
    client.indices().updateAliases(r -> r
        .actions(a -> a.add(add -> add
            .index(index1)
            .alias(alias)
            .isWriteIndex(index1.equals(writeIndex))))
        .actions(a -> a.add(add -> add
            .index(index2)
            .alias(alias)
            .isWriteIndex(index2.equals(writeIndex)))));
  }

  /**
   * Returns the hits from a match-all search of the given index.
   *
   * <p>Returns {@link Hit}s rather than bare source maps because callers need both the document id
   * ({@code hit.id()}) and the source ({@code hit.source()}). Deserializing the source to
   * {@code Map} routes through the configured {@link JacksonJsonpMapper}, so field values arrive as
   * plain Java types rather than JSON-P wrapper types.
   */
  @SuppressWarnings("rawtypes")
  public List<Hit<Map>> search(String index) throws IOException {
    return client.search(r -> r.index(index), Map.class).hits().hits();
  }

  /**
   * Creates a role.
   *
   * <p>The role name is passed separately because, unlike the high level REST client's
   * {@code Role}, {@link RoleDescriptor} does not carry its own name -- the new client's API takes
   * the name on the request instead.
   */
  public void createRole(String name, RoleDescriptor role) throws IOException {
    PutRoleResponse response = client.security().putRole(r -> r
        .name(name)
        .indices(role.indices())
        .cluster(role.cluster())
        .refresh(Refresh.True));
    if (!response.role().created()) {
      throw new RuntimeException(String.format("Failed to create a role %s", name));
    }
  }

  public void createUser(Entry<User, String> userToPassword) throws IOException {
    User user = userToPassword.getKey();
    PutUserResponse response = client.security().putUser(r -> r
        .username(user.username())
        .password(userToPassword.getValue())
        .roles(user.roles())
        .refresh(Refresh.True));
    if (!response.created()) {
      throw new RuntimeException(
          String.format("Failed to create a user %s", user.username()));
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
      restClient.close();
    } catch (IOException e) {
      log.error("Error closing client.", e);
    }
  }
}
