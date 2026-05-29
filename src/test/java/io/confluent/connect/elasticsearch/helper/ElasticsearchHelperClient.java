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
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.DataStream;
import co.elastic.clients.elasticsearch.indices.update_aliases.Action;
import co.elastic.clients.elasticsearch.security.PutRoleRequest;
import co.elastic.clients.elasticsearch.security.PutUserRequest;
import co.elastic.clients.util.ObjectBuilder;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.kafka.test.TestUtils;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.confluent.connect.elasticsearch.ConfigCallbackHandler;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;

public class ElasticsearchHelperClient {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchHelperClient.class);

  private final String url;
  private final ElasticsearchSinkConnectorConfig config;
  private final ElasticsearchClient client;

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

  public void deleteIndex(String index, boolean isDataStream) throws IOException {
    if (isDataStream) {
      client.indices().deleteDataStream(r -> r.name(index));
      return;
    }
    client.indices().delete(r -> r.index(index));
  }

  public DataStream getDataStream(String dataStream) throws IOException {
    List<DataStream> dataStreams = client.indices()
        .getDataStream(r -> r.name(dataStream))
        .dataStreams();
    return dataStreams.isEmpty() ? null : dataStreams.get(0);
  }

  public long getDocCount(String index) throws IOException {
    return client.count(r -> r.index(index)).count();
  }

  public TypeMapping getMapping(String index) throws IOException {
    return client.indices()
        .getMapping(r -> r.index(index))
        .result()
        .get(index)
        .mappings();
  }

  public boolean indexExists(String index) throws IOException {
    return client.indices().exists(r -> r.index(index)).value();
  }

  public void createIndex(String index, String jsonMappings) throws IOException {
    client.indices().create(r -> r.index(index).mappings(m -> m.withJson(new StringReader(jsonMappings))));
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
    // Add index1 to alias
    Action addIndex1 = Action.of(a -> a.add(add -> add
        .index(index1).alias(alias).isWriteIndex(index1.equals(writeIndex))));
    // Add index2 to alias
    Action addIndex2 = Action.of(a -> a.add(add -> add
        .index(index2).alias(alias).isWriteIndex(index2.equals(writeIndex))));
    client.indices().updateAliases(r -> r.actions(Arrays.asList(addIndex1, addIndex2)));
  }

  @SuppressWarnings("unchecked")
  public List<Hit<Map<String, Object>>> search(String index) throws IOException {
    return client.search(r -> r.index(index), (Class<Map<String, Object>>) (Class<?>) Map.class)
        .hits().hits();
  }

  public void createRole(
      Function<PutRoleRequest.Builder, ObjectBuilder<PutRoleRequest>> fn
  ) throws IOException {
    boolean created = client.security().putRole(fn).role().created();
    if (!created) {
      throw new RuntimeException("Failed to create a role");
    }
  }

  public void createUser(
      Function<PutUserRequest.Builder, ObjectBuilder<PutUserRequest>> fn
  ) throws IOException {
    boolean created = client.security().putUser(fn).created();
    if (!created) {
      throw new RuntimeException("Failed to create a user");
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
      client.close();
    } catch (IOException e) {
      log.error("Error closing client.", e);
    }
  }
}
