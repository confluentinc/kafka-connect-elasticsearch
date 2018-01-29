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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonObject;
import io.confluent.connect.elasticsearch.bulk.BulkRequest;
import io.confluent.connect.elasticsearch.ElasticsearchClient;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.confluent.connect.elasticsearch.IndexableRecord;
import io.confluent.connect.elasticsearch.Key;
import io.confluent.connect.elasticsearch.Mapping;
import io.confluent.connect.elasticsearch.bulk.BulkResponse;
import io.searchbox.action.Action;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.cluster.NodesInfo;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JestElasticsearchClient implements ElasticsearchClient {
  private static final Logger LOG = LoggerFactory.getLogger(JestElasticsearchClient.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final JestClient client;
  private final Version version;

  @VisibleForTesting
  public JestElasticsearchClient(String address) {
    try {
      JestClientFactory factory = new JestClientFactory();
      factory.setHttpClientConfig(new HttpClientConfig.Builder(address)
          .multiThreaded(true)
          .build()
      );
      this.client = factory.getObject();
      this.version = getServerVersion();
    } catch (IOException e) {
      throw new ConnectException(
          "Couldn't start ElasticsearchSinkTask due to connection error:",
          e
      );
    } catch (ConfigException e) {
      throw new ConnectException(
          "Couldn't start ElasticsearchSinkTask due to configuration error:",
          e
      );
    }
  }

  public JestElasticsearchClient(Map<String, String> props) {
    try {
      ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
      final int connTimeout = config.getInt(
              ElasticsearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG);
      final int readTimeout = config.getInt(
              ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG);

      List<String> address =
              config.getList(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG);
      JestClientFactory factory = new JestClientFactory();
      factory.setHttpClientConfig(new HttpClientConfig.Builder(address)
              .connTimeout(connTimeout)
              .readTimeout(readTimeout)
              .multiThreaded(true)
              .build()
      );
      this.client = factory.getObject();
      this.version = getServerVersion();
    } catch (IOException e) {
      throw new ConnectException(
              "Couldn't start ElasticsearchSinkTask due to connection error:",
              e
      );
    } catch (ConfigException e) {
      throw new ConnectException(
              "Couldn't start ElasticsearchSinkTask due to configuration error:",
              e
      );
    }
  }

  private Version getServerVersion() throws IOException {
    // Default to newest version for forward compatibility
    Version defaultVersion = Version.SIX;

    NodesInfo info = new NodesInfo.Builder().addCleanApiParameter("version").build();
    JsonObject result = this.client.execute(info).getJsonObject();
    if (result == null) {
      LOG.warn("Couldn't get Elasticsearch version");
      return defaultVersion;
    }

    JsonObject nodesRoot = result.get("nodes").getAsJsonObject();
    if (nodesRoot == null || nodesRoot.entrySet().size() == 0) {
      LOG.warn("Couldn't get Elasticsearch version");
      return defaultVersion;
    }

    JsonObject nodeRoot = nodesRoot.entrySet().iterator().next().getValue().getAsJsonObject();
    if (nodeRoot == null) {
      LOG.warn("Couldn't get Elasticsearch version");
      return defaultVersion;
    }

    String esVersion = nodeRoot.get("version").getAsString();
    if (esVersion == null) {
      LOG.warn("Couldn't get Elasticsearch version");
      return defaultVersion;
    } else if (esVersion.startsWith("1.")) {
      return Version.ONE;
    } else if (esVersion.startsWith("2.")) {
      return Version.TWO;
    } else if (esVersion.startsWith("5.")) {
      return Version.FIVE;
    } else if (esVersion.startsWith("6.")) {
      return Version.SIX;
    }
    return defaultVersion;
  }

  public Version getVersion() {
    return version;
  }

  private boolean indexExists(String index) {
    Action action = new IndicesExists.Builder(index).build();
    try {
      JestResult result = client.execute(action);
      return result.isSucceeded();
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  public void createIndices(Set<String> indices) {
    for (String index : indices) {
      if (!indexExists(index)) {
        CreateIndex createIndex = new CreateIndex.Builder(index).build();
        try {
          JestResult result = client.execute(createIndex);
          if (!result.isSucceeded()) {
            String msg = result.getErrorMessage() != null ? ": " + result.getErrorMessage() : "";
            throw new ConnectException("Could not create index '" + index + "'" + msg);
          }
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }
    }
  }

  public void createMapping(String index, String type, Schema schema) throws IOException {
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set(type, Mapping.inferMapping(this, schema));
    PutMapping putMapping = new PutMapping.Builder(index, type, obj.toString()).build();
    JestResult result = client.execute(putMapping);
    if (!result.isSucceeded()) {
      throw new ConnectException(
              "Cannot create mapping " + obj + " -- " + result.getErrorMessage()
      );
    }
  }

  /**
   * Get the JSON mapping for given index and type. Returns {@code null} if it does not exist.
   */
  public JsonObject getMapping(String index, String type) throws IOException {
    final JestResult result = client.execute(
            new GetMapping.Builder().addIndex(index).addType(type).build()
    );
    final JsonObject indexRoot = result.getJsonObject().getAsJsonObject(index);
    if (indexRoot == null) {
      return null;
    }
    final JsonObject mappingsJson = indexRoot.getAsJsonObject("mappings");
    if (mappingsJson == null) {
      return  null;
    }
    return mappingsJson.getAsJsonObject(type);
  }

  public BulkRequest createBulkRequest(List<IndexableRecord> batch) {
    final Bulk.Builder builder = new Bulk.Builder();
    for (IndexableRecord record : batch) {
      builder.addAction(toBulkableAction(record));
    }
    return new JestBulkRequest(builder.build());
  }

  private BulkableAction toBulkableAction(IndexableRecord record) {
    // If payload is null, the record was a tombstone and we should delete from the index.
    return record.payload != null ? toIndexRequest(record) : toDeleteRequest(record);
  }

  private Delete toDeleteRequest(IndexableRecord record) {
    Delete.Builder req = new Delete.Builder(record.key.id)
        .index(record.key.index)
        .type(record.key.type);

    // TODO: Should version information be set here?
    return req.build();
  }

  private Index toIndexRequest(IndexableRecord record) {
    Index.Builder req = new Index.Builder(record.payload)
        .index(record.key.index)
        .type(record.key.type)
        .id(record.key.id);
    if (record.version != null) {
      req.setParameter("version_type", "external").setParameter("version", record.version);
    }
    return req.build();
  }

  public BulkResponse executeBulk(BulkRequest bulk) throws IOException {
    final BulkResult result = client.execute(((JestBulkRequest) bulk).getBulk());

    if (result.isSucceeded()) {
      return BulkResponse.success();
    }

    boolean retriable = true;

    final List<Key> versionConflicts = new ArrayList<>();
    final List<String> errors = new ArrayList<>();

    for (BulkResult.BulkResultItem item : result.getItems()) {
      if (item.error != null) {
        final ObjectNode parsedError = (ObjectNode) OBJECT_MAPPER.readTree(item.error);
        final String errorType = parsedError.get("type").asText("");
        if ("version_conflict_engine_exception".equals(errorType)) {
          versionConflicts.add(new Key(item.index, item.type, item.id));
        } else if ("mapper_parse_exception".equals(errorType)) {
          retriable = false;
          errors.add(item.error);
        } else {
          errors.add(item.error);
        }
      }
    }

    if (!versionConflicts.isEmpty()) {
      LOG.debug("Ignoring version conflicts for items: {}", versionConflicts);
      if (errors.isEmpty()) {
        // The only errors were version conflicts
        return BulkResponse.success();
      }
    }

    final String errorInfo = errors.isEmpty() ? result.getErrorMessage() : errors.toString();

    return BulkResponse.failure(retriable, errorInfo);
  }

  public JsonObject search(String query, String index, String type) throws IOException {
    final Search.Builder search = new Search.Builder(query);
    if (index != null) {
      search.addIndex(index);
    }
    if (type != null) {
      search.addType(type);
    }

    final SearchResult result = client.execute(search.build());

    return result.getJsonObject();
  }

  public void shutdown() {
    try {
      client.close();
    } catch (IOException e) {
      LOG.warn("Could not close client");
    }
  }
}
