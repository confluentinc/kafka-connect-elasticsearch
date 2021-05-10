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

import io.confluent.connect.elasticsearch.ConfigCallbackHandler;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import java.io.IOException;
import java.util.List;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.connect.elasticsearch.RetryUtil.callWithRetries;

public class ElasticsearchHelperClient {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchHelperClient.class);

  private RestHighLevelClient client;

  public ElasticsearchHelperClient(String url, ElasticsearchSinkConnectorConfig config) {
    ConfigCallbackHandler configCallbackHandler = new ConfigCallbackHandler(config);
    this.client = new RestHighLevelClient(
        RestClient
            .builder(HttpHost.create(url))
            .setHttpClientConfigCallback(configCallbackHandler)
            .setRequestConfigCallback(configCallbackHandler)
    );
  }

  public int getDataStreamCount(String index) throws IOException {
    // todo: check if amoutn is correct
    GetDataStreamRequest request = new GetDataStreamRequest(index);
    List<DataStream> datastreams = client.indices().getDataStream(request, RequestOptions.DEFAULT).getDataStreams();
    return datastreams.size();
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

  public SearchHits search(String index) throws IOException {
    SearchRequest request = new SearchRequest(index);
    return client.search(request, RequestOptions.DEFAULT).getHits();
  }

  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      log.error("Error closing client.", e);
    }
  }
}
