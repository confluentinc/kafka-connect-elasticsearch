/**
 * Copyright 2016 Confluent Inc.
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

package io.confluent.connect.elasticsearch.internals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResultHandler;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;

public class HttpClient implements Client<Response>, JestResultHandler<BulkResult> {

  private ObjectMapper objectMapper = new ObjectMapper();
  private final JestClient jestClient;
  private Callback<Response> callback;

  public HttpClient(JestClient jestClient) {
    this.jestClient = jestClient;
  }

  @Override
  public void execute(RecordBatch batch, Callback<Response> callback) {
    this.callback = callback;
    Bulk bulk = constructBulk(batch);
    jestClient.executeAsync(bulk, this);
  }

  @Override
  public void close() {
    jestClient.shutdownClient();
  }

  @Override
  public void completed(BulkResult result) {
    callback.onResponse(new Response(result));
  }

  @Override
  public void failed(Exception e) {
    callback.onFailure(e);
  }

  private Bulk constructBulk(RecordBatch batch) {
    Bulk.Builder builder = new Bulk.Builder();
    List<ESRequest> requests = batch.requests();
    for (ESRequest request: requests) {
      JsonNode data = null;
      try {
        data = objectMapper.readTree(request.getPayload());
      } catch (IOException e) {
        callback.onFailure(e);
      }
      Index index = new Index.Builder(data)
          .index(request.getIndex())
          .type(request.getType())
          .id(request.getId())
          .build();
      builder.addAction(index);
    }
    return builder.build();
  }
}
