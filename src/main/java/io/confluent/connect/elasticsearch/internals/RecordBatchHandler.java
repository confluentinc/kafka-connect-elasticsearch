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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RecordBatchHandler implements ActionListener<BulkResponse> {

  private static final Logger log = LoggerFactory.getLogger(RecordBatchHandler.class);
  private final Client client;
  private final Callback<ESResponse> callback;

  public RecordBatchHandler(Client client, Callback<ESResponse> callback) {
    this.client = client;
    this.callback = callback;
  }

  @Override
  public void onResponse(BulkResponse responses) {
    callback.onResponse(new ESResponse(responses));
  }

  @Override
  public void onFailure(Throwable e) {
    callback.onFailure(e);
  }


  public void execute(RecordBatch batch) {
    BulkRequest bulkRequest = constructBulkRequest(batch);
    client.bulk(bulkRequest, this);
  }

  private BulkRequest constructBulkRequest(RecordBatch batch) {
    BulkRequest bulkRequest = new BulkRequest();
    List<ESRequest> requests = batch.requests();
    for (ESRequest request: requests) {
      bulkRequest.add(new IndexRequest(request.getIndex(), request.getType(), request.getId())
                          .source(request.getPayload()));
    }
    return bulkRequest;
  }
}
