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

package io.confluent.connect.elasticsearch;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.Random;

public class MockClient extends NoOpClient {
  private int numberOfCallsToFail;
  private Random random = new Random();

  public MockClient(String testName, int numberOfCallsToFail) {
    super(testName);
    this.numberOfCallsToFail = numberOfCallsToFail;
  }

  @Override
  public ActionFuture<BulkResponse> bulk(BulkRequest request) {
    PlainActionFuture<BulkResponse> responseFuture = new PlainActionFuture<>();
    bulk(request, responseFuture);
    return responseFuture;
  }

  @Override
  public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
    // do everything synchronously, that's fine for a test
    boolean shouldFail = numberOfCallsToFail > 0;
    numberOfCallsToFail--;

    BulkItemResponse[] itemResponses = new BulkItemResponse[request.requests().size()];
    // if we have to fail, we need to fail at least once "reliably", the rest can be random
    int itemToFail = random.nextInt(request.requests().size() - 1);
    for (int idx = 0; idx < request.requests().size(); idx++) {
      if (shouldFail && (random.nextBoolean() || idx == itemToFail)) {
        itemResponses[idx] = failedResponse();
      } else {
        itemResponses[idx] = successfulResponse();
      }
    }
    listener.onResponse(new BulkResponse(itemResponses, 1000L));
  }

  private BulkItemResponse successfulResponse() {
    return new BulkItemResponse(1, "update", new DeleteResponse());
  }

  private BulkItemResponse failedResponse() {
    return new BulkItemResponse(1, "update", new BulkItemResponse.Failure("test", "test", "1", new EsRejectedExecutionException("pool full")));
  }
}
