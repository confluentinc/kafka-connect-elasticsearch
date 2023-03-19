/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.connect.elasticsearch_2_4;

import io.confluent.connect.elasticsearch_2_4.bulk.BulkClient;
import io.confluent.connect.elasticsearch_2_4.bulk.BulkRequest;
import io.confluent.connect.elasticsearch_2_4.bulk.BulkResponse;

import java.io.IOException;
import java.util.List;

public class BulkIndexingClient implements BulkClient<IndexableRecord, BulkRequest> {

  private final ElasticsearchClient client;

  public BulkIndexingClient(ElasticsearchClient client) {
    this.client = client;
  }

  @Override
  public BulkRequest bulkRequest(List<IndexableRecord> batch) {
    return client.createBulkRequest(batch);
  }

  @Override
  public BulkResponse execute(BulkRequest bulk) throws IOException {
    return client.executeBulk(bulk);
  }

}
