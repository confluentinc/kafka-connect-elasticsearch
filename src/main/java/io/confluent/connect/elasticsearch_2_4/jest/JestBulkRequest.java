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

package io.confluent.connect.elasticsearch_2_4.jest;

import io.confluent.connect.elasticsearch_2_4.IndexableRecord;
import io.confluent.connect.elasticsearch_2_4.bulk.BulkRequest;
import io.searchbox.core.Bulk;
import java.util.List;

public class JestBulkRequest implements BulkRequest {

  private final Bulk bulk;
  private final List<IndexableRecord> records;

  public JestBulkRequest(Bulk bulk, List<IndexableRecord> records) {
    this.bulk = bulk;
    this.records = records;
  }

  public Bulk getBulk() {
    return bulk;
  }

  public List<IndexableRecord> records() {
    return records;
  }
}
