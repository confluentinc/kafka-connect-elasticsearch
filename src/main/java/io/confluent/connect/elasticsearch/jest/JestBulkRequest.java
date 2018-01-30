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

import io.confluent.connect.elasticsearch.bulk.BulkRequest;
import io.searchbox.core.Bulk;

public class JestBulkRequest implements BulkRequest {

  private final Bulk bulk;

  public JestBulkRequest(Bulk bulk) {
    this.bulk = bulk;
  }

  public Bulk getBulk() {
    return bulk;
  }
}
