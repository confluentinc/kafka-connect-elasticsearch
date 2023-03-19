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

package io.confluent.connect.elasticsearch_2_4.bulk;


import java.util.List;

/**
 * BulkRequest is a marker interface for use with
 * {@link io.confluent.connect.elasticsearch_2_4.ElasticsearchClient#createBulkRequest(List)} and
 * {@link io.confluent.connect.elasticsearch_2_4.ElasticsearchClient#executeBulk(BulkRequest)}.
 * Implementations will typically hold state comprised of instances of classes that are
 * specific to the client library.
 */
public interface BulkRequest {
}
