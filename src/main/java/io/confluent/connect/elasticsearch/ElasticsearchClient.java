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

package io.confluent.connect.elasticsearch;

import com.google.gson.JsonObject;
import io.confluent.connect.elasticsearch.bulk.BulkRequest;
import io.confluent.connect.elasticsearch.bulk.BulkResponse;
import org.apache.kafka.connect.data.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface ElasticsearchClient {

  enum Version {
    ONE, TWO, FIVE, SIX
  }

  /**
   * Gets the Elasticsearch version.
   *
   * @return the version
   */
  Version getVersion();

  /**
   * Creates indices.
   *
   * @param indices the set of index names to create
   */
  void createIndices(Set<String> indices);

  /**
   * Creates an explicit mapping.
   *
   * @param index the index to write
   * @param type the type for which to create the mapping
   * @param schema the schema used to infer the mapping
   * @throws IOException from underlying client
   */
  void createMapping(String index, String type, Schema schema) throws IOException;

  /**
   * Gets the JSON mapping for the given index and type. Returns {@code null} if it does not exist.
   *
   * @param index the index
   * @param type the type
   * @throws IOException from underlying client
   */
  JsonObject getMapping(String index, String type) throws IOException;

  /**
   * Creates a bulk request for the list of {@link IndexableRecord} records.
   *
   * @param batch the list of records
   * @return the bulk request
   */
  BulkRequest createBulkRequest(List<IndexableRecord> batch);

  /**
   * Executes a bulk action.
   *
   * @param bulk the bulk request
   * @return the bulk response
   */
  BulkResponse executeBulk(BulkRequest bulk) throws IOException;

  /**
   * Executes a search.
   *
   * @param query the search query
   * @param index the index to search
   * @param type the type to search
   * @return the search result
   */
  JsonObject search(String query, String index, String type) throws IOException;

  /**
   * Shuts down the client.
   */
  void shutdown();
}
