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

import com.google.gson.JsonObject;
import io.confluent.connect.elasticsearch_2_4.bulk.BulkRequest;
import io.confluent.connect.elasticsearch_2_4.bulk.BulkResponse;
import org.apache.kafka.connect.data.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface ElasticsearchClient extends AutoCloseable {

  enum Version {
    ES_V1, ES_V2, ES_V5, ES_V6, ES_V7
  }

  /**
   * Gets the Elasticsearch version.
   *
   * @return the version, not null
   */
  Version getVersion();

  /**
   * Creates indices.
   *
   * @param indices the set of index names to create, not null
   */
  void createIndices(Set<String> indices);

  /**
   * Creates an explicit mapping.
   *
   * @param index the index to write
   * @param type the type for which to create the mapping
   * @param schema the schema used to infer the mapping
   * @throws IOException if the client cannot execute the request
   */
  void createMapping(String index, String type, Schema schema) throws IOException;

  /**
   * Gets the JSON mapping for the given index and type. Returns {@code null} if it does not exist.
   *
   * @param index the index
   * @param type the type
   * @throws IOException if the client cannot execute the request
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
   * @throws IOException if the client cannot execute the request
   */
  BulkResponse executeBulk(BulkRequest bulk) throws IOException;

  /**
   * Executes a search.
   *
   * @param query the search query
   * @param index the index to search
   * @param type the type to search
   * @return the search result
   * @throws IOException if the client cannot execute the request
   */
  JsonObject search(String query, String index, String type) throws IOException;

  /**
   * Delete all indexes in Elasticsearch (useful mostly for test)
   *
   * @throws IOException if the client cannot execute the request
   */
  default void deleteAll() throws IOException {
    throw new UnsupportedOperationException("deleteAll is not implemented yet by this client");
  }

  /**
   * Perform a refresh of all indexes, making all indexed data searchable (useful mostly for test)
   *
   * @throws IOException If the client cannot execute the request
   */
  default void refresh() throws IOException {
    throw new UnsupportedOperationException("refresh is not implemented yet by this client");
  }

  /**
   * Shuts down the client.
   */
  void close();
}
