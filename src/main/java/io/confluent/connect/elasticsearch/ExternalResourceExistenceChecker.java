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

package io.confluent.connect.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;

import java.io.IOException;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.ExternalResourceUsage;

/**
 * Strategy pattern implementation for checking external resource existence in Elasticsearch.
 * Provides different strategies for checking existence of indices, data streams, and aliases.
 */
public class ExternalResourceExistenceChecker {

  /**
   * Interface for external resource existence checking strategies.
   */
  public interface ExternalResourceExistenceStrategy {
    /**
     * Checks if an external resource exists in Elasticsearch.
     *
     * @param client the Elasticsearch client
     * @param resource the resource name to check
     * @return true if the resource exists, false otherwise
     * @throws IOException if there's an I/O error
     * @throws ElasticsearchException if there's an Elasticsearch error
     */
    boolean exists(ElasticsearchClient client, String resource) throws IOException;
  }

  /**
   * Strategy for checking index and data stream existence.
   */
  public static class IndexAndDataStreamExistenceStrategy
          implements ExternalResourceExistenceStrategy {
    @Override
    public boolean exists(ElasticsearchClient client, String resource) throws IOException {
      return client.indices().exists(r -> r.index(resource)).value();
    }
  }

  /**
   * Strategy for checking alias existence.
   */
  public static class AliasExistenceStrategy implements ExternalResourceExistenceStrategy {
    @Override
    public boolean exists(ElasticsearchClient client, String resource) throws IOException {
      return client.indices().existsAlias(r -> r.name(resource)).value();
    }
  }

  /**
   * Factory method to get the appropriate existence strategy based on external resource type.
   *
   * @param externalResourceUsage the type of external resource to check
   * @return the appropriate existence strategy
   * @throws IllegalArgumentException if the resource type is not supported
   */
  public static ExternalResourceExistenceStrategy getExistenceStrategy(
          ExternalResourceUsage externalResourceUsage) {
    switch (externalResourceUsage) {
      case INDEX:
      case DATASTREAM:
        return new IndexAndDataStreamExistenceStrategy();
      case ALIAS_INDEX:
      case ALIAS_DATASTREAM:
        return new AliasExistenceStrategy();
      default:
        throw new IllegalArgumentException("Unsupported external resource type: "
                + externalResourceUsage);
    }
  }
}
