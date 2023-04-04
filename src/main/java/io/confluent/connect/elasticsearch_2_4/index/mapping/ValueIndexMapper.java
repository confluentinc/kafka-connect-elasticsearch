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

package io.confluent.connect.elasticsearch_2_4.index.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.INDEX_MAPPER_FIELD;

public class ValueIndexMapper implements IndexMapper {
  private static final Logger log = LoggerFactory.getLogger(ValueIndexMapper.class);
  private String[] path;

  @Override
  public void configure(ElasticsearchSinkConnectorConfig configuration) {
    this.path = configuration.getString(INDEX_MAPPER_FIELD).split("\\.");
  }

  /**
   * Returns the converted index name from a given topic name. If writing to a data stream,
   * returns the index name in the form {type}-{dataset}-{topic}. For both cases, Elasticsearch
   * accepts:
   * <ul>
   *   <li>all lowercase</li>
   *   <li>less than 256 bytes</li>
   *   <li>does not start with - or _</li>
   *   <li>is not . or ..</li>
   * </ul>
   * (<a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params">ref</a>_.)
   */
  @Override
  public String getIndex(String topic, JsonNode jsonNode) throws Exception {
    String index = null;
    JsonNode current = jsonNode;
    try {
      for (int i = 0; i <= path.length; i++) {
        if (i == path.length) {
          index = current.asText();
        } else {
          current = current.get(path[i]);
        }
      }
    } catch (NullPointerException e) {
      String err = String.format(
              "Unable to determine index name for path %s for value %s",
              String.join(".", path),
              jsonNode.toString());
      log.error(err);
      throw new Exception(err);
    }
    return index;
  }
}
