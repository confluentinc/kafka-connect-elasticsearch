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

package io.confluent.connect.elasticsearch_2_4.cluster.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.CLUSTER_MAPPER_FIELD;

public class ValueClusterMapper implements ClusterMapper {
  private static final Logger log = LoggerFactory.getLogger(ValueClusterMapper.class);
  private Set<String> connectionUrls;
  private String[] path;

  @Override
  public void configure(ElasticsearchSinkConnectorConfig configuration) {
    connectionUrls = configuration.connectionUrls();
    this.path = configuration.getString(CLUSTER_MAPPER_FIELD).split("\\.");
  }

  /**
   * Returns the converted cluster name from a given field.
   * accepts:
   * <ul>
   *   <li>all lowercase</li>
   *   <li>less than 256 bytes</li>
   *   <li>does not start with - or _</li>
   *   <li>is not . or ..</li>
   * </ul>
   */
  @Override
  public Set<String> getCluster(String topic, JsonNode jsonNode) throws Exception {
    String cluster = null;
    JsonNode current = jsonNode;
    try {
      for (int i = 0; i <= path.length; i++) {
        if (i == path.length) {
          cluster = current.asText();
        } else {
          current = current.get(path[i]);
        }
      }
    } catch (NullPointerException e) {
      String err = String.format(
              "Unable to determine cluster name for path %s for value %s",
              String.join(".", path),
              jsonNode);
      log.error(err);
      throw new Exception(err);
    }

    if (cluster == null) {
      String err = String.format(
              "Unable to determine cluster name for path %s for value %s",
              String.join(".", path),
              jsonNode);
      log.error(err);
      throw new Exception(err);
    }
    return Arrays.stream(cluster.split(",")).collect(Collectors.toSet());
  }

  @Override
  public Set<String> getDefaultCluster() {
    return connectionUrls;
  }
}
