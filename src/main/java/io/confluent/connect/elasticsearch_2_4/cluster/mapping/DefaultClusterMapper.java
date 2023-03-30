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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class DefaultClusterMapper implements ClusterMapper {
  private static final Logger log = LoggerFactory.getLogger(DefaultClusterMapper.class);

  private Set<String> connectionUrls;

  @Override
  public void configure(ElasticsearchSinkConnectorConfig configuration) {
    connectionUrls = configuration.connectionUrls();
  }

  @Override
  public Map<String, Set<String>> getAllClusters() {
    return Collections.singletonMap("default", connectionUrls);
  }

  @Override
  public String getName(JsonNode jsonNode) {
    return "default";
  }

  @Override
  public Set<String> getClusterUrl(String clusterName) throws Exception {
    return connectionUrls;
  }
}
