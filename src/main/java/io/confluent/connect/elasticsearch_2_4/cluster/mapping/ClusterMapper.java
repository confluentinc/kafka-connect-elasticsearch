/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package io.confluent.connect.elasticsearch_2_4.cluster.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig;

import java.util.Set;

public interface ClusterMapper {
  void configure(ElasticsearchSinkConnectorConfig configuration);

  Set<String> getCluster(String topic, JsonNode jsonNode) throws Exception;

  Set<String> getDefaultCluster();
}
