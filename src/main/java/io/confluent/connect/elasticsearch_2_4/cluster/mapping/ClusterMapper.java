/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package io.confluent.connect.elasticsearch_2_4.cluster.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig;

import java.util.Map;
import java.util.Set;

public interface ClusterMapper {
  void configure(ElasticsearchSinkConnectorConfig configuration);

  Map<String, Set<String>> getAllClusters();

  String getName(JsonNode jsonNode) throws Exception;

  Set<String> getClusterUrl(String clusterName) throws Exception;
}
