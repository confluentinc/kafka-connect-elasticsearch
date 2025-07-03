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

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchSinkConnector extends SinkConnector {

  private Map<String, String> configProperties;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    try {
      configProperties = props;
      // validation
      new ElasticsearchSinkConnectorConfig(props);
    } catch (ConfigException e) {
      throw new ConnectException(
          "Couldn't start ElasticsearchSinkConnector due to configuration error",
          e
      );
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return ElasticsearchSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      HashMap<String, String> taskConfig = new HashMap<>(configProperties);
      taskConfig.put(ElasticsearchSinkTaskConfig.TASK_ID_CONFIG, Integer.toString(i));
      taskConfigs.add(taskConfig);
    }
    return taskConfigs;
  }

  @Override
  public void stop() throws ConnectException { }

  @Override
  public ConfigDef config() {
    return ElasticsearchSinkConnectorConfig.CONFIG;
  }

  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    Validator validator = new Validator(connectorConfigs);
    return validator.validate();
  }
}
