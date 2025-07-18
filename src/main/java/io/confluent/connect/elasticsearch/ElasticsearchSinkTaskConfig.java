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

import org.apache.kafka.common.config.ConfigDef;
import java.util.Map;

public class ElasticsearchSinkTaskConfig extends ElasticsearchSinkConnectorConfig {

  private final int taskId;
  private final String connectorName;

  public static final String TASK_ID_CONFIG =                   "taskId";
  private static final ConfigDef.Type TASK_ID_TYPE =            ConfigDef.Type.INT;
  public static final ConfigDef.Importance TASK_ID_IMPORTANCE = ConfigDef.Importance.LOW;
  public static final int TASK_ID_DEFAULT = 0;

  /**
   * Return a ConfigDef object used to define this config's fields.
   *
   * @return A ConfigDef object used to define this config's fields.
   */
  public static ConfigDef config() {
    return ElasticsearchSinkConnectorConfig.baseConfigDef()
            .defineInternal(
                    TASK_ID_CONFIG,
                    TASK_ID_TYPE,
                    TASK_ID_DEFAULT,
                    TASK_ID_IMPORTANCE
            );
  }

  /**
   * @param properties A Map detailing configuration properties and their respective values.
   */
  public ElasticsearchSinkTaskConfig(Map<String, String> properties) {
    super(config(), properties);
    taskId = properties.containsKey(TASK_ID_CONFIG) ? getInt(TASK_ID_CONFIG) : 0;
    connectorName = originalsStrings().get("name");
  }

  public int getTaskId() {
    return taskId;
  }

  public String getConnectorName() {
    return connectorName;
  }
}