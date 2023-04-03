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

package io.confluent.connect.elasticsearch_2_4.type.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig.TYPE_MAPPER_FIELD;

public class ValueTypeMapper implements TypeMapper {
  private static final Logger log = LoggerFactory.getLogger(ValueTypeMapper.class);

  private String[] path;

  @Override
  public void configure(ElasticsearchSinkConnectorConfig configuration) {
    this.path = configuration.getString(TYPE_MAPPER_FIELD).split("\\.");
  }

  @Override
  public String getType(String topic, JsonNode jsonNode) throws Exception {
    String type = null;
    JsonNode current = jsonNode;
    try {
      for (int i = 0; i <= path.length; i++) {
        if (i == path.length) {
          type = current.asText();
        } else {
          current = current.get(path[i]);
        }
      }
    } catch (NullPointerException e) {
      String err = String.format(
              "Unable to determine type name for path %s for value %s",
              String.join(".", path),
              jsonNode);
      log.error(err);
      throw new Exception(err);
    }

    if (type == null) {
      String err = String.format(
              "Unable to determine type name for path %s for value %s",
              String.join(".", path),
              jsonNode);
      log.error(err);
      throw new Exception(err);
    }
    return type;
  }
}
