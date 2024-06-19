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

package io.confluent.connect.elasticsearch.validator;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WRITE_METHOD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WriteMethod.SCRIPTED_UPSERT;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.confluent.connect.elasticsearch.util.ScriptParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.elasticsearch.script.Script;

public class ScriptValidator implements ConfigDef.Validator, ConfigDef.Recommender {

  @Override
  @SuppressWarnings("unchecked")
  public void ensureValid(String name, Object value) {

    if (value == null) {
      return;
    }

    String script = (String) value;

    try {
      Script parsedScript = ScriptParser.parseScript(script);

      if (parsedScript.getIdOrCode() == null) {
        throw new ConfigException(name, script, "The specified script is missing code");
      } else if (parsedScript.getLang() == null) {
        throw new ConfigException(name, script, "The specified script is missing lang");
      }

    } catch (JsonProcessingException jsonProcessingException) {
      throw new ConfigException(
          name, script, "The specified script is not a valid Elasticsearch painless script");
    }
  }

  @Override
  public String toString() {
    return "A valid script that is able to be parsed";
  }

  @Override
  public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
    if (!parsedConfig.get(WRITE_METHOD_CONFIG).equals(SCRIPTED_UPSERT)) {
      return new ArrayList<>();
    }
    return null;
  }

  @Override
  public boolean visible(String name, Map<String, Object> parsedConfig) {
    return parsedConfig.get(WRITE_METHOD_CONFIG).equals(SCRIPTED_UPSERT.name());
  }
}
