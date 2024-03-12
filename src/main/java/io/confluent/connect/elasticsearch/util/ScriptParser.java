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

package io.confluent.connect.elasticsearch.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.script.Script;

import java.util.Map;

public class ScriptParser {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static Script parseScript(String scriptJson) throws JsonProcessingException {

        Map<String, Object> map = ScriptParser.parseSchemaStringAsJson(scriptJson);

        return Script.parse(map);
    }

    private static Map<String, Object> parseSchemaStringAsJson(String scriptJson)
            throws JsonProcessingException {

        ObjectMapper objectMapper = new ObjectMapper();

        Map<String, Object> scriptConverted;

        scriptConverted = objectMapper.readValue(
                scriptJson, new TypeReference<Map<String, Object>>(){});

        return scriptConverted;
    }

    public static Script parseScriptWithParams(String scriptJson, String jsonPayload)
            throws JsonProcessingException {

        Map<String, Object> map = ScriptParser.parseSchemaStringAsJson(scriptJson);

        Map<String, Object> fields = objectMapper.readValue(jsonPayload,
                new TypeReference<Map<String, Object>>() {});

        map.put("params", fields);

        return Script.parse(map);
    }
}