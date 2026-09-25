/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.connect.elasticsearch.integration;

import co.elastic.clients.elasticsearch.security.CreateApiKeyResponse;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_API_KEY_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class ElasticsearchConnectorApiKeyIT extends ElasticsearchConnectorBaseIT {

  @BeforeClass
  public static void setupBeforeAll() {
    container = ElasticsearchContainer.fromSystemProperties()
        .withBasicAuth(getUsers(), getRoles());
    container.start();
  }

  // The key is scoped to the connector's documented minimal privileges, not the superuser's.
  @Test
  public void testEncodedApiKey() throws Exception {
    props.put(CONNECTION_API_KEY_CONFIG, createMinimalPrivilegesApiKey().encoded());
    runSimpleTest(props);
  }

  @Test
  public void testIdAndSecretApiKey() throws Exception {
    CreateApiKeyResponse key = createMinimalPrivilegesApiKey();
    props.put(CONNECTION_API_KEY_CONFIG, key.id() + ":" + key.apiKey());
    runSimpleTest(props);
  }

  // A well-formed key Elasticsearch does not recognise must be rejected at validation time.
  @Test
  public void testUnknownApiKeyFailsValidation() {
    props.put(CONNECTION_API_KEY_CONFIG, Base64.getEncoder()
        .encodeToString("unknown-id:unknown-secret".getBytes(StandardCharsets.UTF_8)));

    ConfigInfos result = connect.validateConnectorConfig(
        ElasticsearchSinkConnector.class.getSimpleName(), props);

    List<String> apiKeyErrors = result.values().stream()
        .map(ConfigInfo::configValue)
        .filter(v -> v.name().equals(CONNECTION_API_KEY_CONFIG))
        .flatMap(v -> v.errors().stream())
        .collect(Collectors.toList());
    assertEquals(apiKeyErrors.toString(), 1, apiKeyErrors.size());
    assertTrue(apiKeyErrors.get(0).contains("Could not authenticate with the API key."));
  }

  private CreateApiKeyResponse createMinimalPrivilegesApiKey() throws Exception {
    return helperClient.getClient().security().createApiKey(r -> r
        .name("connector-it")
        .roleDescriptors("sink", d -> d
            .cluster("monitor")
            .indices(i -> i
                .names("*")
                .privileges("create_index", "read", "write", "view_index_metadata"))));
  }
}
