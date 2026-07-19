/*
 * Copyright 2025 Confluent Inc.
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

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SECURITY_PROTOCOL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SSL_CONFIG_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.HashMap;

import org.apache.kafka.common.config.SslConfigs;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SecurityProtocol;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import io.confluent.connect.elasticsearch.helper.ElasticsearchHelperClient;

public class ElasticsearchClientSslTest extends ElasticsearchClientTestBase {

  private static final String ELASTIC_SUPERUSER_NAME = "elastic";
  private static final String ELASTIC_SUPERUSER_PASSWORD = "elastic";

  @BeforeClass
  public static void setupBeforeAll() {
    container = ElasticsearchContainer.fromSystemProperties().withSslEnabled(true);
    container.start();
  }

  @Override
  @Before
  public void setup() {
    index = TOPIC;
    props = ElasticsearchSinkConnectorConfigTest.addNecessaryProps(new HashMap<>());
    String address = container.getConnectionUrl(false);
    props.put(CONNECTION_URL_CONFIG, address);
    props.put(CONNECTION_USERNAME_CONFIG, ELASTIC_SUPERUSER_NAME);
    props.put(CONNECTION_PASSWORD_CONFIG, ELASTIC_SUPERUSER_PASSWORD);
    props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, container.getKeystorePath());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, container.getKeystorePassword());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, container.getTruststorePath());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, container.getTruststorePassword());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEY_PASSWORD_CONFIG, container.getKeyPassword());
    config = new ElasticsearchSinkConnectorConfig(props);
    converter = new DataConverter(config);
    helperClient = new ElasticsearchHelperClient(address, config,
        container.shouldStartClientInCompatibilityMode());
    helperClient.waitForConnection(30000);
    offsetTracker = mock(OffsetTracker.class);
  }

  @Test
  public void testSsl() throws Exception {
    ElasticsearchClient client = new ElasticsearchClient(config, null, 
    () -> offsetTracker.updateOffsets(), 1, "elasticsearch-sink");
    client.createIndexOrDataStream(index);

    writeRecord(sinkRecord(0), client);
    client.flush();

    waitUntilRecordsInES(1);
    assertEquals(1, helperClient.getDocCount(index));
    client.close();
  }
} 