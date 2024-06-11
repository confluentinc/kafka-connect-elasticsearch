/*
 * Copyright 2020 Confluent Inc.
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

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;

import org.apache.kafka.common.config.SslConfigs;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.Role;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SSL_CONFIG_PREFIX;


@Category(IntegrationTest.class)
public class ElasticsearchConnectorSslIT extends ElasticsearchConnectorBaseIT {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchConnectorSslIT.class);

  @BeforeClass
  public static void setupBeforeAll() {
    Map<User, String> users = getUsers();
    List<Role> roles = getRoles();
    container = ElasticsearchContainer.fromSystemProperties().withSslEnabled(true).withBasicAuth(users, roles);
    container.start();
  }

  /**
   * Run test against docker image running Elasticsearch.
   * Certificates are generated in src/test/resources/ssl/start-elasticsearch.sh
   */
  @Test
  public void testSecureConnectionVerifiedHostname() throws Throwable {
    // Use container IP address here because that's what the certificates allow
    String address = container.getConnectionUrl(false);
    log.info("Creating connector for {}.", address);

    props.put(CONNECTION_URL_CONFIG, address);
    container.addSslProps(props);

    helperClient = container.getHelperClient(props);

    // Start connector
    runSimpleTest(props);
  }

  @Override
  protected Map<String, String> createProps() {
    props = super.createProps();
    props.put(CONNECTION_USERNAME_CONFIG, ELASTIC_MINIMAL_PRIVILEGES_NAME);
    props.put(CONNECTION_PASSWORD_CONFIG, ELASTIC_MINIMAL_PRIVILEGES_PASSWORD);
    return props;
  }

  @Test
  public void testSecureConnectionHostnameVerificationDisabled() throws Throwable {
    // Use 'localhost' here that is not in self-signed cert
    String address = container.getConnectionUrl();
    address = address.replace(container.getContainerIpAddress(), "localhost");
    log.info("Creating connector for {}", address);

    props.put(CONNECTION_URL_CONFIG, address);
    container.addSslProps(props);

    // disable hostname verification
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

    helperClient = container.getHelperClient(props);

    // Start connector
    runSimpleTest(props);
  }
}
