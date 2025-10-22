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

import java.util.List;
import java.util.Map;

import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.Role;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.*;

@Category(IntegrationTest.class)
public class ElasticsearchConnectorIT extends ElasticsearchConnectorBaseIT {
  private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConnectorIT.class);
  
  // TODO: test compatibility

  @BeforeClass
  public static void setupBeforeAll() {
    Map<User, String> users = getUsers();
    List<Role> roles = getRoles();
    container = ElasticsearchContainer.fromSystemProperties().withBasicAuth(users, roles);
    container.start();
  }

  @Override
  public void setup() throws Exception {
    if (!container.isRunning()) {
      setupBeforeAll();
    }
    super.setup();
  }

  @Override
  protected Map<String, String> createProps() {
    props = super.createProps();
    props.put(CONNECTION_USERNAME_CONFIG, ELASTIC_MINIMAL_PRIVILEGES_NAME);
    props.put(CONNECTION_PASSWORD_CONFIG, ELASTIC_MINIMAL_PRIVILEGES_PASSWORD);
    return props;
  }

  private void testBackwardsCompatibilityDataStreamVersionHelper(
      String version
  ) throws Exception {
    logger.info("=== Starting backward compatibility test for ES version: {} ===", version);
    
    try {
      // since we require older ES container we close the current one and set it back up
      logger.info("Closing current container before switching to ES version: {}", version);
      container.close();
      
      logger.info("Creating new container with ES version: {}", version);
      container = ElasticsearchContainer.withESVersion(version);
      
      logger.info("Starting container with ES version: {}", version);
      container.start();
      logger.info("Container started successfully for ES version: {}", version);
      
      logger.info("Setting up from container for ES version: {}", version);
      setupFromContainer();
      logger.info("Setup completed for ES version: {}", version);

      logger.info("Running simple test for ES version: {}", version);
      runSimpleTest(props);
      logger.info("Simple test completed successfully for ES version: {}", version);

    } catch (Exception e) {
      logger.error("Backward compatibility test failed for ES version: {}", version, e);
      logger.error("Container details: {}", container.getDetailedContainerInfo());
      
      // Try to get container logs for debugging
      try {
        String containerLogs = container.getLogs();
        logger.error("Container logs for ES version {}: {}", version, containerLogs);
      } catch (Exception logException) {
        logger.error("Could not retrieve container logs for ES version: {}", version, logException);
      }
      
      // Log additional diagnostic information
      logger.error("Diagnostic information for ES version {}:", version);
      logger.error("  - Exception type: {}", e.getClass().getSimpleName());
      logger.error("  - Exception message: {}", e.getMessage());
      if (e.getCause() != null) {
        logger.error("  - Root cause: {}", e.getCause().getMessage());
      }
      
      throw e;
    } finally {
      logger.info("Cleaning up container for ES version: {}", version);
      helperClient = null;
      container.close();
      setupBeforeAll();
      logger.info("=== Completed backward compatibility test for ES version: {} ===", version);
    }
  }

  @Test
  public void testBackwardsCompatibility() throws Exception {
    testBackwardsCompatibilityDataStreamVersionHelper("7.16.3");
  }
}
