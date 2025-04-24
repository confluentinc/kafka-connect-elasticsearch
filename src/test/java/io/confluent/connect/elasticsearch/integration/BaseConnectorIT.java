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

import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.confluent.kafka.schemaregistry.RestApp;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

  protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.MINUTES.toMillis(1);
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.MINUTES.toMillis(60);
  protected static final long CONNECTOR_COMMIT_DURATION_MS = TimeUnit.MINUTES.toMillis(1);

  protected EmbeddedConnectCluster connect;
  protected RestApp restApp;

  protected void startConnect() {
    HashMap<String, String> workerProps = new HashMap<>();
    workerProps.put("plugin.discovery", "hybrid_warn");
    connect = new EmbeddedConnectCluster.Builder()
        .name("elasticsearch-it-connect-cluster")
        .workerProps(workerProps)
        .build();

    // start the clusters
    connect.start();
  }

  protected void stopConnect() {
    // stop all Connect, Kafka and Zk threads.
    if (connect != null) {
      connect.stop();
      connect = null;
    }
  }

  /**
   * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the given
   * name to start the specified number of tasks.
   *
   * @param name     the name of the connector
   * @param numTasks the minimum number of tasks that are expected
   * @throws InterruptedException if this was interrupted
   */
  protected void waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
        CONNECTOR_STARTUP_DURATION_MS,
        "Connector tasks did not start in time."
    );
  }

  /**
   * Confirm that a connector with an exact number of tasks is running.
   *
   * @param connectorName the connector
   * @param numTasks      the minimum number of tasks
   * @return true if the connector and tasks are in RUNNING state; false otherwise
   */
  protected Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
    try {
      ConnectorStateInfo info = connect.connectorStatus(connectorName);
      boolean result = info != null
          && info.tasks().size() >= numTasks
          && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
          && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
      return Optional.of(result);
    } catch (Exception e) {
      log.error("Could not check connector state info.");
      return Optional.empty();
    }
  }

  public long waitForConnectorToFail(String name, int numTasks, String trace) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertConnectorAndTasksFailed(name, numTasks, trace).orElse(false),
        CONNECTOR_STARTUP_DURATION_MS,
        "Connector tasks did not fail in time."
    );
    return System.currentTimeMillis();
  }

  protected Optional<Boolean> assertConnectorAndTasksFailed(String connectorName, int numTasks, String trace) {
    try {
      ConnectorStateInfo info = connect.connectorStatus(connectorName);
      boolean result = info != null
          && info.tasks().size() >= numTasks
          && info.tasks().stream()
          .allMatch(s -> s.state().equals(AbstractStatus.State.FAILED.toString())
              && info.tasks().stream().allMatch(t -> t.trace().contains(trace))
          );
      return Optional.of(result);
    } catch (Exception e) {
      log.error("Could not check connector state info.");
      return Optional.empty();
    }
  }
}