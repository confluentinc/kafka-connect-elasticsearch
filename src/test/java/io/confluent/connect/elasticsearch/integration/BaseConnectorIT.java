/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.connect.elasticsearch.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.ElasticsearchClient;
import io.confluent.connect.elasticsearch.jest.JestElasticsearchClient;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

  protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(300);
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(300);
  
  protected EmbeddedConnectCluster connect;
  protected static ElasticsearchContainer container;
  protected ElasticsearchClient client;

  protected void startConnect() {
    connect = new EmbeddedConnectCluster.Builder()
        .name("elasticsearch-connect-cluster")
        .build();
    connect.start();
  }

  protected void stopConnect() {
    connect.stop();
  }
  
  public void startEScontainer() {
    // Relevant and available docker images for elastic can be found at https://www.docker.elastic.co
    container = ElasticsearchContainer.fromSystemProperties();
    container.start();
  }

  public void stopEScontainer() {
    container.close();
  }

  public void setUp() {
    log.info("Attempting to connect to {}", container.getConnectionUrl());
    client = new JestElasticsearchClient(container.getConnectionUrl());
  }
  
  /**
   * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the given
   * name to start the specified number of tasks.
   *
   * @param name the name of the connector
   * @param numTasks the minimum number of tasks that are expected
   * @return the time this method discovered the connector has started, in milliseconds past epoch
   * @throws InterruptedException if this was interrupted
   */
  protected long waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
        CONNECTOR_STARTUP_DURATION_MS,
        "Connector tasks did not start in time."
    );
    return System.currentTimeMillis();
  }

  /**
   * Confirm that a connector with an exact number of tasks is running.
   *
   * @param connectorName the connector
   * @param numTasks the minimum number of tasks
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
      log.error("Could not check connector state info.", e);
      return Optional.empty();
    }
  }

  protected long waitConnectorToWriteDataIntoElasticsearch(String connectorName, int numTasks, String index, int numberOfRecords) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertRecordsCountInElasticsearch(connectorName, numTasks, index, numberOfRecords).orElse(false),
        CONSUME_MAX_DURATION_MS,
        "Either writing into table has not started or row count did not matched."
    );
    return System.currentTimeMillis();
  }

  private Optional<Boolean> assertRecordsCountInElasticsearch(String name, int numTasks, String index, int numberOfRecords) {
    try {
      final JsonObject searchResult = client.search("", index, null);
      final int recordsInserted = searchResult.getAsJsonObject("hits").getAsJsonObject("total").get("value").getAsInt();
      ConnectorStateInfo info = connect.connectorStatus(name);
      boolean result = info != null
          && info.tasks().size() == numTasks
          && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
          && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
      boolean targetRowCountStatus = recordsInserted == numberOfRecords;
      if (!targetRowCountStatus) {
        log.warn("Row count did not matched. Expected {}, Actual {}.", numberOfRecords, recordsInserted);
      }
      return Optional.of(result && targetRowCountStatus);
    } catch (Exception e) {
      log.warn("Could not check connector state info, will retry shortly ...");
      return Optional.empty();
    }
  }

  protected void verifySearchResults(ConsumerRecords<byte[], byte[]> records, String index) throws IOException {
    final JsonObject result = client.search("", index, null);
    final int recordsInserted = result.getAsJsonObject("hits").getAsJsonObject("total").get("value").getAsInt();
    assertEquals(records.count(), recordsInserted);
  }
}
