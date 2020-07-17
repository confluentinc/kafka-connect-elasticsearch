/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.connect.elasticsearch.integration;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.ElasticsearchClient;
import io.confluent.connect.elasticsearch.jest.JestElasticsearchClient;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import org.testcontainers.containers.DockerComposeContainer;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

  protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(300);
  protected static final long FAILURE_DURATION_MS = TimeUnit.SECONDS.toMillis(300);
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(300);
  protected static final String KAFKA_TOPIC = "topic";
  protected static final String CONNECTOR_NAME = "elasticsearch-sink-connector";

  protected static final String MAX_TASKS = "1";

  protected static final Schema SCHEMA = SchemaBuilder.struct().name("com.example.Person")
      .field("userId", Schema.OPTIONAL_INT32_SCHEMA)
      .field("firstName", Schema.STRING_SCHEMA)
      .field("lastName", Schema.STRING_SCHEMA)
      .field("message", Schema.STRING_SCHEMA)
      .build();

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

  protected static DockerComposeContainer pumbaPauseContainer;
  protected void startPumbaPauseContainer() {
    pumbaPauseContainer =
        new DockerComposeContainer(new File("src/test/docker/configA/pumba-pause-compose.yml"));
    pumbaPauseContainer.start();
  }

  protected static DockerComposeContainer pumbaDelayContainer;
  protected void startPumbaDelayContainer() {
    pumbaDelayContainer =
        new DockerComposeContainer(new File("src/test/docker/configA/pumba-delay-compose.yml"));
    pumbaDelayContainer.start();
  }

  public void stopEScontainer() {
    container.close();
  }

  public void setUp() {
    log.info("Attempting to connect to {}", container.getConnectionUrl());
    client = new JestElasticsearchClient(container.getConnectionUrl());
  }

  public void setUp(Map<String, String> props) {
    log.info("Attempting to connect to {}", "container.getConnectionUrl()");
    client = new JestElasticsearchClient(props);
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

  protected long waitForConnectorToWriteDataIntoES(String connectorName, int numTasks, String index, int numberOfRecords) throws InterruptedException {
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

  protected long waitForConnectorTaskToFail() throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertConnectorTaskFailure().orElse(false),
        FAILURE_DURATION_MS,
        "Task did not fail in specified time."
    );
    return System.currentTimeMillis();
  }

  protected Optional<Boolean> assertConnectorTaskFailure() {
    ConnectorStateInfo info = connect.connectorStatus(CONNECTOR_NAME);
    boolean result = info != null
        && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
    if (!info.tasks().iterator().next().state().equals(AbstractStatus.State.RUNNING.toString())) {
      log.info("Connector task status: {}", info.tasks().iterator().next().state());
    }
    return Optional.of(!result);
  }

  protected void assertRecordsCountAndContent(int numRecords, String index) throws IOException {
    String query = "{ \"size\" : " + numRecords + "}";
    final JsonObject result = client.search(query, index, null);
    final JsonArray jsonArray = result.getAsJsonObject("hits").getAsJsonArray("hits");
    int id = 0;
    // Collecting records in ascending order of 'userId'
    TreeMap<Integer , JsonObject> map = new TreeMap<>();
    for (JsonElement element : jsonArray) {
      JsonObject jsonObject = element.getAsJsonObject().get("_source").getAsJsonObject();
      Integer userId = jsonObject.get("userId").getAsInt();
      map.put(userId, jsonObject);
    }
    // Assert actual data in ElasticSearch
    for (Map.Entry<Integer, JsonObject> obj : map.entrySet()) {
      assertEquals(id++, obj.getValue().get("userId").getAsInt());
      assertEquals("Alex", obj.getValue().get("firstName").getAsString());
      assertEquals("Smith", obj.getValue().get("lastName").getAsString());
      assertEquals("ElasticSearch message", obj.getValue().get("message").getAsString());
    }
    // Assert records count in ElasticSearch
    assertEquals(id, jsonArray.size());
    assertEquals(id, numRecords);
  }
}
