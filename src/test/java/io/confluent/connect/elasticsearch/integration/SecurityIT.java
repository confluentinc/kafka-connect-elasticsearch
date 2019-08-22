package io.confluent.connect.elasticsearch.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.core.Search;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.confluent.connect.elasticsearch.jest.JestElasticsearchClient.getClientConfig;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;

@Category(IntegrationTest.class)
public class SecurityIT {
  private static final Logger log = LoggerFactory.getLogger(SecurityIT.class);

  protected static ElasticsearchContainer container;

  private EmbeddedConnectCluster connect;

  private static final String MESSAGE_KEY = "message-key";
  private static final String MESSAGE_VAL = "{ \"schema\": { \"type\": \"map\", \"keys\": "
      + "{ \"type\" : \"string\" }, \"values\": { \"type\" : \"int32\" } }, "
      + "\"payload\": { \"key1\": 12, \"key2\": 15} }";
  private static final String CONNECTOR_NAME = "elastic-sink";
  private static final String KAFKA_TOPIC = "test-elasticsearch-sink";
  private static final String TYPE_NAME = "kafka-connect";
  private static final int TASKS_MAX = 1;
  private static final int NUM_MSG = 200;
  private static final long VERIFY_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(2);

  @BeforeClass
  public static void setupBeforeAll() {
    // Relevant and available docker images for elastic can be found at https://www.docker.elastic.co
    String dockerImageName = ElasticsearchIntegrationTestBase.getElasticsearchDockerImageName();
    String esVersion = ElasticsearchIntegrationTestBase.getElasticsearchContainerVersion();
    container = new ElasticsearchContainer(dockerImageName + ":" + esVersion);
    container.withSharedMemorySize(2L * 1024 * 1024 * 1024);

    // Copy the certs and ES configuration to the image so that ES supports TLS
    container.withCopyFileToContainer(
        MountableFile.forHostPath("./src/test/resources/elasticsearch.yml"),
        "/usr/share/elasticsearch/config/elasticsearch.yml"
    );
    container.withCopyFileToContainer(
        MountableFile.forHostPath("./src/test/resources/certs"),
        "/usr/share/elasticsearch/config/certs"
    );
    container.withLogConsumer(SecurityIT::containerLog);
    container.waitingFor(
        Wait.forLogMessage(".*(Security is enabled|license .* valid).*", 1)
            .withStartupTimeout(Duration.ofMinutes(20))
    );
    // TODO: Enable this once a fix is found for Jenkins; see CC-4886
    //container.start();
  }

  @AfterClass
  public static void teardownAfterAll() {
    container.close();
  }

  @Before
  public void setup() throws IOException {
    connect = new EmbeddedConnectCluster.Builder().name("elastic-sink-cluster").build();
    connect.start();
  }

  @After
  public void close() {
    connect.stop();
  }

  private Map<String, String> getProps () {
    Map<String, String> props = new HashMap<>();
    props.put(CONNECTOR_CLASS_CONFIG, ElasticsearchSinkConnector.class.getName());
    props.put(SinkConnectorConfig.TOPICS_CONFIG, KAFKA_TOPIC);
    props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put("type.name", TYPE_NAME);
    props.put("batch.size", "1");
    return props;
  }

  /**
   * Run test against docker image running Elasticsearch.
   * Certificates are generated with src/test/resources/certs/generate_certificates.sh
   */
  @Ignore("Enable this once a fix is found for Jenkins; see CC-4886")
  @Test
  public void testSecureConnection() throws Throwable {
    final String address = String.format(
        "https://%s:%d",
        container.getContainerIpAddress(),
        container.getMappedPort(9200)
    );
    log.info("Creating connector for {}", address);

    connect.kafka().createTopic(KAFKA_TOPIC, 1);

    // Start connector
    Map<String, String> props = getProps();
    props.put("connection.url", address);
    props.put("elastic.security.protocol", "SSL");
    props.put("elastic.https.ssl.keystore.location", "./src/test/resources/certs/keystore.jks");
    props.put("elastic.https.ssl.keystore.password", "asdfasdf");
    props.put("elastic.https.ssl.key.password", "asdfasdf");
    props.put("elastic.https.ssl.truststore.location", "./src/test/resources/certs/truststore.jks");
    props.put("elastic.https.ssl.truststore.password", "asdfasdf");
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForCondition(() -> {
      ConnectorStateInfo info = connect.connectorStatus(CONNECTOR_NAME);
      return info != null && info.tasks() != null && info.tasks().size() == 1;
    }, "Timed out waiting for connector task to start");

    for (int i=0; i<NUM_MSG; i++){
      connect.kafka().produce(KAFKA_TOPIC, MESSAGE_KEY+i, MESSAGE_VAL);
    }
    verify(getClient(props));
    verify(getClient(props));
  }

  private JestClient getClient(Map<String, String> props) {
    JestClientFactory factory = new JestClientFactory();
    factory.setHttpClientConfig(getClientConfig(props));
    return factory.getObject();
  }

  private void verify(JestClient client) throws Throwable {
    // Read the message out of elastic directly via Jest
    Search search = new Search.Builder("{}").addIndex(KAFKA_TOPIC).addType(TYPE_NAME).build();

    waitForCondition(() -> {
        try {
          int found = client.execute(search).getJsonObject()
              .get("hits").getAsJsonObject()
              .get("total").getAsJsonObject()
              .get("value").getAsInt();
          log.debug("Found {} documents", found);
          return found == NUM_MSG;
        } catch (Exception e) {
          log.error("Retrying after failing to read data from Elastic: {}", e.getMessage(), e);
          return false;
        }
      }, VERIFY_TIMEOUT_MS, "Could not read data from Elastic");
  }

  /**
   * Capture the container log by writing the container's standard output
   * to {@link System#out} (in yellow) and standard error to {@link System#err} (in red).
   *
   * @param logMessage the container log message
   */
  protected static void containerLog(OutputFrame logMessage) {
    switch (logMessage.getType()) {
      case STDOUT:
        // Normal output in yellow
        System.out.print((char)27 + "[33m" + logMessage.getUtf8String());
        System.out.print((char)27 + "[0m"); // reset
        break;
      case STDERR:
        // Error output in red
        System.err.print((char)27 + "[31m" + logMessage.getUtf8String());
        System.out.print((char)27 + "[0m"); // reset
        break;
      case END:
      default:
        break;
    }
  }}
