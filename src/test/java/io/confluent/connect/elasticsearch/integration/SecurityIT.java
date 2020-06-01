package io.confluent.connect.elasticsearch.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.core.Search;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

  EmbeddedConnectCluster connect;

  private static final String MESSAGE_KEY = "message-key";
  private static final String MESSAGE_VAL = "{ \"schema\": { \"type\": \"map\", \"keys\": "
      + "{ \"type\" : \"string\" }, \"values\": { \"type\" : \"int32\" } }, "
      + "\"payload\": { \"key1\": 12, \"key2\": 15} }";
  private static final String CONNECTOR_NAME = "elastic-sink";
  static final String KAFKA_TOPIC = "test-elasticsearch-sink";
  private static final String TYPE_NAME = "kafka-connect";
  private static final int TASKS_MAX = 1;
  private static final int NUM_MSG = 200;
  private static final long VERIFY_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(2);

  @Before
  public void setup() throws IOException {
    // Relevant and available docker images for elastic can be found at https://www.docker.elastic.co
    container = ElasticsearchContainer.fromSystemProperties().withSslEnabled(true);
    container.start();

    connect = new EmbeddedConnectCluster.Builder().name("elastic-sink-cluster").build();
    connect.start();
  }

  @After
  public void close() {
    connect.stop();
    container.close();
  }

  Map<String, String> getProps () {
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
   * Certificates are generated in src/test/resources/ssl/start-elasticsearch.sh
   */
  @Test
  public void testSecureConnectionVerifiedHostname() throws Throwable {
    // Use IP address here because that's what the certificates allow
    String address = container.getConnectionUrl();
    address = address.replace(container.getContainerIpAddress(), container.hostMachineIpAddress());
    log.info("Creating connector for {}", address);

    connect.kafka().createTopic(KAFKA_TOPIC, 1);

    Map<String, String> props = getProps();
    props.put("connection.url", address);
    props.put("elastic.security.protocol", "SSL");
    props.put("elastic.https.ssl.keystore.location", container.getKeystorePath());
    props.put("elastic.https.ssl.keystore.password", container.getKeystorePassword());
    props.put("elastic.https.ssl.key.password", container.getKeyPassword());
    props.put("elastic.https.ssl.truststore.location", container.getTruststorePath());
    props.put("elastic.https.ssl.truststore.password", container.getTruststorePassword());

    // Start connector
    testSecureConnection(props);
  }

  void testSecureConnection(Map<String, String> props) throws Throwable {
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForCondition(() -> {
      try {
        ConnectorStateInfo info = connect.connectorStatus(CONNECTOR_NAME);
        return info != null && info.tasks() != null && info.tasks().size() == 1;
      } catch (ConnectRestException e) {
        return false;
      }
    }, "Timed out waiting for connector task to start");

    for (int i=0; i<NUM_MSG; i++){
      connect.kafka().produce(KAFKA_TOPIC, MESSAGE_KEY+i, MESSAGE_VAL);
    }
    verify(getClient(props));
    verify(getClient(props));
  }

  private JestClient getClient(Map<String, String> props) {
    JestClientFactory factory = new JestClientFactory();
    factory.setHttpClientConfig(getClientConfig(new ElasticsearchSinkConnectorConfig(props)));
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
        } catch (NullPointerException e) {
          // no valid results yet, but kind of expected so no need to log
          return false;
        } catch (Exception e) {
          log.error("Retrying after failing to read data from Elastic: {}", e.getMessage(), e);
          return false;
        }
      }, VERIFY_TIMEOUT_MS, "Could not read data from Elastic");
  }
}
