package io.confluent.connect.elasticsearch.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseTransformer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.Response;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ElasticsearchConnectorNetworkIT extends BaseConnectorIT {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort(), false);

  protected static final int NUM_RECORDS = 5;
  protected static final int TASKS_MAX = 1;

  protected static final String CONNECTOR_NAME = "es-connector";
  protected static final String TOPIC = "test";
  protected Map<String, String> props;

  @Before
  public void setup() {
    startConnect();
    connect.kafka().createTopic(TOPIC);
    props = createProps();

    stubFor(any(anyUrl()).atPriority(10).willReturn(ok()));
  }

  @After
  public void cleanup() {
    stopConnect();
  }

  /**
   * Transformer that blocks all incoming requests until {@link #release(int)} is called
   * to fairly unblock a given number of requests.
   */
  public static class ConcurrencyTransformer extends ResponseTransformer {

    private final Semaphore s = new Semaphore(0, true);
    private final AtomicInteger requestCount = new AtomicInteger();

    public ConcurrencyTransformer() {
      s.drainPermits();
    }

    @Override
    public Response transform(Request request, Response response, FileSource files, Parameters parameters) {
      try {
        s.acquire();
      } catch (InterruptedException e) {
        throw new ConnectException(e);
      } finally {
        s.release();
      }
      requestCount.incrementAndGet();
      return response;
    }

    @Override
    public String getName() {
      return "concurrency";
    }

    public void release(int permits) {
      s.release(permits);
    }

    /**
     * How many requests are currently blocked
     */
    public int queueLength() {
      return s.getQueueLength();
    }

    /**
     * How many requests have been processed
     */
    public int requestCount() {
      return requestCount.get();
    }

    @Override
    public boolean applyGlobally() {
      return false;
    }

  }

  @Test
  public void testRetry() throws InterruptedException {
    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .inScenario("bulkRetry1")
            .whenScenarioStateIs(Scenario.STARTED)
            .withRequestBody(WireMock.containing("{\"doc_num\":0}"))
            .willReturn(aResponse().withStatus(500))
            .willSetStateTo("Failed"));

    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .inScenario("bulkRetry1")
            .whenScenarioStateIs("Failed")
            .withRequestBody(WireMock.containing("{\"doc_num\":0}"))
            .willSetStateTo("Fixed")
            .willReturn(okJson("{\n" +
                    "   \"took\": 30,\n" +
                    "   \"errors\": false,\n" +
                    "   \"items\": [\n" +
                    "      {\n" +
                    "         \"index\": {\n" +
                    "            \"_index\": \"test\",\n" +
                    "            \"_type\": \"_doc\",\n" +
                    "            \"_id\": \"1\",\n" +
                    "            \"_version\": 1,\n" +
                    "            \"result\": \"created\",\n" +
                    "            \"_shards\": {\n" +
                    "               \"total\": 2,\n" +
                    "               \"successful\": 1,\n" +
                    "               \"failed\": 0\n" +
                    "            },\n" +
                    "            \"status\": 201,\n" +
                    "            \"_seq_no\" : 0,\n" +
                    "            \"_primary_term\": 1\n" +
                    "         }\n" +
                    "      }\n" +
                    "   ]\n" +
                    "}")));

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);
    writeRecords(4);

    await().untilAsserted(
            () -> assertThat(wireMockRule.getAllScenarios().getScenarios().get(0).getState())
                    .isEqualTo("Fixed"));

    assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).state())
                    .isEqualTo("RUNNING");
  }

  @Test
  public void testConcurrentRequests() throws Exception {
    ConcurrencyTransformer concurrencyTransformer = new ConcurrencyTransformer();
    WireMockServer wireMockServer = new WireMockServer(options().dynamicPort()
            .extensions(concurrencyTransformer));

    try {
      wireMockServer.start();
      wireMockServer.stubFor(post(urlPathEqualTo("/_bulk"))
              .willReturn(okJson("{\n" +
                      "   \"took\": 30,\n" +
                      "   \"errors\": false,\n" +
                      "   \"items\": [\n" +
                      "      {\n" +
                      "         \"index\": {\n" +
                      "            \"_index\": \"test\",\n" +
                      "            \"_type\": \"_doc\",\n" +
                      "            \"_id\": \"1\",\n" +
                      "            \"_version\": 1,\n" +
                      "            \"result\": \"created\",\n" +
                      "            \"_shards\": {\n" +
                      "               \"total\": 2,\n" +
                      "               \"successful\": 1,\n" +
                      "               \"failed\": 0\n" +
                      "            },\n" +
                      "            \"status\": 201,\n" +
                      "            \"_seq_no\" : 0,\n" +
                      "            \"_primary_term\": 1\n" +
                      "         }\n" +
                      "      }\n" +
                      "   ]\n" +
                      "}")
                      .withTransformers(concurrencyTransformer.getName())));
      wireMockServer.stubFor(any(anyUrl()).atPriority(10).willReturn(ok()));

      props.put(CONNECTION_URL_CONFIG, wireMockServer.url("/"));
      props.put(READ_TIMEOUT_MS_CONFIG, "60000");
      props.put(MAX_RETRIES_CONFIG, "0");
      props.put(LINGER_MS_CONFIG, "60000");
      props.put(BATCH_SIZE_CONFIG, "1");
      props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "4");

      connect.configureConnector(CONNECTOR_NAME, props);
      waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);
      writeRecords(10);

      // TODO MAX_IN_FLIGHT_REQUESTS_CONFIG is misleading (it allows 1 less concurrent request
      // than configure), but fixing it would be a breaking change.
      // Consider allowing 0 (blocking) and removing "-1"
      await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
              assertThat(concurrencyTransformer.queueLength()).isEqualTo(3));

      concurrencyTransformer.release(10);

      await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
              assertThat(concurrencyTransformer.requestCount()).isEqualTo(10));
    } finally {
      wireMockServer.stop();
    }
  }

  @Test
  public void testReadTimeout() throws Exception {
    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(ok().withFixedDelay(2_000)));

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);
    writeRecords(NUM_RECORDS);

    // Connector should fail since the request takes longer than request timeout
    await().atMost(Duration.ofMinutes(1)).untilAsserted(() ->
            assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).state())
                    .isEqualTo("FAILED"));

    assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).trace())
            .contains("Failed to execute bulk request due to 'java.net.SocketTimeoutException: " +
                    "1,000 milliseconds timeout on connection")
            .contains("after 3 attempt(s)");

    // 1 + 2 retries
    verify(3, postRequestedFor(urlPathEqualTo("/_bulk")));
  }

  @Test
  public void testTooManyRequests() throws Exception {
    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(aResponse()
                    .withStatus(429)
                    .withHeader(CONTENT_TYPE, "application/json")
                    .withBody("{\n" +
                    "  \"error\": {\n" +
                    "    \"type\": \"circuit_breaking_exception\",\n" +
                    "    \"reason\": \"Data too large\",\n" +
                    "    \"bytes_wanted\": 123848638,\n" +
                    "    \"bytes_limit\": 123273216,\n" +
                    "    \"durability\": \"TRANSIENT\"\n" +
                    "  },\n" +
                    "  \"status\": 429\n" +
                    "}")));

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);
    writeRecords(NUM_RECORDS);

    // Connector should fail since the request takes longer than request timeout
    await().atMost(Duration.ofMinutes(1)).untilAsserted(() ->
            assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).state())
                    .isEqualTo("FAILED"));

    assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).trace())
            .contains("Failed to execute bulk request due to 'ElasticsearchStatusException" +
                    "[Elasticsearch exception [type=circuit_breaking_exception, " +
                    "reason=Data too large]]' after 3 attempt(s)");

    // 1 + 2 retries
    verify(3, postRequestedFor(urlPathEqualTo("/_bulk")));
  }

  @Test
  public void testServiceUnavailable() throws Exception {
    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(aResponse()
                    .withStatus(503)));

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);
    writeRecords(NUM_RECORDS);

    // Connector should fail since the request takes longer than request timeout
    await().atMost(Duration.ofMinutes(1)).untilAsserted(() ->
            assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).state())
                    .isEqualTo("FAILED"));

    assertThat(connect.connectorStatus(CONNECTOR_NAME).tasks().get(0).trace())
            .contains("[HTTP/1.1 503 Service Unavailable]")
            .contains("after 3 attempt(s)");

    // 1 + 2 retries
    verify(3, postRequestedFor(urlPathEqualTo("/_bulk")));
  }

  protected Map<String, String> createProps() {
    Map<String, String> props = new HashMap<>();

    // generic configs
    props.put(CONNECTOR_CLASS_CONFIG, ElasticsearchSinkConnector.class.getName());
    props.put(TOPICS_CONFIG, TOPIC);
    props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put("value.converter." + SCHEMAS_ENABLE_CONFIG, "false");

    // connectors specific
    props.put(CONNECTION_URL_CONFIG, wireMockRule.url("/"));
    props.put(IGNORE_KEY_CONFIG, "true");
    props.put(IGNORE_SCHEMA_CONFIG, "true");

    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(LINGER_MS_CONFIG, "60000");
    props.put(BATCH_SIZE_CONFIG, "4");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");

    return props;
  }

  protected void writeRecords(int numRecords) {
    for (int i = 0; i < numRecords; i++) {
      connect.kafka().produce(TOPIC, String.valueOf(i), String.format("{\"doc_num\":%d}", i));
    }
  }

}
