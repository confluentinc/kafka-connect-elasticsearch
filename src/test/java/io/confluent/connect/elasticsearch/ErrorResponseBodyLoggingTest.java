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

package io.confluent.connect.elasticsearch;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG;
import static io.confluent.connect.elasticsearch.helper.ElasticSearchMockUtil.basicEmptyOk;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.confluent.connect.elasticsearch.helper.LogCapture;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * A proxy in front of Elasticsearch answered bulk requests with a 504 whose XML body echoed the
 * client's Authorization header. The low-level client puts the whole response body in its
 * exception message, so nothing derived from that exception may reach a log line, a task trace
 * or a validation error.
 */
public class ErrorResponseBodyLoggingTest {

  private static final String TOPIC = "test";
  private static final String USERNAME = "elastic";
  private static final String PASSWORD = "gateway-echo-test-only";
  // The value the client sends as "Authorization: Basic <token>" on every request.
  private static final String TOKEN = Base64.getEncoder()
      .encodeToString((USERNAME + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8));
  private static final String BODY_MARKER = "Unsupported Authorization Type";
  private static final String ECHO_BODY = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      + "<Error><Code>InvalidArgument</Code><Message>" + BODY_MARKER + "</Message>"
      + "<ArgumentName>Authorization</ArgumentName><ArgumentValue>Basic " + TOKEN
      + "</ArgumentValue></Error>";

  @Rule
  public WireMockRule wireMock = new WireMockRule(options().dynamicPort(), false);

  private LogCapture logs;

  @Before
  public void setUp() {
    wireMock.stubFor(any(anyUrl()).atPriority(10).willReturn(basicEmptyOk()));
    logs = new LogCapture();
  }

  @After
  public void tearDown() {
    logs.close();
  }

  // Each retry warning and the final error of a rejected bulk request must omit the response body.
  @Test
  public void failedBulkRequestLogsNoResponseBody() {
    wireMock.stubFor(post(urlPathEqualTo("/_bulk")).atPriority(1).willReturn(gatewayEcho()));

    failTaskOnBulkRequest();

    assertNoResponseBody(logs.rendered());
  }

  // The exception that fails the task becomes the task trace, so its cause chain must omit the body too.
  @Test
  public void failedBulkRequestFailsTaskWithoutResponseBody() {
    wireMock.stubFor(post(urlPathEqualTo("/_bulk")).atPriority(1).willReturn(gatewayEcho()));

    ConnectException failure = failTaskOnBulkRequest();

    assertNoResponseBody(Collections.singletonList(stackTraceOf(failure)));
    assertThat(stackTraceOf(failure)).contains("Failed to execute bulk request due to "
        + "org.elasticsearch.ElasticsearchStatusException: Unable to parse response body; caused by "
        + "org.elasticsearch.client.ResponseException: method [POST]");
  }

  // The status line must be in the failure's own message, since the task trace may be all on-call sees.
  @Test
  public void failedBulkRequestMessageCarriesTheStatusLine() {
    wireMock.stubFor(post(urlPathEqualTo("/_bulk")).atPriority(1).willReturn(gatewayEcho()));

    ConnectException failure = failTaskOnBulkRequest();

    assertThat(failure.getCause().getMessage())
        .contains("status line [HTTP/1.1 504 Gateway Time-out] after 3 attempt(s)");
  }

  // The retry warning must keep the response summary on-call searches for, with the body cut off.
  @Test
  public void bulkRetryWarningKeepsTheResponseSummaryLine() {
    wireMock.stubFor(post(urlPathEqualTo("/_bulk")).atPriority(1).willReturn(gatewayEcho()));

    failTaskOnBulkRequest();

    assertThat(logs.messages(Level.WARN, RetryUtil.class))
        .isNotEmpty()
        .allSatisfy(m -> assertThat(m)
            .contains("bulk request due to org.elasticsearch.ElasticsearchStatusException: Unable to "
                + "parse response body; caused by org.elasticsearch.client.ResponseException: "
                + "method [POST]")
            .contains("status line [HTTP/1.1 504 Gateway Time-out]. Retrying attempt"));
  }

  // A JSON body with an "error" string is parsed onto the first line of the client's exception.
  @Test
  public void jsonErrorFieldEchoIsNotLogged() {
    wireMock.stubFor(post(urlPathEqualTo("/_bulk")).atPriority(1).willReturn(aResponse()
        .withStatus(504)
        .withHeader("Content-Type", "application/json")
        .withBody("{\"error\":\"" + BODY_MARKER + ": Basic " + TOKEN + "\",\"status\":504}")));

    ConnectException failure = failTaskOnBulkRequest();

    assertThat(logs.rendered()).noneSatisfy(e -> assertThat(e).contains(TOKEN));
    assertThat(stackTraceOf(failure)).doesNotContain(TOKEN);
  }

  // HLRC's version check wraps a refused connection in an ElasticsearchException although no response
  // arrived, so nothing can be quoted and the real exception must stay attached.
  @Test
  public void networkErrorAtStartupKeepsItsExceptionAndStackTrace() throws IOException {
    int closedPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      closedPort = socket.getLocalPort();
    }
    Map<String, String> props = props();
    props.put(CONNECTION_URL_CONFIG, "http://localhost:" + closedPort);

    new ElasticsearchClient(new ElasticsearchSinkConnectorConfig(props), null, () -> { }, 0,
        "test").close();

    assertThat(logs.rendered()).anySatisfy(e -> assertThat(e)
        .startsWith("WARN " + ElasticsearchClient.class.getName() + " Failed to get ES server version")
        .contains("Caused by: java.net.ConnectException")
        .contains("\tat "));
  }

  // Guards every path that talks to Elasticsearch, so a new log statement that attaches a client
  // exception fails here: every request gets the echo, then validation, the version probe and the
  // task's index check all run. The positive checks prove each path really logged.
  @Test
  public void noPathLogsTheResponseBody() {
    wireMock.stubFor(any(anyUrl()).atPriority(1).willReturn(gatewayEcho()));

    List<String> outputs = new ArrayList<>();
    for (ConfigValue value : new Validator(props()).validate().configValues()) {
      outputs.addAll(value.errorMessages());
    }
    outputs.add(stackTraceOf(failTaskOnBulkRequest()));
    outputs.addAll(logs.rendered());

    assertNoResponseBody(outputs);
    assertThat(logs.rendered())
        .anySatisfy(e -> assertThat(e).startsWith("ERROR " + Validator.class.getName()
            + " Failed to initialize Elasticsearch client due to"))
        .anySatisfy(e -> assertThat(e).startsWith("WARN " + ElasticsearchClient.class.getName()
            + " Failed to get ES server version due to"))
        .anySatisfy(e -> assertThat(e).startsWith("ERROR " + RetryUtil.class.getName()));
  }

  // A failure with no HTTP response, such as a read timeout, keeps its real exception and stack trace.
  @Test
  public void transportFailureKeepsItsExceptionAndStackTrace() {
    wireMock.stubFor(post(urlPathEqualTo("/_bulk")).atPriority(1)
        .willReturn(basicEmptyOk().withFixedDelay(2_000)));
    Map<String, String> props = props();
    props.put(READ_TIMEOUT_MS_CONFIG, "300");

    ConnectException failure = failTaskOnBulkRequest(props);

    assertThat(failure.getCause().getCause()).isInstanceOf(SocketTimeoutException.class);
    assertThat(logs.rendered()).anySatisfy(e -> assertThat(e)
        .startsWith("ERROR " + RetryUtil.class.getName())
        .contains("Caused by: java.net.SocketTimeoutException")
        .contains("\tat "));
  }

  // A gateway that labels the echo as JSON makes the client try to parse it, and parse errors quote input.
  @Test
  public void mislabelledJsonErrorBodyIsNotLogged() {
    wireMock.stubFor(post(urlPathEqualTo("/_bulk")).atPriority(1).willReturn(aResponse()
        .withStatus(401)
        .withHeader("Content-Type", "application/json")
        .withBody(ECHO_BODY)));

    ConnectException failure = failTaskOnBulkRequest();

    assertNoResponseBody(logs.rendered());
    assertNoResponseBody(Collections.singletonList(stackTraceOf(failure)));
  }

  // Elasticsearch's own one-line error reason is what makes a failure actionable; it must survive.
  @Test
  public void parsedElasticsearchErrorKeepsItsReason() {
    wireMock.stubFor(post(urlPathEqualTo("/_bulk")).atPriority(1).willReturn(aResponse()
        .withStatus(429)
        .withHeader("Content-Type", "application/json")
        .withHeader("X-Elastic-Product", "Elasticsearch")
        .withBody("{\"error\":{\"root_cause\":[{\"type\":\"circuit_breaking_exception\","
            + "\"reason\":\"Data too large\"}],\"type\":\"circuit_breaking_exception\","
            + "\"reason\":\"Data too large\"},\"status\":429}")));

    ConnectException failure = failTaskOnBulkRequest();

    assertThat(stackTraceOf(failure))
        .contains("type=circuit_breaking_exception, reason=Data too large");
  }

  // A parsed error has no cause, so a retry warning that logs only the cause says "due to null".
  @Test
  public void parsedElasticsearchErrorRetryWarningShowsTheReason() {
    wireMock.stubFor(post(urlPathEqualTo("/_bulk")).atPriority(1).willReturn(aResponse()
        .withStatus(429)
        .withHeader("Content-Type", "application/json")
        .withHeader("X-Elastic-Product", "Elasticsearch")
        .withBody("{\"error\":{\"type\":\"circuit_breaking_exception\","
            + "\"reason\":\"Data too large\"},\"status\":429}")));

    failTaskOnBulkRequest();

    assertThat(logs.messages(Level.WARN, RetryUtil.class))
        .isNotEmpty()
        .allSatisfy(m -> assertThat(m).contains("reason=Data too large"));
  }

  // The version probe at client start-up goes through its own catch block, outside RetryUtil.
  @Test
  public void serverVersionProbeLogsNoResponseBody() {
    wireMock.stubFor(get(urlPathEqualTo("/")).atPriority(1).willReturn(gatewayEcho()));

    new ElasticsearchClient(new ElasticsearchSinkConnectorConfig(props()), null, () -> { }, 0,
        "test").close();

    assertNoResponseBody(logs.rendered());
    assertThat(logs.rendered()).anySatisfy(e -> assertThat(e)
        .startsWith("WARN " + ElasticsearchClient.class.getName() + " Failed to get ES server version")
        .contains("status line [HTTP/1.1 504 Gateway Time-out]"));
  }

  // Validation errors are returned by the REST API and shown to the user, so they must omit the body.
  @Test
  public void validationErrorsOmitResponseBody() {
    wireMock.stubFor(any(anyUrl()).atPriority(1).willReturn(gatewayEcho()));

    List<String> errors = new ArrayList<>();
    for (ConfigValue value : new Validator(props()).validate().configValues()) {
      errors.addAll(value.errorMessages());
    }

    assertNoResponseBody(errors);
    assertThat(errors).anySatisfy(e -> assertThat(e).contains("status line [HTTP/1.1 504 Gateway Time-out]"));
  }

  // Validation logs a client that fails to initialize, which a failed version check looks like.
  @Test
  public void validationLogsNoResponseBody() {
    wireMock.stubFor(any(anyUrl()).atPriority(1).willReturn(gatewayEcho()));

    new Validator(props()).validate();

    assertNoResponseBody(logs.rendered());
  }

  private static ResponseDefinitionBuilder gatewayEcho() {
    return aResponse()
        .withStatus(504)
        .withStatusMessage("Gateway Time-out")
        .withHeader("Content-Type", "application/xml")
        .withBody(ECHO_BODY);
  }

  private Map<String, String> props() {
    Map<String, String> props = new HashMap<>();
    props.put(CONNECTION_URL_CONFIG, wireMock.url("/"));
    props.put(CONNECTION_USERNAME_CONFIG, USERNAME);
    props.put(CONNECTION_PASSWORD_CONFIG, PASSWORD);
    props.put(IGNORE_KEY_CONFIG, "true");
    props.put(IGNORE_SCHEMA_CONFIG, "true");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(BATCH_SIZE_CONFIG, "1");
    props.put(ElasticsearchSinkTaskConfig.TASK_ID_CONFIG, "1");
    return props;
  }

  // Starts a task, hands it one record and returns the failure it reports: at once when the index
  // check fails, otherwise once the bulk request has exhausted its retries in the background.
  private ConnectException failTaskOnBulkRequest() {
    return failTaskOnBulkRequest(props());
  }

  private ConnectException failTaskOnBulkRequest(Map<String, String> props) {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(Collections.singleton(tp));
    ElasticsearchSinkTask task = new ElasticsearchSinkTask();
    task.initialize(context);
    task.start(props);
    try {
      try {
        task.put(Collections.singletonList(new SinkRecord(TOPIC, 0, null, "key", null,
            Collections.singletonMap("field", "value"), 0)));
      } catch (ConnectException e) {
        return e;
      }
      return await().atMost(Duration.ofSeconds(30))
          .until(() -> putFailure(task), Objects::nonNull);
    } finally {
      task.stop();
    }
  }

  private static ConnectException putFailure(ElasticsearchSinkTask task) {
    try {
      task.put(Collections.emptyList());
      return null;
    } catch (ConnectException e) {
      return e;
    }
  }

  private static String stackTraceOf(Throwable t) {
    StringWriter out = new StringWriter();
    t.printStackTrace(new PrintWriter(out));
    return out.toString();
  }

  private static void assertNoResponseBody(List<String> outputs) {
    assertThat(outputs).noneSatisfy(o -> assertThat(o).contains(TOKEN));
    assertThat(outputs).noneSatisfy(o -> assertThat(o).contains(BODY_MARKER));
  }
}
