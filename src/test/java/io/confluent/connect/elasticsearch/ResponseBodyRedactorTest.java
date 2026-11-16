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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorResponse;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletionException;
import org.apache.http.HttpHost;
import org.apache.http.HttpVersion;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.junit.Test;

public class ResponseBodyRedactorTest {

  private static final String SUMMARY = "method [POST], host [http://gw:8080], URI [/_bulk], "
      + "status line [HTTP/1.1 504 Gateway Time-out]";
  private static final String BODY = "<Error><ArgumentValue>Basic c2VjcmV0</ArgumentValue></Error>";
  private static final String CIRCUIT_BREAKER = "{\n"
      + "  \"error\": {\n"
      + "    \"type\": \"circuit_breaking_exception\",\n"
      + "    \"reason\": \"Data too large\",\n"
      + "    \"bytes_wanted\": 123848638\n"
      + "  },\n"
      + "  \"status\": 429\n"
      + "}";

  // The production case: the low-level client raises a gateway's 504 with the body after the summary.
  @Test
  public void describesAResponseByItsSummaryLine() throws IOException {
    ResponseException failure = response(504, "Gateway Time-out", BODY, ContentType.APPLICATION_XML);

    assertThat(ResponseBodyRedactor.describe(failure))
        .isEqualTo("org.elasticsearch.client.ResponseException: " + SUMMARY);
  }

  // The async client wraps the response in a CompletionException that repeats its whole message.
  @Test
  public void describesWrappersThatOnlyRepeatTheirCauseByClassName() throws IOException {
    CompletionException failure = new CompletionException(
        response(504, "Gateway Time-out", BODY, ContentType.APPLICATION_XML));

    assertThat(ResponseBodyRedactor.describe(failure)).isEqualTo(
        "java.util.concurrent.CompletionException; caused by "
            + "org.elasticsearch.client.ResponseException: " + SUMMARY);
  }

  // The Java API client leaves a 429 or 5xx undecoded, so Elasticsearch's reason is read from the body.
  @Test
  public void keepsTheElasticsearchErrorOfAJsonResponse() throws IOException {
    ResponseException failure =
        response(429, "Too Many Requests", CIRCUIT_BREAKER, ContentType.APPLICATION_JSON);
    String summary = "method [POST], host [http://gw:8080], URI [/_bulk], "
        + "status line [HTTP/1.1 429 Too Many Requests], "
        + "error [type=circuit_breaking_exception, reason=Data too large]";

    assertThat(ResponseBodyRedactor.describe(failure))
        .isEqualTo("org.elasticsearch.client.ResponseException: " + summary);
    assertThat(ResponseBodyRedactor.safeMessage(failure)).isEqualTo(summary);
  }

  // A gateway can put the echoed header in a JSON error string, which is kept as the reason.
  @Test
  public void masksCredentialsInAJsonErrorString() throws IOException {
    ResponseException failure = response(504, "Gateway Time-out",
        "{\"error\":\"Unsupported Authorization Type: Basic dXNlcjpwYXNz\",\"status\":504}",
        ContentType.APPLICATION_JSON);

    assertThat(ResponseBodyRedactor.describe(failure)).isEqualTo(
        "org.elasticsearch.client.ResponseException: " + SUMMARY
            + ", error [Unsupported Authorization Type: Basic [redacted]]");
  }

  // Only an Elasticsearch-shaped JSON error is read; anything else adds nothing to the summary.
  @Test
  public void ignoresABodyThatIsNotAnElasticsearchError() throws IOException {
    assertThat(ResponseBodyRedactor.describe(
        response(504, "Gateway Time-out", BODY, ContentType.APPLICATION_JSON)))
        .isEqualTo("org.elasticsearch.client.ResponseException: " + SUMMARY);
    assertThat(ResponseBodyRedactor.describe(
        response(504, "Gateway Time-out", "{\"message\":\"timeout\"}", ContentType.APPLICATION_JSON)))
        .isEqualTo("org.elasticsearch.client.ResponseException: " + SUMMARY);
    assertThat(ResponseBodyRedactor.describe(
        response(504, "Gateway Time-out", "{\"error\":{\"type\":\"x\",\"reason\":\"y\"}}",
            ContentType.TEXT_PLAIN)))
        .isEqualTo("org.elasticsearch.client.ResponseException: " + SUMMARY);
  }

  // The client decodes a 4xx error into its own exception, whose message carries the error's reason.
  @Test
  public void describeMasksCredentialsThatFollowAnAuthScheme() {
    ElasticsearchException failure = new ElasticsearchException("bulk", ErrorResponse.of(r -> r
        .status(401)
        .error(e -> e.type("exception").reason("Unsupported Authorization Type: Basic dXNlcjpwYXNz"))));

    assertThat(ResponseBodyRedactor.describe(failure)).isEqualTo(
        "co.elastic.clients.elasticsearch._types.ElasticsearchException: [bulk] failed: [exception] "
            + "Unsupported Authorization Type: Basic [redacted]");
    assertThat(ResponseBodyRedactor.describe(new IOException("bearer eyJhbGciOi.eyJzdWIi.c2ln")))
        .isEqualTo("java.io.IOException: bearer [redacted]");
    assertThat(ResponseBodyRedactor.describe(new IOException("ApiKey VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHA")))
        .isEqualTo("java.io.IOException: ApiKey [redacted]");
  }

  // A cause cycle must not loop forever.
  @Test
  public void describeTerminatesOnACauseCycle() {
    IOException first = new IOException("first");
    IOException second = new IOException("second", first);
    first.initCause(second);

    assertThat(ResponseBodyRedactor.describe(first))
        .startsWith("java.io.IOException: first; caused by java.io.IOException: second");
  }

  // A raw response or an error the client decoded from one may sit anywhere in the chain.
  @Test
  public void flagsAResponseAnywhereInTheChain() throws IOException {
    ResponseException bodyless = response(504, "Gateway Time-out", null, null);
    IOException suppressing = new IOException("outer");
    suppressing.addSuppressed(bodyless);
    ElasticsearchException decoded = new ElasticsearchException("bulk", ErrorResponse.of(r -> r
        .status(403)
        .error(e -> e.type("security_exception").reason("denied"))));

    assertThat(ResponseBodyRedactor.mayQuoteResponse(bodyless)).isTrue();
    assertThat(ResponseBodyRedactor.mayQuoteResponse(new IOException("outer", bodyless))).isTrue();
    assertThat(ResponseBodyRedactor.mayQuoteResponse(suppressing)).isTrue();
    assertThat(ResponseBodyRedactor.mayQuoteResponse(new CompletionException(decoded))).isTrue();
  }

  // Anything carrying more than one line of text is treated as possibly quoting a response.
  @Test
  public void flagsMultiLineMessages() {
    assertThat(ResponseBodyRedactor.mayQuoteResponse(new IOException(SUMMARY + "\n" + BODY))).isTrue();
  }

  // Failures with no response keep their full exception.
  @Test
  public void leavesFailuresWithoutAResponseAlone() {
    assertThat(ResponseBodyRedactor.mayQuoteResponse(
        new CompletionException(new ConnectException("Connection refused")))).isFalse();
    assertThat(ResponseBodyRedactor.mayQuoteResponse(
        new SocketTimeoutException("1,000 milliseconds timeout on connection"))).isFalse();
    assertThat(ResponseBodyRedactor.mayQuoteResponse(
        new IOException("wrapped", new ConnectException("Connection refused")))).isFalse();
    assertThat(ResponseBodyRedactor.mayQuoteResponse(new NullPointerException())).isFalse();
  }

  // The unexamined rest of a chain deeper than the walk may still hold a response.
  @Test
  public void treatsAChainDeeperThanTheWalkAsQuotingAResponse() {
    Throwable failure = new IOException("Connection refused");
    for (int i = 0; i < 11; i++) {
      failure = new IOException("wrapper " + i, failure);
    }

    assertThat(ResponseBodyRedactor.mayQuoteResponse(failure)).isTrue();
  }

  // Validation errors show one message, so it gets the same first-line cut and masking.
  @Test
  public void safeMessageKeepsOneMaskedLine() {
    assertThat(ResponseBodyRedactor.safeMessage(new IOException(SUMMARY + "\n" + BODY)))
        .isEqualTo(SUMMARY);
    assertThat(ResponseBodyRedactor.safeMessage(new IOException("reason: Basic dXNlcjpwYXNz")))
        .isEqualTo("reason: Basic [redacted]");
    assertThat(ResponseBodyRedactor.safeMessage(new IOException("Deleted resource.")))
        .isEqualTo("Deleted resource.");
    assertThat(ResponseBodyRedactor.safeMessage(new IOException())).isNull();
  }

  private static ResponseException response(int status, String reason, String body,
                                            ContentType contentType) throws IOException {
    Response response = mock(Response.class);
    when(response.getRequestLine()).thenReturn(new BasicRequestLine("POST", "/_bulk", HttpVersion.HTTP_1_1));
    when(response.getHost()).thenReturn(new HttpHost("gw", 8080, "http"));
    when(response.getStatusLine())
        .thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, status, reason));
    when(response.getEntity())
        .thenReturn(body == null ? null : new StringEntity(body, contentType));
    return new ResponseException(response);
  }
}
