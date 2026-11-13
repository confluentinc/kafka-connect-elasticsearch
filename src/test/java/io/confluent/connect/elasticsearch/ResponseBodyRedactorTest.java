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

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import org.apache.http.HttpHost;
import org.apache.http.HttpVersion;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

public class ResponseBodyRedactorTest {

  private static final String SUMMARY = "method [POST], host [http://gw:8080], URI [/_bulk], "
      + "status line [HTTP/1.1 504 Gateway Time-out]";
  private static final String BODY = "<Error><ArgumentValue>Basic c2VjcmV0</ArgumentValue></Error>";

  // The production case: HLRC could not parse the gateway's body and kept the raw response as the cause.
  @Test
  public void describesAnUnparsableResponseByTheSummaryLinesOfItsChain() {
    RuntimeException failure = new RuntimeException("Unable to parse response body",
        new IOException(SUMMARY + "\n" + BODY));
    failure.addSuppressed(new IllegalStateException("Unsupported Content-Type: application/xml"));

    assertThat(ResponseBodyRedactor.describe(failure)).isEqualTo(
        "java.lang.RuntimeException: Unable to parse response body; caused by java.io.IOException: "
            + SUMMARY);
  }

  // HLRC's failed version check wraps the response twice, and each wrapper repeats the whole message.
  @Test
  public void describesWrappersThatOnlyRepeatTheirCauseByClassName() {
    RuntimeException failure = new RuntimeException(
        new ExecutionException(new IOException(SUMMARY + "\n" + BODY)));

    assertThat(ResponseBodyRedactor.describe(failure)).isEqualTo(
        "java.lang.RuntimeException; caused by java.util.concurrent.ExecutionException; "
            + "caused by java.io.IOException: " + SUMMARY);
  }

  // A JSON error body is parsed onto line 1, so an echoed credential there must be masked.
  @Test
  public void describeMasksCredentialsThatFollowAnAuthScheme() {
    ElasticsearchStatusException failure = new ElasticsearchStatusException(
        "Elasticsearch exception [type=exception, reason=Unsupported Authorization Type: "
            + "Basic dXNlcjpwYXNz]", RestStatus.GATEWAY_TIMEOUT);

    assertThat(ResponseBodyRedactor.describe(failure)).isEqualTo(
        "org.elasticsearch.ElasticsearchStatusException: Elasticsearch exception [type=exception, "
            + "reason=Unsupported Authorization Type: Basic [redacted]]");
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

  // HLRC attaches the raw response as the cause, or as a suppressed exception once it parsed the body.
  @Test
  public void flagsAResponseAnywhereInTheChain() throws IOException {
    ResponseException bodyless = response("HEAD", "/index", 403, "Forbidden", null);
    ElasticsearchStatusException parsed = new ElasticsearchStatusException(
        "Elasticsearch exception [type=security_exception, reason=denied]", RestStatus.FORBIDDEN);
    parsed.addSuppressed(response("POST", "/_bulk", 403, "Forbidden", "{\"error\":\"denied\"}"));

    assertThat(ResponseBodyRedactor.mayQuoteResponse(bodyless)).isTrue();
    assertThat(ResponseBodyRedactor.mayQuoteResponse(parsed)).isTrue();
    assertThat(ResponseBodyRedactor.mayQuoteResponse(new IOException("outer", bodyless))).isTrue();
  }

  // Anything carrying more than one line of text is treated as possibly quoting a response.
  @Test
  public void flagsMultiLineMessages() {
    assertThat(ResponseBodyRedactor.mayQuoteResponse(new IOException(SUMMARY + "\n" + BODY))).isTrue();
  }

  // Failures with no response keep their full exception, including a network error that HLRC's
  // version check wraps in an ElasticsearchException.
  @Test
  public void leavesFailuresWithoutAResponseAlone() {
    assertThat(ResponseBodyRedactor.mayQuoteResponse(new ElasticsearchException(
        new ExecutionException(new ConnectException("Connection refused"))))).isFalse();
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

  private static ResponseException response(String method, String uri, int status, String reason,
                                            String body) throws IOException {
    Response response = mock(Response.class);
    when(response.getRequestLine()).thenReturn(new BasicRequestLine(method, uri, HttpVersion.HTTP_1_1));
    when(response.getHost()).thenReturn(new HttpHost("gw", 8080, "http"));
    when(response.getStatusLine())
        .thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, status, reason));
    when(response.getEntity())
        .thenReturn(body == null ? null : new StringEntity(body, StandardCharsets.UTF_8));
    return new ResponseException(response);
  }
}
