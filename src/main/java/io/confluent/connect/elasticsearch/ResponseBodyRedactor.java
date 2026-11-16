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

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.elasticsearch.client.ResponseException;

/**
 * Keeps HTTP response bodies, and credentials echoed into them, out of logs, task traces and
 * validation errors. The Java API client decodes a 400, 401, 403, 404 or 405 response itself,
 * into an {@link ElasticsearchException} whose message is the reason it read from the body. Any
 * other error status reaches the connector as the low-level client's {@link ResponseException},
 * whose message is a one-line request summary followed by the raw response body; and wrapping
 * exceptions repeat their cause's message. A proxy in front of Elasticsearch can fill the body
 * with the request's Authorization header.
 */
final class ResponseBodyRedactor {

  // The credential after an HTTP authentication scheme, as in "Basic dXNlcjpwYXNz".
  private static final Pattern CREDENTIAL = Pattern.compile(
      "(?i)\\b(Basic|Bearer|ApiKey|Negotiate)\\s+[A-Za-z0-9+/=._~-]+");
  private static final int MAX_DEPTH = 10;
  private static final ObjectMapper JSON = new ObjectMapper();

  private ResponseBodyRedactor() {
  }

  /**
   * Whether {@code failure} may quote an HTTP response: it, a cause or a suppressed exception is a
   * {@link ResponseException}, which carries the response, or an {@link ElasticsearchException},
   * which quotes the reason the client decoded from one, or has a message of more than one line.
   * Such a failure must be logged through {@link #describe} and never attached as a throwable.
   * A network error carries no response, so it keeps its stack trace.
   */
  static boolean mayQuoteResponse(Throwable failure) {
    return mayQuoteResponse(failure, 0);
  }

  private static boolean mayQuoteResponse(Throwable t, int depth) {
    if (t == null) {
      return false;
    }
    if (depth > MAX_DEPTH) {
      return true;  // the unexamined rest of the chain may still hold a response
    }
    if (t instanceof ResponseException || t instanceof ElasticsearchException
        || (t.getMessage() != null && t.getMessage().indexOf('\n') >= 0)) {
      return true;
    }
    for (Throwable suppressed : t.getSuppressed()) {
      if (mayQuoteResponse(suppressed, depth + 1)) {
        return true;
      }
    }
    return mayQuoteResponse(t.getCause(), depth + 1);
  }

  /**
   * Describes {@code failure} and its causes on one line: each one's class name and the first line
   * of its message, plus the Elasticsearch error of a raw JSON response, with credentials after an
   * authentication scheme masked. A wrapper whose message only repeats its cause is shown by class
   * name alone.
   */
  static String describe(Throwable failure) {
    StringBuilder line = new StringBuilder();
    Throwable t = failure;
    for (int depth = 0; t != null && depth < MAX_DEPTH; depth++, t = t.getCause()) {
      if (line.length() > 0) {
        line.append("; caused by ");
      }
      line.append(t.getClass().getName());
      String message = t.getMessage();
      if (message != null && (t.getCause() == null || !message.equals(t.getCause().toString()))) {
        line.append(": ").append(summary(t));
      }
    }
    return mask(line.toString());
  }

  /**
   * The first line of {@code failure}'s message, plus the Elasticsearch error of a raw JSON
   * response, with credentials masked; null if it has no message.
   */
  static String safeMessage(Throwable failure) {
    return failure.getMessage() == null ? null : mask(summary(failure));
  }

  // The Java API client leaves the body of a 429 or 5xx undecoded, and Elasticsearch's reason
  // there (such as a tripped circuit breaker) is what makes the failure actionable.
  private static String summary(Throwable t) {
    String summary = firstLine(t.getMessage());
    if (t instanceof ResponseException) {
      String error = elasticsearchError(((ResponseException) t).getResponse().getEntity());
      if (error != null) {
        summary += ", error [" + error + "]";
      }
    }
    return summary;
  }

  /**
   * The error of an Elasticsearch-shaped JSON body, as "type=..., reason=..." for an error object
   * or the text of an error string; null for any other body. The low-level client buffers the body
   * to build its message, so reading it again here is safe.
   */
  private static String elasticsearchError(HttpEntity body) {
    if (body == null || !body.isRepeatable() || !isJson(body.getContentType())) {
      return null;
    }
    try (InputStream content = body.getContent()) {
      JsonNode error = JSON.readTree(content).path("error");
      if (error.isTextual()) {
        return firstLine(error.asText());
      }
      if (error.hasNonNull("type")) {
        return firstLine("type=" + error.path("type").asText()
            + ", reason=" + error.path("reason").asText());
      }
    } catch (Exception e) {
      // Not JSON after all; the summary line is all there is to show.
    }
    return null;
  }

  private static boolean isJson(Header contentType) {
    if (contentType == null) {
      return false;
    }
    String type = contentType.getValue().toLowerCase(Locale.ROOT);
    return type.startsWith("application/json")
        || type.startsWith("application/vnd.elasticsearch+json");
  }

  private static String firstLine(String message) {
    int newline = message.indexOf('\n');
    return newline < 0 ? message : message.substring(0, newline);
  }

  private static String mask(String text) {
    return CREDENTIAL.matcher(text).replaceAll("$1 [redacted]");
  }
}
