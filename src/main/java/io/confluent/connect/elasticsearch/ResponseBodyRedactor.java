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

import java.util.regex.Pattern;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.ResponseException;

/**
 * Keeps HTTP response bodies, and credentials echoed into them, out of logs, task traces and
 * validation errors. The low-level client's {@link ResponseException} message is a one-line
 * request summary followed by the raw response body; HLRC copies text it parses from that body
 * into {@link ElasticsearchException} messages; and wrapping exceptions repeat their cause's
 * message. A proxy in front of Elasticsearch can fill the body with the request's Authorization
 * header.
 */
final class ResponseBodyRedactor {

  // The credential after an HTTP authentication scheme, as in "Basic dXNlcjpwYXNz".
  private static final Pattern CREDENTIAL = Pattern.compile(
      "(?i)\\b(Basic|Bearer|ApiKey|Negotiate)\\s+[A-Za-z0-9+/=._~-]+");
  private static final int MAX_DEPTH = 10;

  private ResponseBodyRedactor() {
  }

  /**
   * Whether {@code failure} may quote an HTTP response: it, a cause or a suppressed exception is an
   * {@link ElasticsearchException}, which HLRC builds for every error response, or has a message
   * of more than one line. Such a failure must be logged through {@link #describe} and never
   * attached as a throwable.
   */
  static boolean mayQuoteResponse(Throwable failure) {
    return mayQuoteResponse(failure, 0);
  }

  private static boolean mayQuoteResponse(Throwable t, int depth) {
    if (t == null || depth > MAX_DEPTH) {
      return false;
    }
    if (t instanceof ElasticsearchException
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
   * of its message, with credentials after an authentication scheme masked. A wrapper whose
   * message only repeats its cause is shown by class name alone.
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
        line.append(": ").append(firstLine(message));
      }
    }
    return mask(line.toString());
  }

  /**
   * The first line of {@code failure}'s message, with credentials masked; null if it has none.
   */
  static String safeMessage(Throwable failure) {
    String message = failure.getMessage();
    return message == null ? null : mask(firstLine(message));
  }

  private static String firstLine(String message) {
    int newline = message.indexOf('\n');
    return newline < 0 ? message : message.substring(0, newline);
  }

  private static String mask(String text) {
    return CREDENTIAL.matcher(text).replaceAll("$1 [redacted]");
  }
}
