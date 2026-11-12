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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Copies an exception chain without HTTP response bodies. The low-level client's
 * {@code ResponseException} message is a one-line request summary followed by the raw response
 * body, a proxy in front of Elasticsearch can echo the request's Authorization header into that
 * body, and wrapping exceptions repeat their cause's message in their own. Every link of the copy
 * therefore keeps only the first line of its message, along with its class name and stack trace,
 * so the copy can be logged or attached as a cause in place of the original.
 */
final class ResponseBodyRedactor {

  private ResponseBodyRedactor() {
  }

  /**
   * Returns a copy of {@code failure}, its causes and its suppressed exceptions, each with its
   * message cut to the first line.
   *
   * @param failure the exception to copy; may be null
   * @return the redacted copy, or null when {@code failure} is null
   */
  static Throwable redact(Throwable failure) {
    return redact(failure, Collections.newSetFromMap(new IdentityHashMap<>()));
  }

  private static Throwable redact(Throwable t, Set<Throwable> seen) {
    if (t == null || !seen.add(t)) {
      return null;
    }
    RedactedException copy = new RedactedException(
        t.getClass().getName(), firstLine(t.getMessage()), redact(t.getCause(), seen));
    copy.setStackTrace(t.getStackTrace());
    for (Throwable suppressed : t.getSuppressed()) {
      Throwable redacted = redact(suppressed, seen);
      if (redacted != null) {
        copy.addSuppressed(redacted);
      }
    }
    return copy;
  }

  private static String firstLine(String message) {
    if (message == null) {
      return null;
    }
    int newline = message.indexOf('\n');
    return newline < 0 ? message : message.substring(0, newline);
  }

  /** A copied link that prints as the exception it was copied from. */
  static final class RedactedException extends RuntimeException {

    private final String originalClass;

    RedactedException(String originalClass, String message, Throwable cause) {
      super(message, cause);
      this.originalClass = originalClass;
    }

    @Override
    public String toString() {
      String message = getLocalizedMessage();
      return message == null ? originalClass : originalClass + ": " + message;
    }
  }
}
