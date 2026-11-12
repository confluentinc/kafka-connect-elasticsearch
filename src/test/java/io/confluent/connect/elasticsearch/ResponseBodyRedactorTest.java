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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.SocketTimeoutException;
import org.junit.Test;

public class ResponseBodyRedactorTest {

  private static final String BODY = "<Error><ArgumentValue>Basic c2VjcmV0</ArgumentValue></Error>";

  // Every link keeps its class and summary line, but nothing after the first line of any message.
  @Test
  public void printedTraceKeepsClassesAndSummariesButNoLaterLines() {
    IOException cause = new IOException("status line [HTTP/1.1 504 Gateway Time-out]\n" + BODY);
    RuntimeException failure = new RuntimeException("wrapped: " + cause, cause);

    String printed = stackTraceOf(ResponseBodyRedactor.redact(failure));

    assertThat(printed)
        .startsWith("java.lang.RuntimeException: wrapped: java.io.IOException: status line "
            + "[HTTP/1.1 504 Gateway Time-out]\n")
        .contains("Caused by: java.io.IOException: status line [HTTP/1.1 504 Gateway Time-out]\n")
        .doesNotContain(BODY);
  }

  // The copy must still point at the code that failed, or the stack trace is useless.
  @Test
  public void copyKeepsTheOriginalStackFrames() {
    IOException failure = new IOException("summary\n" + BODY);

    Throwable redacted = ResponseBodyRedactor.redact(failure);

    assertThat(redacted.getStackTrace()).isEqualTo(failure.getStackTrace());
  }

  // HLRC attaches the response-parsing failure, which can quote the body, as a suppressed exception.
  @Test
  public void suppressedExceptionsAreRedactedToo() {
    IOException failure = new IOException("Unable to parse response body");
    failure.addSuppressed(new IOException("Unexpected character ('<')\n at [Source: " + BODY + "]"));

    String printed = stackTraceOf(ResponseBodyRedactor.redact(failure));

    assertThat(printed)
        .contains("Suppressed: java.io.IOException: Unexpected character ('<')\n")
        .doesNotContain(BODY);
  }

  // A cause cycle must not recurse forever.
  @Test
  public void causeCycleTerminates() {
    IOException first = new IOException("first");
    IOException second = new IOException("second", first);
    first.initCause(second);

    String printed = stackTraceOf(ResponseBodyRedactor.redact(first));

    assertThat(printed).contains("java.io.IOException: first").contains("java.io.IOException: second");
  }

  // A single-line message prints exactly as before, so existing log searches keep working.
  @Test
  public void singleLineFailurePrintsAsTheOriginal() {
    SocketTimeoutException failure = new SocketTimeoutException("1,000 milliseconds timeout");
    IOException noMessage = new IOException();

    assertThat(ResponseBodyRedactor.redact(failure).toString()).isEqualTo(failure.toString());
    assertThat(ResponseBodyRedactor.redact(noMessage).toString()).isEqualTo("java.io.IOException");
  }

  private static String stackTraceOf(Throwable t) {
    StringWriter out = new StringWriter();
    t.printStackTrace(new PrintWriter(out));
    return out.toString();
  }
}
