/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package io.confluent.connect.elasticsearch;

import org.slf4j.MDC;

public class LogContext implements AutoCloseable {

  private static final String CONNECTOR_CONTEXT = "connector.context";

  private final String previousContext;
  private final String currentContext;

  public LogContext() {
    this(null);
  }

  public LogContext(String suffix) {
    previousContext = MDC.get(CONNECTOR_CONTEXT);
    if (previousContext != null && suffix != null && !suffix.trim().isEmpty()) {
      currentContext = previousContext + " " + suffix + " ";
      MDC.put(CONNECTOR_CONTEXT, currentContext);
    } else {
      currentContext = null;
    }
  }

  public LogContext create(String suffix) {
    return new LogContext(suffix);
  }

  @Override
  public void close() {
    if (currentContext != null) {
      MDC.put(CONNECTOR_CONTEXT, previousContext);
    }
  }
}
