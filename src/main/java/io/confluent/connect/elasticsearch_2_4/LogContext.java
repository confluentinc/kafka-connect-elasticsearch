/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package io.confluent.connect.elasticsearch_2_4;

import org.slf4j.MDC;

/**
 * Utility that works with Connect's MDC logging when it is enabled, so that the threads
 * created by this connector also include the same connector task specific MDC context plus
 * additional information that distinguishes those threads from each other and from the task thread.
 */
public class LogContext implements AutoCloseable {

  /**
   * We can't reference Connect's constant, since this connector could be deployed to Connect
   * runtimes that don't yet have it.
   */
  private static final String CONNECTOR_CONTEXT = "connector.context";

  private final String previousContext;
  private final String currentContext;

  public LogContext() {
    this(MDC.get(CONNECTOR_CONTEXT), null);
  }

  protected LogContext(String currentContext, String suffix) {
    this.previousContext = currentContext;
    if (currentContext != null && suffix != null && !suffix.trim().isEmpty()) {
      this.currentContext = currentContext + suffix.trim() + " ";
      MDC.put(CONNECTOR_CONTEXT, this.currentContext);
    } else {
      this.currentContext = null;
    }
  }

  public LogContext create(String suffix) {
    if (previousContext != null && suffix != null && !suffix.trim().isEmpty()) {
      return new LogContext(previousContext, suffix);
    }
    return this;
  }

  @Override
  public void close() {
    if (currentContext != null) {
      if (previousContext != null) {
        MDC.put(CONNECTOR_CONTEXT, previousContext);
      } else {
        MDC.remove(CONNECTOR_CONTEXT);
      }
    }
  }
}
