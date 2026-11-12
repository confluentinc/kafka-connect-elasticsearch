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

package io.confluent.connect.elasticsearch.helper;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Records every event reaching the root logger while open, each rendered the way an appender
 * would print it: level, logger, message and, when one is attached, the full stack trace.
 */
public class LogCapture extends AppenderSkeleton implements AutoCloseable {

  private final List<LoggingEvent> events = new CopyOnWriteArrayList<>();

  public LogCapture() {
    Logger.getRootLogger().addAppender(this);
  }

  @Override
  protected void append(LoggingEvent event) {
    events.add(event);
  }

  /** Every captured event, rendered with its stack trace. */
  public List<String> rendered() {
    return events.stream().map(LogCapture::render).collect(Collectors.toList());
  }

  /** The messages logged at {@code level} by {@code loggerClass}. */
  public List<String> messages(Level level, Class<?> loggerClass) {
    return events.stream()
        .filter(e -> e.getLevel().equals(level))
        .filter(e -> e.getLoggerName().equals(loggerClass.getName()))
        .map(LoggingEvent::getRenderedMessage)
        .collect(Collectors.toList());
  }

  private static String render(LoggingEvent event) {
    StringBuilder line = new StringBuilder()
        .append(event.getLevel()).append(' ')
        .append(event.getLoggerName()).append(' ')
        .append(event.getRenderedMessage());
    String[] trace = event.getThrowableStrRep();
    if (trace != null) {
      for (String frame : trace) {
        line.append('\n').append(frame);
      }
    }
    return line.toString();
  }

  @Override
  public void close() {
    Logger.getRootLogger().removeAppender(this);
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }
}
