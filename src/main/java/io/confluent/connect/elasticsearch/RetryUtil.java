/*
 * Copyright 2018 Confluent Inc.
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

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to compute the retry times for a given attempt, using exponential backoff.
 *
 * <p>The purposes of using exponential backoff is to give the ES service time to recover when it
 * becomes overwhelmed. Adding jitter attempts to prevent a thundering herd, where large numbers
 * of requests from many tasks overwhelm the ES service, and without randomization all tasks
 * retry at the same time. Randomization should spread the retries out and should reduce the
 * overall time required to complete all attempts.
 * See <a href="https://www.awsarchitectureblog.com/2015/03/backoff.html">this blog post</a>
 * for details.
 */
public class RetryUtil {

  private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);

  /**
   * An arbitrary absolute maximum practical retry time.
   */
  public static final long MAX_RETRY_TIME_MS = TimeUnit.HOURS.toMillis(24);

  /**
   * Compute the time to sleep using exponential backoff with jitter. This method computes the
   * normal exponential backoff as {@code initialRetryBackoffMs << retryAttempt}, and then
   * chooses a random value between 0 and that value.
   *
   * @param retryAttempts         the number of previous retry attempts; must be non-negative
   * @param initialRetryBackoffMs the initial time to wait before retrying; assumed to
   *                              be 0 if value is negative
   * @return the non-negative time in milliseconds to wait before the next retry attempt,
   *         or 0 if {@code initialRetryBackoffMs} is negative
   */
  public static long computeRandomRetryWaitTimeInMillis(int retryAttempts,
                                                        long initialRetryBackoffMs) {
    if (initialRetryBackoffMs < 0) {
      return 0;
    }
    if (retryAttempts < 0) {
      return initialRetryBackoffMs;
    }
    long maxRetryTime = computeRetryWaitTimeInMillis(retryAttempts, initialRetryBackoffMs);
    return ThreadLocalRandom.current().nextLong(0, maxRetryTime);
  }

  /**
   * Compute the time to sleep using exponential backoff. This method computes the normal
   * exponential backoff as {@code initialRetryBackoffMs << retryAttempt}, bounded to always
   * be less than {@link #MAX_RETRY_TIME_MS}.
   *
   * @param retryAttempts         the number of previous retry attempts; must be non-negative
   * @param initialRetryBackoffMs the initial time to wait before retrying; assumed to be 0
   *                              if value is negative
   * @return the non-negative time in milliseconds to wait before the next retry attempt,
   *         or 0 if {@code initialRetryBackoffMs} is negative
   */
  public static long computeRetryWaitTimeInMillis(int retryAttempts,
                                                  long initialRetryBackoffMs) {
    if (initialRetryBackoffMs < 0) {
      return 0;
    }
    if (retryAttempts <= 0) {
      return initialRetryBackoffMs;
    }
    if (retryAttempts > 32) {
      // This would overflow the exponential algorithm ...
      return MAX_RETRY_TIME_MS;
    }
    long result = initialRetryBackoffMs << retryAttempts;
    return result < 0L ? MAX_RETRY_TIME_MS : Math.min(MAX_RETRY_TIME_MS, result);
  }

  /**
   * Call the supplied function up to the {@code maxTotalAttempts}.
   *
   * <p>The description of the function should be a succinct, human-readable present tense phrase
   * that summarizes the function, such as "read tables" or "connect to database" or
   * "make remote request". This description will be used within exception and log messages.
   *
   * @param description      present tense description of the action, used to create the error
   *                         message; may not be null
   * @param function         the function to call; may not be null
   * @param maxTotalAttempts maximum number of total attempts, including the first call
   * @param initialBackoff   the initial backoff in ms before retrying
   * @param <T>              the return type of the function to retry
   * @return the function's return value
   * @throws ConnectException        if the function failed after retries
   */
  public static <T> T callWithRetries(
      String description,
      Callable<T> function,
      int maxTotalAttempts,
      long initialBackoff
  ) {
    return callWithRetries(description, function, maxTotalAttempts, initialBackoff, Time.SYSTEM);
  }

  /**
   * Call the supplied function up to the {@code maxTotalAttempts}.
   *
   * <p>The description of the function should be a succinct, human-readable present tense phrase
   * that summarizes the function, such as "read tables" or "connect to database" or
   * "make remote request". This description will be used within exception and log messages.
   *
   * @param description      present tense description of the action, used to create the error
   *                         message; may not be null
   * @param function         the function to call; may not be null
   * @param maxTotalAttempts maximum number of attempts
   * @param initialBackoff   the initial backoff in ms before retrying
   * @param clock            the clock to use for waiting
   * @param <T>              the return type of the function to retry
   * @return the function's return value
   * @throws ConnectException        if the function failed after retries
   */
  protected static <T> T callWithRetries(
      String description,
      Callable<T> function,
      int maxTotalAttempts,
      long initialBackoff,
      Time clock
  ) {
    assert description != null;
    assert function != null;
    int attempt = 0;
    while (true) {
      ++attempt;
      try {
        log.trace(
            "Try {} (attempt {} of {})",
            description,
            attempt,
            maxTotalAttempts
        );
        T call = function.call();
        return call;
      } catch (Exception e) {
        if (attempt >= maxTotalAttempts) {
          String msg = String.format("Failed to %s due to '%s' after %d attempt(s)",
                  description, e, attempt);
          log.error(msg, e);
          throw new ConnectException(msg, e);
        }

        // Otherwise it is retriable and we should retry
        long backoff = computeRandomRetryWaitTimeInMillis(attempt, initialBackoff);

        log.warn("Failed to {} due to {}. Retrying attempt ({}/{}) after backoff of {} ms",
            description, e.getCause(), attempt, maxTotalAttempts, backoff);
        log.trace("Failed to {} due to exception :: {} and its stacktrace {}  "
                        + "Retrying attempt ({}/{}) after backoff of {} ms",
                         description, e, e.getStackTrace(), attempt,
                         maxTotalAttempts, backoff);
        log.trace("exception::", e);
        clock.sleep(backoff);
      }
    }
  }
}
