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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

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


}
