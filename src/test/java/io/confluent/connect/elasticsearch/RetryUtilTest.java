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

import java.io.IOException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RetryUtilTest {

  private int timesThrown;

  @Before
  public void setup() {
    timesThrown = 0;
  }

  @Test
  public void computeRetryBackoffForNegativeAttempts() {
    assertComputeRetryInRange(0, 10L);
    assertEquals(10L, RetryUtil.computeRandomRetryWaitTimeInMillis(-1, 10L));
  }

  @Test
  public void computeRetryBackoffForValidRanges() {
    assertComputeRetryInRange(10, 10L);
    assertComputeRetryInRange(10, 100L);
    assertComputeRetryInRange(10, 1000L);
    assertComputeRetryInRange(100, 1000L);
  }

  @Test
  public void computeRetryBackoffForNegativeRetryTimes() {
    assertComputeRetryInRange(1, -100L);
    assertComputeRetryInRange(10, -100L);
    assertComputeRetryInRange(100, -100L);
  }

  @Test
  public void computeNonRandomRetryTimes() {
    assertEquals(100L, RetryUtil.computeRetryWaitTimeInMillis(0, 100L));
    assertEquals(200L, RetryUtil.computeRetryWaitTimeInMillis(1, 100L));
    assertEquals(400L, RetryUtil.computeRetryWaitTimeInMillis(2, 100L));
    assertEquals(800L, RetryUtil.computeRetryWaitTimeInMillis(3, 100L));
    assertEquals(1600L, RetryUtil.computeRetryWaitTimeInMillis(4, 100L));
    assertEquals(3200L, RetryUtil.computeRetryWaitTimeInMillis(5, 100L));
  }

  @Test
  public void testCallWithRetriesNoRetries() throws Exception {
    MockTime mockClock = new MockTime();
    long expectedTime = mockClock.milliseconds();

    assertTrue(RetryUtil.callWithRetries("test", () -> testFunction(0), 3, 100, mockClock));
    assertEquals(expectedTime, mockClock.milliseconds());
  }

  @Test
  public void testCallWithRetriesSomeRetries() throws Exception {
    MockTime mockClock = spy(new MockTime());

    assertTrue(RetryUtil.callWithRetries("test", () -> testFunction(2), 3, 100, mockClock));
    verify(mockClock, times(2)).sleep(anyLong());
  }

  @Test(expected = ConnectException.class)
  public void testCallWithRetriesExhaustedRetries() throws Exception {
    MockTime mockClock = new MockTime();

    assertTrue(RetryUtil.callWithRetries("test", () -> testFunction(4), 3, 100, mockClock));
    verify(mockClock, times(3)).sleep(anyLong());
  }

  private boolean testFunction(int timesToThrow) throws IOException {
    if (timesThrown < timesToThrow) {
      timesThrown++;
      throw new IOException("oh no i iz borke, plz retry");
    }

    return true;
  }

  protected void assertComputeRetryInRange(int retryAttempts, long retryBackoffMs) {
    for (int i = 0; i != 20; ++i) {
      for (int retries = 0; retries <= retryAttempts; ++retries) {
        long maxResult = RetryUtil.computeRetryWaitTimeInMillis(retries, retryBackoffMs);
        long result = RetryUtil.computeRandomRetryWaitTimeInMillis(retries, retryBackoffMs);
        if (retryBackoffMs < 0) {
          assertEquals(0, result);
        } else {
          assertTrue(result >= 0L);
          assertTrue(result <= maxResult);
        }
      }
    }
  }
}