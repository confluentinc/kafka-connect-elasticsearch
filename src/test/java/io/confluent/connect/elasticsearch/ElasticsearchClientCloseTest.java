/*
 * Copyright 2024 Confluent Inc.
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

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class ElasticsearchClientCloseTest {

  @Test(timeout = 30_000)
  public void awaitTerminationWithinSharesOneDeadlineAcrossPools() throws Exception {
    // Three pools that will not drain within the budget: each holds a task sleeping well
    // past it. With one shared deadline the caller waits ~budget once; awaiting each pool
    // for the full budget in turn would cost ~budget * poolCount.
    List<ExecutorService> pools = Arrays.asList(
        Executors.newSingleThreadExecutor(),
        Executors.newSingleThreadExecutor(),
        Executors.newSingleThreadExecutor());
    for (ExecutorService pool : pools) {
      pool.submit(() -> {
        try {
          Thread.sleep(30_000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      pool.shutdown();
    }

    long budgetMs = 2_000;
    long start = System.nanoTime();
    ElasticsearchClient.awaitTerminationWithin(pools, budgetMs);
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    // Shared deadline: ~budget total. Sequential-per-pool would be ~3 * budget.
    assertTrue("awaitTerminationWithin took " + elapsedMs + "ms; expected ~" + budgetMs + "ms",
        elapsedMs < budgetMs * 2);
    for (ExecutorService pool : pools) {
      assertTrue("pool was not forced down", pool.isShutdown());
      assertTrue("pool tasks did not terminate", pool.awaitTermination(5, TimeUnit.SECONDS));
    }
  }
}
