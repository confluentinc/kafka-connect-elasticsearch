package io.confluent.connect.elasticsearch.integration;

import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseTransformer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.Response;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Transformer that blocks all incoming requests until {@link #release(int)} is called
 * to fairly unblock a given number of requests.
 */
public class BlockingTransformer extends ResponseTransformer {

  // Static reference to track the current instance (WireMock creates via reflection)
  private static final AtomicReference<BlockingTransformer> CURRENT_INSTANCE = new AtomicReference<>();

  private final Semaphore s = new Semaphore(0, true);
  private final AtomicInteger requestCount = new AtomicInteger();

  public static final String NAME = "blockingTransformer";

  public BlockingTransformer() {
    // Register this instance when WireMock creates it via reflection
    CURRENT_INSTANCE.set(this);
  }

  @Override
  public Response transform(Request request, Response response, FileSource files, Parameters parameters) {
    try {
      s.acquire();
    } catch (InterruptedException e) {
      throw new ConnectException(e);
    } finally {
      s.release();
    }
    requestCount.incrementAndGet();
    return response;
  }

  @Override
  public String getName() {
    return NAME;
  }

  public void release(int permits) {
    s.release(permits);
  }

  /**
   * How many requests are currently blocked
   */
  public int queueLength() {
    return s.getQueueLength();
  }

  /**
   * How many requests have been processed
   */
  public int requestCount() {
    return requestCount.get();
  }

  @Override
  public boolean applyGlobally() {
    return false;
  }

  /**
   * Get the BlockingTransformer instance created by WireMock.
   * WireMock creates the instance via reflection when the rule is initialized.
   */
  public static BlockingTransformer getInstance(WireMockRule wireMockRule) {
    BlockingTransformer instance = CURRENT_INSTANCE.get();
    if (instance == null) {
      throw new IllegalStateException(
              "No BlockingTransformer instance found. Ensure WireMockRule is initialized.");
    }
    return instance;
  }

}
