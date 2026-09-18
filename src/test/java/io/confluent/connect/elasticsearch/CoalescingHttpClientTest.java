/*
 * Copyright 2020 Confluent Inc.
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import co.elastic.clients.transport.http.TransportHttpClient.Request;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class CoalescingHttpClientTest {

  // Mirrors the transport, which appends one static ByteBuffer as every NDJSON separator.
  private static final ByteBuffer SEPARATOR = ByteBuffer.wrap("\n".getBytes(UTF_8));

  // Reading the shared separator without duplicate() would consume it, dropping every later
  // newline in this body and in every request that follows.
  @Test
  public void testCoalesceMergesLinesWithoutConsumingTheSharedSeparator() {
    Request first = ndjson("{\"a\":1}", "{\"b\":2}");
    Request second = ndjson("{\"c\":3}");

    assertEquals("{\"a\":1}\n{\"b\":2}\n", body(CoalescingHttpClient.coalesce(first)));
    assertEquals("{\"c\":3}\n", body(CoalescingHttpClient.coalesce(second)));
    assertEquals(1, SEPARATOR.remaining());
  }

  @Test
  public void testCoalescePassesThroughBodiesThatNeedNoMerging() {
    Request single = request(Collections.singletonList(ByteBuffer.wrap("x".getBytes(UTF_8))));
    Request bodiless = request(null);

    assertSame(single, CoalescingHttpClient.coalesce(single));
    assertSame(bodiless, CoalescingHttpClient.coalesce(bodiless));
  }

  private static Request ndjson(String... lines) {
    List<ByteBuffer> body = new ArrayList<>();
    for (String line : lines) {
      body.add(ByteBuffer.wrap(line.getBytes(UTF_8)));
      body.add(SEPARATOR);
    }
    return request(body);
  }

  private static Request request(Iterable<ByteBuffer> body) {
    return new Request("POST", "/_bulk", Collections.emptyMap(), Collections.emptyMap(), body);
  }

  private static String body(Request request) {
    List<ByteBuffer> buffers = new ArrayList<>();
    request.body().forEach(buffers::add);
    assertEquals(1, buffers.size());
    byte[] bytes = new byte[buffers.get(0).remaining()];
    buffers.get(0).duplicate().get(bytes);
    return new String(bytes, UTF_8);
  }
}
