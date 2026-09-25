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

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_API_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.junit.Test;

public class ConfigCallbackHandlerTest {

  @Test
  public void redactUserInfoStripsCredentials() {
    assertEquals(
        "https://host:9243",
        ConfigCallbackHandler.redactUserInfo("https://user:password@host:9243")
    );
  }

  @Test
  public void redactUserInfoStripsCredentialsContainingAtSign() {
    // A password containing '@' must not leave any trailing credential fragment behind:
    // the separator between user-info and host is the LAST '@' in the authority, not the first.
    assertEquals(
        "https://host:9243",
        ConfigCallbackHandler.redactUserInfo("https://user:p@ssw0rd@host:9243")
    );
  }

  @Test
  public void redactUserInfoStripsCredentialsWithMultipleAtSigns() {
    assertEquals(
        "https://host:9243",
        ConfigCallbackHandler.redactUserInfo("https://user:p@ss@w@rd@host:9243")
    );
  }

  @Test
  public void redactUserInfoLeavesUrlWithoutCredentialsUnchanged() {
    assertEquals(
        "https://host:9243",
        ConfigCallbackHandler.redactUserInfo("https://host:9243")
    );
  }

  @Test
  public void redactUserInfoDoesNotStripAtSignInPath() {
    // An '@' appearing after the authority component (e.g. in the path) is not a credential
    // separator and must be left alone.
    assertEquals(
        "https://host:9243/index/user@example.com",
        ConfigCallbackHandler.redactUserInfo("https://host:9243/index/user@example.com")
    );
  }

  @Test
  public void redactUserInfoHandlesUrlWithoutScheme() {
    assertEquals(
        "host:9243",
        ConfigCallbackHandler.redactUserInfo("user:password@host:9243")
    );
  }

  @Test
  public void createRedactedHttpHostParsesValidUrl() {
    HttpHost host = ConfigCallbackHandler.createRedactedHttpHost("https://host:9243");
    assertEquals("host", host.getHostName());
    assertEquals(9243, host.getPort());
  }

  @Test
  public void createRedactedHttpHostRedactsCredentialOnParseFailure() {
    // A space is illegal in a URI authority and forces HttpHost.create() to throw; the raw
    // credential must not survive into the resulting exception's message.
    IllegalArgumentException e = assertThrows(
        IllegalArgumentException.class,
        () -> ConfigCallbackHandler.createRedactedHttpHost(
            "https://user:p@ssw0rd@host name:9243")
    );
    assertFalse(e.getMessage().contains("p@ssw0rd"));
    assertTrue(e.getMessage().contains("host name:9243"));
  }

  @Test
  public void createRedactedHttpHostRedactsCredentialWhenUnderlyingExceptionEchoesIt() {
    IllegalArgumentException e = assertThrows(
        IllegalArgumentException.class,
        () -> ConfigCallbackHandler.createRedactedHttpHost(
            "https://user:p@ssw0rd@host:9243/")
    );
    assertFalse(e.getMessage().contains("p@ssw0rd"));
    assertTrue(e.getMessage().contains("host:9243"));
  }

  @Test
  public void redactUserInfoStripsCredentialsFromSchemeRelativeUrl() {
    assertEquals(
        "//host:9243",
        ConfigCallbackHandler.redactUserInfo("//user:password@host:9243")
    );
  }

  @Test
  public void redactUserInfoStripsCredentialsFromMalformedSingleSlashScheme() {
    assertEquals(
        "host:9243",
        ConfigCallbackHandler.redactUserInfo("https:/user:password@host:9243")
    );
  }

  @Test
  public void encodedApiKeyIsSentAsApiKeyAuthorizationHeader() throws Exception {
    String encoded = base64("id1:secret1");
    assertEquals(
        Collections.singletonList("ApiKey " + encoded),
        authorizationHeadersSent(Collections.singletonMap(CONNECTION_API_KEY_CONFIG, encoded)));
  }

  @Test
  public void rawIdAndSecretApiKeyIsBase64EncodedIntoHeader() throws Exception {
    assertEquals(
        Collections.singletonList("ApiKey " + base64("id1:secret1")),
        authorizationHeadersSent(
            Collections.singletonMap(CONNECTION_API_KEY_CONFIG, "  id1:secret1  ")));
  }

  @Test
  public void noAuthorizationHeaderWithoutApiKey() throws Exception {
    assertEquals(Collections.emptyList(), authorizationHeadersSent(Collections.emptyMap()));
  }

  // Sends one request through a RestClient built exactly as the connector builds it, against a
  // local server, and returns the Authorization header values the server received.
  private static List<String> authorizationHeadersSent(Map<String, String> extraProps)
      throws Exception {
    List<String> received = new CopyOnWriteArrayList<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", exchange -> {
      List<String> auth = exchange.getRequestHeaders().get(HttpHeaders.AUTHORIZATION);
      if (auth != null) {
        received.addAll(auth);
      }
      byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    server.start();
    try {
      String url = "http://127.0.0.1:" + server.getAddress().getPort();
      Map<String, String> props = new HashMap<>(extraProps);
      props.put(CONNECTION_URL_CONFIG, url);
      ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
      try (RestClient client = RestClient.builder(HttpHost.create(url))
          .setHttpClientConfigCallback(new ConfigCallbackHandler(config))
          .build()) {
        client.performRequest(new Request("GET", "/"));
      }
      return received;
    } finally {
      server.stop(0);
    }
  }

  private static String base64(String s) {
    return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
  }
}
