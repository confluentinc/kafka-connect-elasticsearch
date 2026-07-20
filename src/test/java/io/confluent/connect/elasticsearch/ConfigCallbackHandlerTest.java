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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.http.HttpHost;
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
}
