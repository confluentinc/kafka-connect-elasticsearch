/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.elasticsearch;

import com.google.common.collect.Sets;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;

import java.util.List;
import java.util.Set;

public class ElasticsearchAuth {
  private Set<HttpHost> targetHosts;
  private HttpClientConfig.Builder clientConfig;
  private String username;
  private String password;

  public ElasticsearchAuth(List<String> address,
                           HttpClientConfig.Builder clientConfig,
                           String username,
                           String password) {
    this.clientConfig = clientConfig;
    this.username = username;
    this.password = password;
    this.targetHosts = Sets.newHashSet();
    for (String s : address) {
      targetHosts.add(HttpHost.create(s));
    }
  }

  public void invoke() {

    BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    for (HttpHost h : targetHosts) {
      AuthScope as =  new AuthScope(h.getHostName(), h.getPort());
      UsernamePasswordCredentials cr = new UsernamePasswordCredentials(username, password);
      credentialsProvider.setCredentials(as,cr);
    }
    clientConfig.credentialsProvider(credentialsProvider);

    clientConfig.preemptiveAuthTargetHosts(targetHosts);
  }
}