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

package io.confluent.connect.elasticsearch_2_4.jest;

import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.http.client.config.RequestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyJestClientFactory extends JestClientFactory {

  private static final Logger log = LoggerFactory.getLogger(MyJestClientFactory.class);

  @Override
  public void setHttpClientConfig(HttpClientConfig httpClientConfig) {
    super.setHttpClientConfig(httpClientConfig);
  }

  @Override
  protected RequestConfig getRequestConfig() {
    RequestConfig config = super.getRequestConfig();
    return RequestConfig
            .copy(config)
            .setExpectContinueEnabled(true)
            .build();
  }
}
