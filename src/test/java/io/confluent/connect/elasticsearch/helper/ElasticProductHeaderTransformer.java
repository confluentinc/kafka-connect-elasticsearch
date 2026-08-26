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

package io.confluent.connect.elasticsearch.helper;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformer;
import com.github.tomakehurst.wiremock.http.HttpHeaders;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;

/**
 * Stamps the {@code X-Elastic-Product: Elasticsearch} header on every WireMock response
 * that does not already carry one.
 *
 * <p>The Elasticsearch client rejects responses without this header, surfacing them as
 * transport failures. A stub written with a plain {@code aResponse()}/{@code okJson()}
 * would therefore silently shift its test onto the transport-failure path instead of the
 * scenario it names. Registering this transformer globally makes the header a property of
 * the mock server rather than a convention each stub must remember.
 */
public class ElasticProductHeaderTransformer extends ResponseDefinitionTransformer {

  public static final String NAME = "elastic-product-header";
  private static final String PRODUCT_HEADER = "X-Elastic-Product";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean applyGlobally() {
    return true;
  }

  @Override
  public ResponseDefinition transform(Request request, ResponseDefinition responseDefinition,
                                      FileSource files, Parameters parameters) {
    HttpHeaders headers = responseDefinition.getHeaders();
    if (headers != null && headers.getHeader(PRODUCT_HEADER).isPresent()) {
      return responseDefinition;
    }
    return ResponseDefinitionBuilder.like(responseDefinition).but()
        .withHeader(PRODUCT_HEADER, "Elasticsearch")
        .build();
  }
}
