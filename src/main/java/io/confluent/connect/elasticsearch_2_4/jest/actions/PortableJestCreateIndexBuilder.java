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

package io.confluent.connect.elasticsearch_2_4.jest.actions;

import io.confluent.connect.elasticsearch_2_4.ElasticsearchClient.Version;
import io.searchbox.indices.CreateIndex;

/**
 * Portable Jest action builder, across ES versions, to create indexes.
 * This builder add support for ES version 7 by keeping the type support still enabled, this is
 * done by passing the include_type_name parameter. This parameter is no longer required with ES 8,
 * as types should not be used anymore by the time of ES 8 release.
 */
public class PortableJestCreateIndexBuilder extends CreateIndex.Builder {

  public static final String INCLUDE_TYPE_NAME_PARAM = "include_type_name";
  public static final String TIMEOUT_PARAM = "timeout";

  private final Version version;
  private final long timeout;

  public PortableJestCreateIndexBuilder(String index, Version version, long timeout) {
    super(index);
    this.version = version;
    this.timeout = timeout;
  }

  @Override
  public CreateIndex build() {
    if (version.equals(Version.ES_V7)) {
      setParameter(INCLUDE_TYPE_NAME_PARAM, true);
    }
    if (timeout > 0) {
      setParameter(TIMEOUT_PARAM, timeout + "ms");
    }

    return super.build();
  }
}
