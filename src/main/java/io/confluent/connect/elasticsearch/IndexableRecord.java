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

import java.util.Objects;

public class IndexableRecord {

  public final Key key;
  public final String payload;
  public final Long version;

  public IndexableRecord(Key key, String payload, Long version) {
    this.key = key;
    this.version = version;
    this.payload = payload;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IndexableRecord that = (IndexableRecord) o;
    return Objects.equals(key, that.key)
        && Objects.equals(payload, that.payload)
        && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, version, payload);
  }

  @Override
  public String toString() {
    return String.format("%s/payload{%s}/version{%s}", key, payload, version);
  }
}
