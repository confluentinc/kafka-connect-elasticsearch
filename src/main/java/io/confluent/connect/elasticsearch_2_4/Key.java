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

package io.confluent.connect.elasticsearch_2_4;

import java.util.Objects;

public class Key {

  public final String index;
  public final String type;
  public final String id;

  public Key(String index, String type, String id) {
    this.index = index;
    this.type = type;
    this.id = id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Key that = (Key) o;
    return Objects.equals(index, that.index)
           && Objects.equals(type, that.type)
           && Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, type, id);
  }

  @Override
  public String toString() {
    return String.format("Key{%s/%s/%s}", index, type, id);
  }

}
