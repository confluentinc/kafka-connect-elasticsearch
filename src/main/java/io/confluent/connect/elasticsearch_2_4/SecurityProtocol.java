/*
 * Copyright 2019 Confluent Inc.
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

public enum SecurityProtocol {

  /**
   * Un-authenticated, non-encrypted channel
   */
  PLAINTEXT(0, "PLAINTEXT"),
  /**
   * SSL channel
   */
  SSL(1, "SSL");

  private final int id;
  private final String name;

  SecurityProtocol(int id, String name) {
    this.id = id;
    this.name = name;
  }
}
