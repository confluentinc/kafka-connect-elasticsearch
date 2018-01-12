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

import io.searchbox.action.BulkableAction;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;

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

  public BulkableAction toBulkableAction() {
    // If payload is null, the record was a tombstone and we should delete from the index.
    return payload != null ? toIndexRequest() : toDeleteRequest();
  }

  public Delete toDeleteRequest() {
    Delete.Builder req = new Delete.Builder(key.id)
        .index(key.index)
        .type(key.type);

    // TODO: Find out if version information should be set here. Currently just following the lead
    //       of an old PR found here:
    //       https://github.com/confluentinc/kafka-connect-elasticsearch/pull/92/files#diff-4d562453cf74ef634cc77124945ff8a2R55

    return req.build();
  }

  public Index toIndexRequest() {
    Index.Builder req = new Index.Builder(payload)
        .index(key.index)
        .type(key.type)
        .id(key.id);
    if (version != null) {
      req.setParameter("version_type", "external").setParameter("version", version);
    }
    return req.build();
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
}
