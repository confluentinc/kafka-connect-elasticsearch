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

package io.confluent.connect.elasticsearch.internals;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.searchbox.client.JestResult;
import io.searchbox.core.BulkResult;
import io.searchbox.core.BulkResult.BulkResultItem;

public class Response {

  private Throwable throwable;
  private JestResult result;
  private String nonRetriableError = "mapper_parse_exception";

  public Response(BulkResult result) {
    this.result = result;
    Throwable firstException = null;
    for (BulkResultItem bulkResultItem: result.getFailedItems()) {
      JsonObject obj = parseError(bulkResultItem.error);
      if (obj.get("type").getAsString().equals(nonRetriableError)) {
        throwable = new Throwable(obj.get("type").getAsString());
        break;
      } else {
        if (firstException == null) {
          firstException = new Throwable(bulkResultItem.error);
        }
      }
    }
    if (throwable == null) {
      throwable = firstException;
    }
  }

  public boolean canRetry() {
    return throwable == null || !throwable.getMessage().equals(nonRetriableError);
  }

  public JestResult getResult() {
    return result;
  }

  public Throwable getThrowable() {
    return throwable;
  }

  public boolean hasFailures() {
    return !result.isSucceeded();
  }

  private JsonObject parseError(String error) {
    if (error != null && !error.trim().isEmpty()) {
      return new JsonParser().parse(error).getAsJsonObject();
    }
    return new JsonObject();
  }
}
