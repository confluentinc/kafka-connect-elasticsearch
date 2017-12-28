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
package io.confluent.connect.elasticsearch.bulk;

public class BulkResponse {

  private static final BulkResponse SUCCESS_RESPONSE = new BulkResponse(true, false, "");

  public boolean succeeded;
  public  boolean retriable;
  public  String errorInfo;
  public long lastWrittenAt;
  public String uuid;
  private BulkResponse(boolean succeeded, boolean retriable, String errorInfo) {
    this.succeeded = succeeded;
    this.retriable = retriable;
    this.errorInfo = errorInfo;
  }
  public BulkResponse(boolean succeeded, String uuid, long time) {
    this.succeeded = succeeded;
    this.uuid = uuid;
    this.lastWrittenAt = time;
  }
  public static BulkResponse success() {
    return SUCCESS_RESPONSE;
  }

  public static BulkResponse failure(boolean retriable, String errorInfo) {
    return new BulkResponse(false, retriable, errorInfo);
  }

  public boolean isSucceeded() {
    return succeeded;
  }

  public boolean isRetriable() {
    return retriable;
  }

  public String getErrorInfo() {
    return errorInfo;
  }

  public long getLastWrittenAt() {
    return lastWrittenAt;
  }

  public void setLastWrittenAt(long lastWrittenAt) {
    this.lastWrittenAt = lastWrittenAt;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

}
