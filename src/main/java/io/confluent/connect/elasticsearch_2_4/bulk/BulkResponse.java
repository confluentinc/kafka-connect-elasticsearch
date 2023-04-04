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

package io.confluent.connect.elasticsearch_2_4.bulk;

import io.confluent.connect.elasticsearch_2_4.IndexableRecord;
import io.searchbox.core.BulkResult.BulkResultItem;
import java.util.Collections;
import java.util.Map;

public class BulkResponse {

  private static final BulkResponse SUCCESS_RESPONSE =
      new BulkResponse(true, false, "", Collections.emptyMap());

  public final boolean succeeded;
  public final boolean retriable;
  public final String errorInfo;
  public final Map<IndexableRecord, BulkResultItem> failedRecords;

  /**
   * Creates a BulkResponse.
   * @param succeeded whether the bulk request was successful or not.
   * @param retriable whether the bulk request should be retried.
   * @param errorInfo the error string
   * @param failedRecords map of failed records and their results. Never null.
   */
  private BulkResponse(
      boolean succeeded,
      boolean retriable,
      String errorInfo,
      Map<IndexableRecord, BulkResultItem> failedRecords
  ) {
    this.succeeded = succeeded;
    this.retriable = retriable;
    this.errorInfo = errorInfo;
    this.failedRecords = failedRecords;
  }

  public static BulkResponse success() {
    return SUCCESS_RESPONSE;
  }

  /**
   * Creates a failed BulkResponse.
   * @param retriable whether the error is retriable
   * @param errorInfo the error string
   * @param failedRecords map of failed records and their results. Never null.
   * @return
   */
  public static BulkResponse failure(
      boolean retriable,
      String errorInfo,
      Map<IndexableRecord, BulkResultItem> failedRecords
  ) {
    return new BulkResponse(false, retriable, errorInfo, failedRecords);
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

  @Override
  public String toString() {
    return "BulkResponse{"
            + "succeeded=" + succeeded
            + ", retriable=" + retriable
            + ", errorInfo='" + (errorInfo == null ? "" : errorInfo) + '\''
            + '}';
  }
}
