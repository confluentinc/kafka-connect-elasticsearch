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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.index.mapper.MapperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESResponse {

  private static final Logger log = LoggerFactory.getLogger(ESResponse.class);
  // TODO: Add more non retriable exceptions. The other major one is version conflict.
  private static final Class<? extends Throwable> NON_RETRIABLE_EXCEPTION_CLASS = MapperException.class;
  // To save the first non reriable exception. If all exceptions are retriable, then it saves the first exception.
  private Throwable throwable;
  private BulkResponse bulkResponse;

  public ESResponse(BulkResponse bulkResponse) {
    this.bulkResponse = bulkResponse;
    Throwable firstException = null;
    for (BulkItemResponse bulkItemResponse : bulkResponse) {
      if (bulkItemResponse.isFailed()) {
        Throwable cause = bulkItemResponse.getFailure().getCause();
        Throwable rootCause = ExceptionsHelper.unwrapCause(cause);
        log.error("Exception when executing the batch:", rootCause);
        if (firstException == null) {
          firstException = rootCause;
        }
        if (NON_RETRIABLE_EXCEPTION_CLASS.isAssignableFrom(rootCause.getClass())) {
          throwable = rootCause;
          break;
        }
      }
    }
    if (throwable == null) {
      throwable = firstException;
    }
  }

  public boolean canRetry() {
    return !NON_RETRIABLE_EXCEPTION_CLASS.isAssignableFrom(throwable.getClass());
  }

  public Throwable getThrowable() {
    return throwable;
  }

  public boolean hasFailures() {
    return bulkResponse.hasFailures();
  }
}
