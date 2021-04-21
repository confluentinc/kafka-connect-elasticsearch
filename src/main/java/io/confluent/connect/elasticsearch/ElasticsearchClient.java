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

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnMalformedDoc;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.http.HttpHost;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchClient {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchClient.class);

  private static final long WAIT_TIME = TimeUnit.MILLISECONDS.toMillis(10);
  private static final String RESOURCE_ALREADY_EXISTS_EXCEPTION =
      "resource_already_exists_exception";
  private static final String VERSION_CONFLICT_EXCEPTION = "version_conflict_engine_exception";
  private static final Set<String> MALFORMED_DOC_ERRORS = new HashSet<>(
      Arrays.asList(
          "mapper_parsing_exception",
          "illegal_argument_exception",
          "action_request_validation_exception"
      )
  );

  protected final AtomicInteger numRecords;
  private final AtomicReference<ConnectException> error;
  protected final BulkProcessor bulkProcessor;
  private final ConcurrentMap<DocWriteRequest<?>, SinkRecord> requestToRecord;
  private final ConcurrentMap<Long, List<SinkRecord>> inFlightRequests;
  private final ElasticsearchSinkConnectorConfig config;
  private final ErrantRecordReporter reporter;
  private final RestHighLevelClient client;
  private final ScheduledExecutorService executorService;
  private final Time clock;

  public ElasticsearchClient(
      ElasticsearchSinkConnectorConfig config,
      ErrantRecordReporter reporter
  ) {
    ConfigCallbackHandler configCallbackHandler = new ConfigCallbackHandler(config);
    NHttpClientConnectionManager cm = configCallbackHandler.connectionManager();
    /*
     * Handles closing any idle or expired connections to avoid SocketTimeoutExceptions. Expired
     * connections occur when the server closes their half of the connection without notifying
     * the client.
     */
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.scheduleAtFixedRate(
        () -> {
          cm.closeExpiredConnections();
          cm.closeIdleConnections(config.maxIdleTimeMs(), TimeUnit.MILLISECONDS);
        },
        config.maxIdleTimeMs(),
        config.maxIdleTimeMs() / 2,
        TimeUnit.MILLISECONDS
    );

    this.numRecords = new AtomicInteger(0);
    this.error = new AtomicReference<>();
    this.requestToRecord = reporter != null ? new ConcurrentHashMap<>() : null;
    this.inFlightRequests = reporter != null ? new ConcurrentHashMap<>() : null;
    this.config = config;
    this.reporter = reporter;
    this.clock = Time.SYSTEM;
    this.client = new RestHighLevelClient(
        RestClient
            .builder(
                config.connectionUrls()
                    .stream()
                    .map(HttpHost::create)
                    .collect(Collectors.toList())
                    .toArray(new HttpHost[config.connectionUrls().size()])
            )
            .setHttpClientConfigCallback(configCallbackHandler)
            .setRequestConfigCallback(configCallbackHandler)
    );
    this.bulkProcessor = BulkProcessor
        .builder((req, lis) -> client.bulkAsync(req, RequestOptions.DEFAULT, lis), buildListener())
        .setBulkActions(config.batchSize())
        .setConcurrentRequests(config.maxInFlightRequests() - 1) // 0 = no concurrent requests
        .setFlushInterval(TimeValue.timeValueMillis(config.lingerMs()))
        .setBackoffPolicy(
            BackoffPolicy.exponentialBackoff(
                TimeValue.timeValueMillis(config.retryBackoffMs()),
                config.maxRetries()
            )
        )
        .build();
  }

  /**
   * Returns the underlying Elasticsearch client.
   *
   * @return the underlying RestHighLevelClient
   */
  public RestHighLevelClient client() {
    return client;
  }

  /**
   * Closes the ElasticsearchClient.
   *
   * @throws ConnectException if all of the records fail to flush before the timeout.
   */
  public void close() {
    try {
      if (!bulkProcessor.awaitClose(config.flushTimeoutMs(), TimeUnit.MILLISECONDS)) {
        throw new ConnectException(
            "Failed to process outstanding requests in time while closing the ElasticsearchClient."
        );
      }
    } catch (InterruptedException e) {
      throw new ConnectException(
          "Interrupted while processing all in-flight requests on ElasticsearchClient close."
      );
    } finally {
      closeConnections();
    }
  }

  /**
   * Creates an index. Will not recreate the index if it already exists.
   *
   * @param index the index to create
   * @return true if the index was created, false if it already exists
   */
  public boolean createIndex(String index) {
    if (indexExists(index)) {
      return false;
    }

    CreateIndexRequest request = new CreateIndexRequest(index);
    return callWithRetries(
        "create index " + index,
        () -> {
          try {
            client.indices().create(request, RequestOptions.DEFAULT);
          } catch (ElasticsearchStatusException | IOException e) {
            if (!e.getMessage().contains(RESOURCE_ALREADY_EXISTS_EXCEPTION)) {
              throw e;
            }
            return false;
          }
          return true;
        }
    );
  }

  /**
   * Creates a mapping for the given index and schema.
   *
   * @param index the index to create the mapping for
   * @param schema the schema to map
   */
  public void createMapping(String index, Schema schema) {
    PutMappingRequest request = new PutMappingRequest(index).source(Mapping.buildMapping(schema));
    callWithRetries(
        String.format("create mapping for index %s with schema %s", index, schema),
        () -> client.indices().putMapping(request, RequestOptions.DEFAULT)
    );
  }

  /**
   * Flushes any buffered records.
   */
  public void flush() {
    bulkProcessor.flush();
  }

  /**
   * Checks whether the index already has a mapping or not.
   * @param index the index to check
   * @return true if a mapping exists, false if it does not
   */
  public boolean hasMapping(String index) {
    MappingMetaData mapping = mapping(index);
    return mapping != null && mapping.sourceAsMap() != null && !mapping.sourceAsMap().isEmpty();
  }

  /**
   * Buffers a record to index. Will ensure that there are no concurrent requests for the same
   * document id when either the DLQ is configured or
   * {@link ElasticsearchSinkConnectorConfig#IGNORE_KEY_CONFIG} is set to <code>false</code> because
   * they require the use of a map keyed by document id.
   *
   * @param record the record to index
   * @param request the associated request to send
   * @throws ConnectException if one of the requests failed
   */
  public void index(SinkRecord record, DocWriteRequest<?> request) {
    if (isFailed()) {
      try {
        close();
      } catch (ConnectException e) {
        // if close fails, want to still throw the original exception
      }
      throw error.get();
    }

    // wait for internal buffer to be less than max.buffered.records configuration
    long maxWaitTime = clock.milliseconds() + config.flushTimeoutMs();
    while (numRecords.get() >= config.maxBufferedRecords()) {
      clock.sleep(WAIT_TIME);
      if (clock.milliseconds() > maxWaitTime) {
        throw new ConnectException(
            String.format(
                "Could not make space in the internal buffer fast enough. Consider increasing %s"
                    + " or %s.",
                FLUSH_TIMEOUT_MS_CONFIG,
                MAX_BUFFERED_RECORDS_CONFIG
            )
        );
      }
    }

    addToRequestToRecordMap(request, record);
    numRecords.incrementAndGet();
    bulkProcessor.add(request);
  }

  /**
   * Checks whether the index exists.
   *
   * @param index the index to check
   * @return true if it exists, false if it does not
   */
  public boolean indexExists(String index) {
    GetIndexRequest request = new GetIndexRequest(index);
    return callWithRetries(
        "check if index " + index + " exists",
        () -> client.indices().exists(request, RequestOptions.DEFAULT)
    );
  }

  /**
   * Maps a record to the write request.
   *
   * @param request the write request
   * @param record  the record
   */
  private void addToRequestToRecordMap(DocWriteRequest<?> request, SinkRecord record) {
    if (requestToRecord != null) {
      requestToRecord.put(request, record);
    }
  }

  /**
   * Creates a listener with callback functions to handle completed requests for the BulkProcessor.
   *
   * @return the listener
   */
  private BulkProcessor.Listener buildListener() {
    return new Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {
        if (requestToRecord != null && inFlightRequests != null) {
          List<SinkRecord> sinkRecords = new ArrayList<>(request.requests().size());
          for (DocWriteRequest<?> req : request.requests()) {
            sinkRecords.add(requestToRecord.get(req));
            requestToRecord.remove(req);
          }

          inFlightRequests.put(executionId, sinkRecords);
        }
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        for (BulkItemResponse bulkItemResponse : response) {
          handleResponse(bulkItemResponse, executionId);
        }

        removeFromInFlightRequests(executionId);
        numRecords.addAndGet(-response.getItems().length);
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        log.warn("Bulk request {} failed. Retrying request.", executionId, failure);
        try {
          // manually retry the bulk request until it succeeds or retries are exhausted using the
          // initial request thread
          BulkResponse bulkResponse = callWithRetries(
              "retrying bulk request",
              () -> client.bulk(request, RequestOptions.DEFAULT)
          );
          afterBulk(executionId, request, bulkResponse);
        } catch (ConnectException e) {
          removeFromInFlightRequests(executionId);
          error.compareAndSet(null, new ConnectException("Bulk request failed.", e.getCause()));
          numRecords.addAndGet(-request.requests().size());
        }
      }
    };
  }

  /**
   * Calls the specified function with retries and backoffs until the retries are exhausted or the
   * function succeeds.
   *
   * @param description description of the attempted action in present tense
   * @param function the function to call and retry
   * @param <T> the return type of the function
   * @return the return value of the called function
   */
  private <T> T callWithRetries(String description, Callable<T> function) {
    try {
      return RetryUtil.callWithRetries(
          description,
          function,
          config.maxRetries(),
          config.retryBackoffMs()
      );
    } catch (Exception e) {
      throw new ConnectException("Failed to " + description + ".", e);
    }
  }

  /**
   * Closes all of the connection and thread resources of the client.
   */
  private void closeConnections() {
    executorService.shutdown();

    try {
      client.close();
    } catch (IOException e) {
      log.warn("Failed to close Elasticsearch client.", e);
    }
  }

  /**
   * Processes a response from a {@link org.elasticsearch.action.bulk.BulkItemRequest}.
   * Successful responses are ignored. Failed responses are reported to the DLQ and handled
   * according to configuration (ignore or fail). Version conflicts are ignored.
   *
   * @param response    the response to process
   * @param executionId the execution id of the request
   */
  private void handleResponse(BulkItemResponse response, long executionId) {
    if (response.isFailed()) {
      for (String error : MALFORMED_DOC_ERRORS) {
        if (response.getFailureMessage().contains(error)) {
          handleMalformedDocResponse(response);
          reportBadRecord(response, executionId);
          return;
        }
      }

      if (response.getFailureMessage().contains(VERSION_CONFLICT_EXCEPTION)) {
        log.warn(
            "Ignoring version conflict for operation {} on document '{}' version {} in index '{}'.",
            response.getOpType(),
            response.getId(),
            response.getVersion(),
            response.getIndex()
        );

        reportBadRecord(response, executionId);
        return;
      }

      error.compareAndSet(
          null,
          new ConnectException("Indexing record failed.", response.getFailure().getCause())
      );
    }
  }

  /**
   * Handle a failed response as a result of a malformed document. Depending on the configuration,
   * ignore or fail.
   *
   * @param response the failed response from ES
   */
  private void handleMalformedDocResponse(BulkItemResponse response) {
    String errorMsg = String.format(
        "Encountered an illegal document error '%s'. Ignoring and will not index record.",
        response.getFailureMessage()
    );
    switch (config.behaviorOnMalformedDoc()) {
      case IGNORE:
        log.debug(errorMsg);
        return;
      case WARN:
        log.warn(errorMsg);
        return;
      case FAIL:
      default:
        log.error(
            "Encountered an illegal document error '{}'. To ignore future records like this,"
                + " change the configuration '{}' to '{}'.",
            response.getFailureMessage(),
            ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG,
            BehaviorOnMalformedDoc.IGNORE
        );
        error.compareAndSet(
            null,
            new ConnectException("Indexing record failed.", response.getFailure().getCause())
        );
    }
  }

  /**
   * Whether there is a failed response.
   *
   * @return true if a response has failed, false if none have failed
   */
  private boolean isFailed() {
    return error.get() != null;
  }

  /**
   * Gets the mapping for an index.
   *
   * @param index the index to fetch the mapping for
   * @return the MappingMetaData for the index
   */
  private MappingMetaData mapping(String index) {
    GetMappingsRequest request = new GetMappingsRequest().indices(index);
    GetMappingsResponse response = callWithRetries(
        "get mapping for index " + index,
        () -> client.indices().getMapping(request, RequestOptions.DEFAULT)
    );
    return response.mappings().get(index);
  }

  /**
   * Removes the mapping for bulk request id to records being written.
   *
   * @param executionDd the execution id of the bulk request
   */
  private void removeFromInFlightRequests(long executionDd) {
    if (inFlightRequests != null) {
      inFlightRequests.remove(executionDd);
    }
  }

  /**
   * Reports a bad record to the DLQ.
   *
   * @param response    the failed response from ES
   * @param executionId the execution id of the request associated with the response
   */
  private synchronized void reportBadRecord(BulkItemResponse response, long executionId) {
    if (reporter != null) {
      List<SinkRecord> sinkRecords = inFlightRequests.getOrDefault(executionId, new ArrayList<>());
      SinkRecord original = sinkRecords.size() > response.getItemId()
          ? sinkRecords.get(response.getItemId())
          : null;
      if (original != null) {
        reporter.report(
            original,
            new ReportingException("Indexing failed: " + response.getFailureMessage())
        );
      }
    }
  }

  /**
   * Exception that swallows the stack trace used for reporting errors from Elasticsearch
   * (mapper_parser_exception, illegal_argument_exception, and action_request_validation_exception)
   * resulting from bad records using the AK 2.6 reporter DLQ interface.
   */
  @SuppressWarnings("serial")
  public static class ReportingException extends RuntimeException {

    public ReportingException(String message) {
      super(message);
    }

    /**
     * This method is overridden to swallow the stack trace.
     *
     * @return Throwable
     */
    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }
}
