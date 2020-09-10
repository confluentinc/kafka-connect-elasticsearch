/*
 * Copyright 2020 Confluent Inc.
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

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnMalformedDoc;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.ssl.SslFactory;
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

  private AtomicReference<ConnectException> error;
  private BulkProcessor bulkProcessor;
  private ConcurrentHashMap<String, SinkRecord> recordMap;
  private ElasticsearchSinkConnectorConfig config;
  private ErrantRecordReporter reporter;
  private RestHighLevelClient client;

  public ElasticsearchClient(
      ElasticsearchSinkConnectorConfig config,
      ErrantRecordReporter reporter
  ) {
    int pos = 0;
    HttpHost[] hosts = new HttpHost[config.connectionUrls().size()];
    for (String address : config.connectionUrls()) {
      try {
        URL url = new URL(address);
        hosts[pos++] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
      } catch (MalformedURLException e) {
        throw new ConnectException(e);
      }
    }

    this.error = new AtomicReference<>();
    this.recordMap = reporter != null || !config.ignoreKey()
        ? new ConcurrentHashMap<>()
        : null;
    this.config = config;
    this.reporter = reporter;
    this.client = new RestHighLevelClient(
        RestClient
            .builder(hosts)
            .setHttpClientConfigCallback(this::customizeHttpClientConfig)
            .setRequestConfigCallback(this::customizeRequestConfig)
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
      if (!bulkProcessor.awaitClose(1, TimeUnit.MINUTES)) {
        throw new ConnectException("Failed to process all outstanding requests in time.");
      }
    } catch (InterruptedException e) {
      log.error("Bulk processor close was interrupted.", e);
    }
  }

  /**
   * Creates an index. Will not recreate the index if it already exists.
   *
   * @param index the index to create
   */
  public void createIndex(String index) {
    if (indexExists(index)) {
      return;
    }

    CreateIndexRequest request = new CreateIndexRequest(index);
    callWithRetries(
        "create index " + index,
        () -> {
          try {
            client.indices().create(request, RequestOptions.DEFAULT);
          } catch (ElasticsearchStatusException | IOException e) {
            if (!e.getMessage().contains(RESOURCE_ALREADY_EXISTS_EXCEPTION)) {
              throw e;
            }
          }

          return null;
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
        "create mapping",
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
    MappingMetaData mapping = getMapping(index);
    return mapping != null && !mapping.sourceAsMap().isEmpty();
  }

  /**
   * Buffers a record to index. Will ensure that there are no concurrent requests for the same
   * document id.
   *
   * @param record the record to index
   * @param request the associated request to send
   * @throws ConnectException if one of the requests failed
   */
  public void index(SinkRecord record, DocWriteRequest<?> request) {
    if (isFailed()) {
      close();
      throw error.get();
    }

    if (recordMap != null) {
      // make sure that only unique records are being sent in a batch
      while (recordMap.contains(request.id())) {
        flush();
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        } catch (InterruptedException e)  {
          throw new ConnectException(e);
        }
      }
    }

    addToRecordMap(request.id(), record);
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
   * Maps a record to the document id.
   * @param id the document id
   * @param record the record
   */
  private void addToRecordMap(String id, SinkRecord record) {
    if (recordMap != null) {
      recordMap.put(id, record);
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
      public void beforeBulk(long executionId, BulkRequest request) { }

      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        for (BulkItemResponse bulkItemResponse : response) {
          handleResponse(bulkItemResponse);
          removeFromRecordMap(bulkItemResponse.getId());
        }
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        error.compareAndSet(null, new ConnectException("Bulk request failed.", failure));
      }
    };
  }

  /**
   * Calls the specified function with retries and backoffs until the retries are exhausted or the
   * function succeeds.
   *
   * @param description description of the attempted action n present tense
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
   * Configures HTTP authentication and proxy authentication according to the client configuration.
   *
   * @param builder the HttpAsyncClientBuilder
   */
  private void configureAuthentication(HttpAsyncClientBuilder builder) {

    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    if (config.isAuthenticatedConnection()) {
      config.connectionUrls().forEach(
          url ->
              credentialsProvider.setCredentials(
                  new AuthScope(new HttpHost(url)),
                  new UsernamePasswordCredentials(config.username(), config.password().value())
              )
      );
      builder.setDefaultCredentialsProvider(credentialsProvider);
    }

    if (config.isBasicProxyConfigured()) {
      HttpHost proxy = new HttpHost(config.proxyHost(), config.proxyPort());
      builder.setProxy(proxy);

      if (config.isProxyWithAuthenticationConfigured()) {
        credentialsProvider.setCredentials(
            new AuthScope(proxy),
            new UsernamePasswordCredentials(config.proxyUsername(), config.proxyPassword().value())
        );
      }

      builder.setDefaultCredentialsProvider(credentialsProvider);
    }
  }

  /**
   * Configures the client to use SSL if configured.
   *
   * @param builder the HttpAsyncClientBuilder
   */
  private void configureSslContext(HttpAsyncClientBuilder builder) {
    SslFactory kafkaSslFactory = new SslFactory(Mode.CLIENT, null, false);
    kafkaSslFactory.configure(config.sslConfigs());

    SSLContext sslContext;
    try {
      // try AK <= 2.2 first
      sslContext =
          (SSLContext) SslFactory.class.getDeclaredMethod("sslContext").invoke(kafkaSslFactory);
      log.debug("Using AK 2.2 SslFactory methods.");
    } catch (Exception e) {
      // must be running AK 2.3+
      log.debug("Could not find AK 2.2 SslFactory methods. Trying AK 2.3+ methods for SslFactory.");

      Object sslEngine;
      try {
        // try AK <= 2.6 second
        sslEngine = SslFactory.class.getDeclaredMethod("sslEngineBuilder").invoke(kafkaSslFactory);
        log.debug("Using AK 2.2-2.5 SslFactory methods.");

      } catch (Exception ex) {
        // must be running AK 2.6+
        log.debug(
            "Could not find Ak 2.3-2.5 methods for SslFactory."
                + " Trying AK 2.6+ methods for SslFactory."
        );
        try {
          sslEngine =
              SslFactory.class.getDeclaredMethod("sslEngineFactory").invoke(kafkaSslFactory);
          log.debug("Using AK 2.6+ SslFactory methods.");
        } catch (Exception exc) {
          throw new ConnectException("Failed to find methods for SslFactory.", exc);
        }
      }

      try {
        sslContext =
            (SSLContext) sslEngine.getClass().getDeclaredMethod("sslContext").invoke(sslEngine);
      } catch (Exception ex) {
        throw new ConnectException("Could not create SSLContext.", ex);
      }
    }

    HostnameVerifier hostnameVerifier = config.shouldDisableHostnameVerification()
        ? (hostname, session) -> true
        : SSLConnectionSocketFactory.getDefaultHostnameVerifier();

    builder.setSSLContext(sslContext);
    builder.setSSLHostnameVerifier(hostnameVerifier);
    builder.setSSLStrategy(new SSLIOSessionStrategy(sslContext, hostnameVerifier));
  }

  /**
   * Customizes the client according to the configurations.
   *
   * @param builder the HttpAsyncClientBuilder
   * @return the builder
   */
  private HttpAsyncClientBuilder customizeHttpClientConfig(HttpAsyncClientBuilder builder) {
    builder.setMaxConnPerRoute(config.maxInFlightRequests());
    configureAuthentication(builder);

    if (config.secured()) {
      log.info("Using secured connection to {}.", config.connectionUrls());
      configureSslContext(builder);
    } else {
      log.info("Using unsecured connection to {}.", config.connectionUrls());
    }
    return builder;
  }

  /**
   * Customizes each request according to configurations.
   *
   * @param builder the RequestConfigBuilder
   * @return the builder
   */
  private RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder builder) {
    return builder
        .setContentCompressionEnabled(config.compression())
        .setConnectTimeout(config.connectionTimeoutMs())
        .setConnectionRequestTimeout(config.readTimeoutMs());
  }

  /**
   * Gets the mapping for an index.
   *
   * @param index the index to fetch the mapping for
   * @return the MappingMetaData for the index
   */
  private MappingMetaData getMapping(String index) {
    GetMappingsRequest request = new GetMappingsRequest().indices(index);
    GetMappingsResponse response = callWithRetries(
        "get mapping for index " + index,
        () -> client.indices().getMapping(request, RequestOptions.DEFAULT)
    );
    return response.mappings().get(index);
  }

  /**
   * Processes a response from a BulkAction. Successful responses are ignored. Failed responses are
   * reported to the DLQ and handled according to configuration (ignore or fail). Version conflicts
   * are ignored.
   *
   * @param response the response to process
   */
  private void handleResponse(BulkItemResponse response) {
    if (response.isFailed()) {
      for (String error : MALFORMED_DOC_ERRORS) {
        if (response.getFailureMessage().contains(error)) {
          handleMalformedDocResponse(response);
          reportBadRecord(response);
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

        reportBadRecord(response);
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
   * Removes the mapping for document id to record being written.
   *
   * @param id the document id
   */
  private void removeFromRecordMap(String id) {
    if (recordMap != null) {
      recordMap.remove(id);
    }
  }

  /**
   * Reports a bad record to the DLQ.
   *
   * @param response the failed response from ES
   */
  private synchronized void reportBadRecord(BulkItemResponse response) {
    if (reporter != null) {
      SinkRecord original = recordMap.get(response.getId());
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
     * This method is overriden to swallow the stack trace.
     *
     * @return Throwable
     */
    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }
}
