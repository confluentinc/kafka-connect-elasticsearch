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

package io.confluent.connect.elasticsearch_2_4.jest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonObject;
import com.instana.sdk.annotation.Span;
import io.confluent.connect.elasticsearch_2_4.ElasticsearchClient;
import io.confluent.connect.elasticsearch_2_4.ElasticsearchSinkConnectorConfig;
import io.confluent.connect.elasticsearch_2_4.IndexableRecord;
import io.confluent.connect.elasticsearch_2_4.Key;
import io.confluent.connect.elasticsearch_2_4.Mapping;
import io.confluent.connect.elasticsearch_2_4.RetryUtil;
import io.confluent.connect.elasticsearch_2_4.bulk.BulkRequest;
import io.confluent.connect.elasticsearch_2_4.bulk.BulkResponse;
import io.confluent.connect.elasticsearch_2_4.jest.actions.PortableJestCreateIndexBuilder;
import io.confluent.connect.elasticsearch_2_4.jest.actions.PortableJestGetMappingBuilder;
import io.confluent.connect.elasticsearch_2_4.jest.actions.PortableJestPutMappingBuilder;
import io.searchbox.action.Action;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.cluster.NodesInfo;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.BulkResult.BulkResultItem;
import io.searchbox.core.Delete;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.Update;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.Refresh;
import io.searchbox.indices.mapping.PutMapping;

import java.util.HashMap;
import java.util.Objects;
import javax.net.ssl.HostnameVerifier;

import io.searchbox.params.Parameters;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;

public class JestElasticsearchClient implements ElasticsearchClient {
  private static final Logger log = LoggerFactory.getLogger(JestElasticsearchClient.class);

  // visible for testing
  protected static final String MAPPER_PARSE_EXCEPTION
      = "mapper_parse_exception";
  protected static final String VERSION_CONFLICT_ENGINE_EXCEPTION
      = "version_conflict_engine_exception";
  protected static final String ALL_FIELD_PARAM
      = "_all";
  protected static final String RESOURCE_ALREADY_EXISTS_EXCEPTION
      = "resource_already_exists_exception";

  private static final Logger LOG = LoggerFactory.getLogger(JestElasticsearchClient.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final JestClient client;
  private final Version version;
  private long timeout;
  private WriteMethod writeMethod = WriteMethod.DEFAULT;

  private final Set<String> indexCache = new HashSet<>();

  private int maxRetries;
  private long retryBackoffMs;
  private final Time time = new SystemTime();
  private int retryOnConflict;

  // visible for testing
  public JestElasticsearchClient(JestClient client) {
    try {
      this.client = client;
      this.version = getServerVersion();
    } catch (IOException e) {
      throw new ConnectException(
          "Couldn't start ElasticsearchSinkTask due to connection error:",
          e
      );
    }
  }

  // visible for testing
  public JestElasticsearchClient(String address) {
    try {
      JestClientFactory factory = new JestClientFactory();
      factory.setHttpClientConfig(new HttpClientConfig.Builder(address)
          .multiThreaded(true)
          .build()
      );
      this.client = factory.getObject();
      this.version = getServerVersion();
    } catch (IOException e) {
      throw new ConnectException(
          "Couldn't start ElasticsearchSinkTask due to connection error:",
          e
      );
    } catch (ConfigException e) {
      throw new ConnectException(
          "Couldn't start ElasticsearchSinkTask due to configuration error:",
          e
      );
    }
  }

  public JestElasticsearchClient(Map<String, String> props, Set<String> overrideUrls) {
    this(props, new JestClientFactory(), overrideUrls);
  }

  // visible for testing
  protected JestElasticsearchClient(
          Map<String, String> props,
          JestClientFactory factory,
          Set<String> overrideUrls) {
    try {
      ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
      factory.setHttpClientConfig(getClientConfig(config, overrideUrls));
      this.client = factory.getObject();
      this.version = getServerVersion();
      this.writeMethod = config.writeMethod();
      this.retryBackoffMs = config.retryBackoffMs();
      this.maxRetries = config.maxRetries();
      this.timeout = config.readTimeoutMs();
      this.retryOnConflict = config.maxInFlightRequests();
    } catch (IOException e) {
      throw new ConnectException(
          "Couldn't start ElasticsearchSinkTask due to connection error:",
          e
      );
    } catch (ConfigException e) {
      throw new ConnectException(
          "Couldn't start ElasticsearchSinkTask due to configuration error:",
          e
      );
    }
  }

  // Visible for Testing
  public static HttpClientConfig getClientConfig(
          ElasticsearchSinkConnectorConfig config,
          Set<String> overrideUrls) {
    Set<String> addresses;
    if (overrideUrls != null) {
      addresses = overrideUrls;
    } else {
      addresses = config.connectionUrls();
    }
    HttpClientConfig.Builder builder =
        new HttpClientConfig.Builder(addresses)
            .connTimeout(config.connectionTimeoutMs())
            .readTimeout(config.readTimeoutMs())
            .requestCompressionEnabled(config.compression())
            .defaultMaxTotalConnectionPerRoute(config.maxInFlightRequests())
            .maxConnectionIdleTime(config.maxIdleTimeMs(), TimeUnit.MILLISECONDS)
            .multiThreaded(true);
    if (config.isAuthenticatedConnection()) {
      builder.defaultCredentials(config.username(), config.password().value())
          .preemptiveAuthTargetHosts(
              addresses.stream().map(addr -> HttpHost.create(addr)).collect(Collectors.toSet())
        );
    }

    configureProxy(config, builder);

    if (config.secured()) {
      log.info("Using secured connection to {}", addresses);
      configureSslContext(builder, config);
    } else {
      log.info("Using unsecured connection to {}", addresses);
    }
    return builder.build();
  }

  private static void configureProxy(
      ElasticsearchSinkConnectorConfig config,
      HttpClientConfig.Builder builder
  ) {

    if (config.isBasicProxyConfigured()) {
      HttpHost proxy = new HttpHost(config.proxyHost(), config.proxyPort());
      builder.proxy(proxy);

      if (config.isProxyWithAuthenticationConfigured()) {

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        if (config.isAuthenticatedConnection()) {
          config.connectionUrls().forEach(
              addr ->
                  credentialsProvider.setCredentials(
                      new AuthScope(new HttpHost(addr)),
                      new UsernamePasswordCredentials(config.username(), config.password().value())
                  )
          );
        }

        credentialsProvider.setCredentials(
            new AuthScope(proxy),
            new UsernamePasswordCredentials(config.proxyUsername(), config.proxyPassword().value())
        );

        builder.credentialsProvider(credentialsProvider);
      }
    }
  }

  private static void configureSslContext(
      HttpClientConfig.Builder builder,
      ElasticsearchSinkConnectorConfig config
  ) {
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

    // Sync calls
    SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(
            sslContext, hostnameVerifier);
    builder.sslSocketFactory(sslSocketFactory);

    // Async calls
    SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslContext, hostnameVerifier);
    builder.httpsIOSessionStrategy(sessionStrategy);
  }

  // visible for testing
  protected void setWriteMethod(WriteMethod writeMethod) {
    this.writeMethod = writeMethod;
  }

  /*
   * This method uses the NodesInfo request to get the server version, which is expected to work
   * with all versions of Elasticsearch.
   */
  private Version getServerVersion() throws IOException {
    // Default to newest version for forward compatibility
    Version defaultVersion = Version.ES_V6;

    NodesInfo info = new NodesInfo.Builder().addCleanApiParameter("version").build();
    JsonObject result = client.execute(info).getJsonObject();
    if (result == null) {
      LOG.warn("Couldn't get Elasticsearch version (result is null); assuming {}", defaultVersion);
      return defaultVersion;
    }
    if (!result.has("nodes")) {
      LOG.warn("Couldn't get Elasticsearch version from result {} (result has no nodes). "
          + "Assuming {}.", result, defaultVersion);
      return defaultVersion;
    }

    checkForError(result);

    JsonObject nodesRoot = result.get("nodes").getAsJsonObject();
    if (nodesRoot == null || nodesRoot.entrySet().size() == 0) {
      LOG.warn(
          "Couldn't get Elasticsearch version (response nodesRoot is null or empty); assuming {}",
          defaultVersion
      );
      return defaultVersion;
    }

    JsonObject nodeRoot = nodesRoot.entrySet().iterator().next().getValue().getAsJsonObject();
    if (nodeRoot == null) {
      LOG.warn(
          "Couldn't get Elasticsearch version (response nodeRoot is null); assuming {}",
          defaultVersion
      );
      return defaultVersion;
    }

    String esVersion = nodeRoot.get("version").getAsString();
    Version version;
    if (esVersion == null) {
      version = defaultVersion;
      LOG.warn(
          "Couldn't get Elasticsearch version (response version is null); assuming {}",
          version
      );
    } else if (esVersion.startsWith("1.")) {
      version = Version.ES_V1;
      log.info("Detected Elasticsearch version is {}", version);
    } else if (esVersion.startsWith("2.")) {
      version = Version.ES_V2;
      log.info("Detected Elasticsearch version is {}", version);
    } else if (esVersion.startsWith("5.")) {
      version = Version.ES_V5;
      log.info("Detected Elasticsearch version is {}", version);
    } else if (esVersion.startsWith("6.")) {
      version = Version.ES_V6;
      log.info("Detected Elasticsearch version is {}", version);
    } else if (esVersion.startsWith("7.")) {
      version = Version.ES_V7;
      log.info("Detected Elasticsearch version is {}", version);
    } else {
      version = defaultVersion;
      log.info("Detected unexpected Elasticsearch version {}, using {}", esVersion, version);
    }
    return version;
  }

  private void checkForError(JsonObject result) {
    if (result.has("error") && result.get("error").isJsonObject()) {
      final JsonObject errorObject = result.get("error").getAsJsonObject();
      String errorType = errorObject.has("type") ? errorObject.get("type").getAsString() : "";
      String errorReason = errorObject.has("reason") ? errorObject.get("reason").getAsString() : "";
      throw new ConnectException("Couldn't connect to Elasticsearch, error: "
          + errorType + ", reason: " + errorReason);
    }
  }

  public Version getVersion() {
    return version;
  }

  private boolean indexExists(String index) {
    if (indexCache.contains(index)) {
      return true;
    }
    Action<?> action = new IndicesExists.Builder(index).build();
    try {
      log.info("Index '{}' not found in local cache; checking for existence", index);
      JestResult result = client.execute(action);
      log.debug("Received response for checking existence of index '{}'", index);
      boolean exists = result.isSucceeded();
      if (exists) {
        indexCache.add(index);
        log.info("Index '{}' exists in Elasticsearch; adding to local cache", index);
      } else {
        log.info("Index '{}' not found in Elasticsearch. Error message: {}",
            index, result.getErrorMessage());
      }
      return exists;
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  public void createIndices(Set<String> indices) {
    log.trace("Attempting to discover or create indexes in Elasticsearch: {}", indices);
    for (String index : indices) {
      if (!indexExists(index)) {
        final int maxAttempts = maxRetries + 1;
        int attempts = 1;
        CreateIndex createIndex =
            new PortableJestCreateIndexBuilder(index, version, timeout).build();
        boolean indexed = false;
        while (!indexed) {
          try {
            createIndex(index, createIndex);
            indexed = true;
          } catch (ConnectException e) {
            if (attempts < maxAttempts) {
              long sleepTimeMs = RetryUtil.computeRandomRetryWaitTimeInMillis(attempts - 1,
                  retryBackoffMs);
              log.warn("Failed to create index {} with attempt {}/{}, "
                      + "will attempt retry after {} ms. Failure reason: {}",
                  index, attempts, maxAttempts, sleepTimeMs, e.getMessage());
              time.sleep(sleepTimeMs);
            } else {
              throw e;
            }
            attempts++;
          }
        }
      }
    }
  }

  private void createIndex(String index, CreateIndex createIndex) throws ConnectException {
    try {
      log.info("Requesting Elasticsearch create index '{}'", index);
      JestResult result = client.execute(createIndex);
      log.debug("Received response for request to create index '{}'", index);
      if (!result.isSucceeded()) {
        boolean exists = result.getErrorMessage().contains(RESOURCE_ALREADY_EXISTS_EXCEPTION)
            || indexExists(index);

        // Check if index was created by another client
        if (!exists) {
          String msg =
              result.getErrorMessage() != null ? ": " + result.getErrorMessage() : "";
          throw new ConnectException("Could not create index '" + index + "'" + msg);
        }
        log.info("Index '{}' exists in Elasticsearch; adding to local cache", index);
      } else {
        log.info("Index '{}' created in Elasticsearch; adding to local cache", index);
      }
      indexCache.add(index);
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  public void createMapping(String index, String type, Schema schema) throws IOException {
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set(type, Mapping.inferMapping(this, schema));
    PutMapping putMapping = new PortableJestPutMappingBuilder(index, type, obj.toString(), version)
        .build();
    log.info("Submitting put mapping (type={}) for index '{}' and schema {}", type, index, schema);
    JestResult result = client.execute(putMapping);
    if (!result.isSucceeded()) {
      throw new ConnectException(
          "Cannot create mapping " + obj + " -- " + result.getErrorMessage()
      );
    }
    log.info("Completed put mapping (type={}) for index '{}' and schema {}", type, index, schema);
  }

  /**
   * Get the JSON mapping for given index and type. Returns {@code null} if it does not exist.
   */
  public JsonObject getMapping(String index, String type) throws IOException {
    log.info("Get mapping (type={}) for index '{}'", type, index);
    final JestResult result = client.execute(
        new PortableJestGetMappingBuilder(version)
            .addIndex(index)
            .addType(type)
            .build()
    );
    final JsonObject indexRoot = result.getJsonObject().getAsJsonObject(index);
    if (indexRoot == null) {
      log.debug("Received null (root) mapping (type={}) for index '{}'", type, index);
      return null;
    }
    final JsonObject mappingsJson = indexRoot.getAsJsonObject("mappings");
    if (mappingsJson == null) {
      log.debug("Received null mapping (type={}) for index '{}'", type, index);
      return null;
    }
    log.debug("Received mapping (type={}) for index '{}'", type, index);
    return mappingsJson.getAsJsonObject(type);
  }

  /**
   * Delete all indexes in Elasticsearch (useful for test)
   */
  // For testing purposes
  public void deleteAll() throws IOException {
    log.info("Request deletion of all indexes");
    final JestResult result = client.execute(new DeleteIndex
        .Builder(ALL_FIELD_PARAM)
        .build());
    if (result.isSucceeded()) {
      log.info("Deletion of all indexes succeeded");
    } else {
      String msg = result.getErrorMessage() != null ? ": " + result.getErrorMessage() : "";
      log.warn("Could not delete all indexes: {}", msg);
    }
  }

  /**
   * Refresh all data in elasticsearch, making it available for search (useful for testing)
   */
  // For testing purposes
  public void refresh() throws IOException {
    log.info("Request refresh");
    final JestResult result = client.execute(
        new Refresh.Builder().build()
    );
    if (result.isSucceeded()) {
      log.info("Refresh completed");
    } else {
      String msg = result.getErrorMessage() != null ? ": " + result.getErrorMessage() : "";
      log.warn("Could not refresh: {}", msg);
    }
  }

  public BulkRequest createBulkRequest(List<IndexableRecord> batch) {
    final Bulk.Builder builder = new Bulk.Builder();
    for (IndexableRecord record : batch) {
      builder.addAction(toBulkableAction(record));
    }
    return new JestBulkRequest(builder.build(), batch);
  }

  // visible for testing
  protected BulkableAction<DocumentResult> toBulkableAction(IndexableRecord record) {
    // If payload is null, the record was a tombstone and we should delete from the index.
    if (record.payload == null) {
      return toDeleteRequest(record);
    }
    return writeMethod == WriteMethod.INSERT
            ? toIndexRequest(record)
            : toUpdateRequest(record);
  }

  private Delete toDeleteRequest(IndexableRecord record) {
    Delete.Builder req = new Delete.Builder(record.key.id)
        .index(record.key.index)
        .type(record.key.type);

    if (record.route != null) {
      req.setParameter(Parameters.ROUTING, record.route);
    }
    if (record.parent != null) {
      req.setParameter(Parameters.PARENT, record.parent);
    }

    // TODO: Should version information be set here?
    return req.build();
  }

  private Index toIndexRequest(IndexableRecord record) {
    Index.Builder req = new Index.Builder(record.payload)
        .index(record.key.index)
        .type(record.key.type)
        .id(record.key.id);
    if (record.version != null) {
      req.setParameter("version_type", "external").setParameter("version", record.version);
    }
    if (record.route != null) {
      req.setParameter(Parameters.ROUTING, record.route);
    }
    if (record.parent != null) {
      req.setParameter(Parameters.PARENT, record.parent);
    }
    return req.build();
  }

  private Update toUpdateRequest(IndexableRecord record) {
    String payload = "{\"doc\":" + record.payload
        + ", \"doc_as_upsert\":true}";
    Update.Builder builder =  new Update.Builder(payload)
        .index(record.key.index)
        .type(record.key.type)
        .id(record.key.id)
        .setParameter("retry_on_conflict", retryOnConflict);

    if (record.route != null) {
      builder.setParameter(Parameters.ROUTING, record.route);
    }
    if (record.parent != null) {
      builder.setParameter(Parameters.PARENT, record.parent);
    }

    return builder.build();

  }

  @Span("execute bulk")
  public BulkResponse executeBulk(BulkRequest bulk) throws IOException {
    BulkResult result = client.execute(((JestBulkRequest) bulk).getBulk());
    if (result.isSucceeded()) {
      return BulkResponse.success();
    }
    log.debug("Bulk request failed; collecting error(s)");

    boolean retriable = true;

    final List<Key> versionConflicts = new ArrayList<>();
    final List<String> errors = new ArrayList<>();

    for (BulkResult.BulkResultItem item : result.getItems()) {
      if (item.error != null) {
        final ObjectNode parsedError = (ObjectNode) OBJECT_MAPPER.readTree(item.error);
        final String errorType = parsedError.get("type").asText("");
        if ("version_conflict_engine_exception".equals(errorType)) {
          versionConflicts.add(new Key(item.index, item.type, item.id));
        } else if ("mapper_parse_exception".equals(errorType)) {
          retriable = false;
          errors.add(item.error);
        } else {
          errors.add(item.error);
        }
      }
    }

    if (!versionConflicts.isEmpty()) {
      LOG.warn("Ignoring version conflicts for items: {}", versionConflicts);
      if (errors.isEmpty()) {
        // The only errors were version conflicts
        return BulkResponse.success();
      }
    }

    final String errorInfo = errors.isEmpty() ? result.getErrorMessage() : errors.toString();

    Map<IndexableRecord, BulkResultItem> failedResponses = new HashMap<>();
    List<BulkResultItem> items = result.getItems();
    List<IndexableRecord> records = ((JestBulkRequest) bulk).records();
    for (int i = 0; i < items.size() && i < records.size() ; i++) {
      BulkResultItem item = items.get(i);
      IndexableRecord record = records.get(i);
      if (item.error != null && Objects.equals(item.id, record.key.id)) {
        // sanity check matching IDs
        failedResponses.put(record, item);
      }
    }

    if (items.size() != records.size()) {
      log.error(
          "Elasticsearch bulk response size ({}) does not correspond to records sent ({})",
          ((JestBulkRequest) bulk).records().size(),
          items.size()
      );
    }
    return BulkResponse.failure(retriable, errorInfo, failedResponses);
  }

  // For testing purposes
  public JsonObject search(String query, String index, String type) throws IOException {
    final Search.Builder search = new Search.Builder(query);
    if (index != null) {
      search.addIndex(index);
    }
    if (type != null) {
      search.addType(type);
    }

    log.info("Executing search on index '{}' (type={}): {}", index, type, query);
    final SearchResult result = client.execute(search.build());
    if (result.isSucceeded()) {
      log.info("Executing search succeeded: {}", result);
    } else {
      String msg = result.getErrorMessage() != null ? ": " + result.getErrorMessage() : "";
      log.warn("Failed to execute search: {}", msg);
    }
    return result.getJsonObject();
  }

  public void close() {
    log.debug("Closing Elasticsearch clients");
    try {
      client.close();
    } catch (IOException e) {
      LOG.error("Exception while closing a JEST client", e);
    }
  }

  public enum WriteMethod {
    INSERT,
    UPSERT,
    ;

    public static final WriteMethod DEFAULT = INSERT;
    public static final ConfigDef.Validator VALIDATOR = new ConfigDef.Validator() {
      private final ConfigDef.ValidString validator = ConfigDef.ValidString.in(names());

      @Override
      public void ensureValid(String name, Object value) {
        validator.ensureValid(name, value);
      }

      // Overridden here so that ConfigDef.toEnrichedRst shows possible values correctly
      @Override
      public String toString() {
        return "One of " + INSERT.toString() + " or " + UPSERT.toString();
      }

    };

    public static String[] names() {
      return new String[] {INSERT.toString(), UPSERT.toString()};
    }

    @Override
    public String toString() {
      return name().toLowerCase(Locale.ROOT);
    }
  }
}
