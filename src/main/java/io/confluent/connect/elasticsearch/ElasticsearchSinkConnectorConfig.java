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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.CaseInsensitiveValidString;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import static io.confluent.connect.elasticsearch.jest.JestElasticsearchClient.WriteMethod;
import static io.confluent.connect.elasticsearch.DataConverter.BehaviorOnNullValues;
import static io.confluent.connect.elasticsearch.bulk.BulkProcessor.BehaviorOnMalformedDoc;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.addClientSslSupport;

public class ElasticsearchSinkConnectorConfig extends AbstractConfig {

  // Connector group
  public static final String CONNECTION_URL_CONFIG = "connection.url";
  private static final String CONNECTION_URL_DOC =
      "The comma-separated list of one or more Elasticsearch URLs, such as ``http://eshost1:9200,"
      + "http://eshost2:9200`` or ``https://eshost3:9200``. HTTPS is used for all connections "
      + "if any of the URLs starts with ``https:``. A URL without a protocol is treated as "
      + "``http``.";
  private static final String CONNECTION_URL_DISPLAY = "Connection URLs";

  public static final String CONNECTION_USERNAME_CONFIG = "connection.username";
  private static final String CONNECTION_USERNAME_DOC =
      "The username used to authenticate with Elasticsearch. "
      + "The default is the null, and authentication will only be performed if "
      + " both the username and password are non-null.";
  private static final String CONNECTION_USERNAME_DISPLAY = "Connection Username";
  private static final String CONNECTION_USERNAME_DEFAULT = null;

  public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
  private static final String CONNECTION_PASSWORD_DOC =
      "The password used to authenticate with Elasticsearch. "
      + "The default is the null, and authentication will only be performed if "
      + " both the username and password are non-null.";
  private static final String CONNECTION_PASSWORD_DISPLAY = "Connection Password";
  private static final String CONNECTION_PASSWORD_DEFAULT = null;

  public static final String BATCH_SIZE_CONFIG = "batch.size";
  private static final String BATCH_SIZE_DOC =
      "The number of records to process as a batch when writing to Elasticsearch.";
  private static final String BATCH_SIZE_DISPLAY = "Batch Size";
  private static final int BATCH_SIZE_DEFAULT = 2000;

  public static final String MAX_IN_FLIGHT_REQUESTS_CONFIG = "max.in.flight.requests";
  private static final String MAX_IN_FLIGHT_REQUESTS_DOC =
      "The maximum number of indexing requests that can be in-flight to Elasticsearch before "
      + "blocking further requests.";
  private static final String MAX_IN_FLIGHT_REQUESTS_DISPLAY = "Max In-flight Requests";
  private static final int MAX_IN_FLIGHT_REQUESTS_DEFAULT = 5;

  public static final String MAX_BUFFERED_RECORDS_CONFIG = "max.buffered.records";
  private static final String MAX_BUFFERED_RECORDS_DOC =
      "The maximum number of records each task will buffer before blocking acceptance of more "
      + "records. This config can be used to limit the memory usage for each task.";
  private static final String MAX_BUFFERED_RECORDS_DISPLAY = "Max Buffered Records";
  private static final int MAX_BUFFERED_RECORDS_DEFAULT = 20000;

  public static final String LINGER_MS_CONFIG = "linger.ms";
  private static final String LINGER_MS_DOC =
      "Linger time in milliseconds for batching.\n"
      + "Records that arrive in between request transmissions are batched into a single bulk "
      + "indexing request, based on the ``" + BATCH_SIZE_CONFIG + "`` configuration. Normally "
      + "this only occurs under load when records arrive faster than they can be sent out. "
      + "However it may be desirable to reduce the number of requests even under light load and "
      + "benefit from bulk indexing. This setting helps accomplish that - when a pending batch is"
      + " not full, rather than immediately sending it out the task will wait up to the given "
      + "delay to allow other records to be added so that they can be batched into a single "
      + "request.";
  private static final String LINGER_MS_DISPLAY = "Linger (ms)";
  private static final long LINGER_MS_DEFAULT = 1;

  public static final String FLUSH_TIMEOUT_MS_CONFIG = "flush.timeout.ms";
  private static final String FLUSH_TIMEOUT_MS_DOC =
      "The timeout in milliseconds to use for periodic flushing, and when waiting for buffer "
      + "space to be made available by completed requests as records are added. If this timeout "
      + "is exceeded the task will fail.";
  private static final String FLUSH_TIMEOUT_MS_DISPLAY = "Flush Timeout (ms)";
  private static final int FLUSH_TIMEOUT_MS_DEFAULT = (int) TimeUnit.SECONDS.toMillis(10);

  public static final String MAX_RETRIES_CONFIG = "max.retries";
  private static final String MAX_RETRIES_DOC =
      "The maximum number of retries that are allowed for failed indexing requests. If the retry "
      + "attempts are exhausted the task will fail.";
  private static final String MAX_RETRIES_DISPLAY = "Max Retries";
  private static final int MAX_RETRIES_DEFAULT = 5;

  public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
  private static final String RETRY_BACKOFF_MS_DOC =
      "How long to wait in milliseconds before attempting the first retry of a failed indexing "
      + "request. Upon a failure, this connector may wait up to twice as long as the previous "
      + "wait, up to the maximum number of retries. "
      + "This avoids retrying in a tight loop under failure scenarios.";
  private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (ms)";
  private static final long RETRY_BACKOFF_MS_DEFAULT = 100;

  public static final String CONNECTION_COMPRESSION_CONFIG = "connection.compression";
  private static final String CONNECTION_COMPRESSION_DOC = "Whether to use GZip compression on "
      + "HTTP connection to ElasticSearch. Valid options are ``true`` and ``false``. "
      + "Default is ``false``. To make this setting to work "
      + "the ``http.compression`` setting also needs to be enabled at the Elasticsearch nodes "
      + "or the load-balancer before using it.";
  private static final String CONNECTION_COMPRESSION_DISPLAY = "Compression";
  private static final boolean CONNECTION_COMPRESSION_DEFAULT = false;

  public static final String CONNECTION_TIMEOUT_MS_CONFIG = "connection.timeout.ms";
  private static final String CONNECTION_TIMEOUT_MS_CONFIG_DOC = "How long to wait "
      + "in milliseconds when establishing a connection to the Elasticsearch server. "
      + "The task fails if the client fails to connect to the server in this "
      + "interval, and will need to be restarted.";
  private static final String CONNECTION_TIMEOUT_MS_DISPLAY = "Connection Timeout";
  private static final int CONNECTION_TIMEOUT_MS_DEFAULT = (int) TimeUnit.SECONDS.toMillis(1);

  public static final String READ_TIMEOUT_MS_CONFIG = "read.timeout.ms";
  private static final String READ_TIMEOUT_MS_CONFIG_DOC = "How long to wait in "
      + "milliseconds for the Elasticsearch server to send a response. The task fails "
      + "if any read operation times out, and will need to be restarted to resume "
      + "further operations.";
  private static final String READ_TIMEOUT_MS_DISPLAY = "Read Timeout";
  private static final int READ_TIMEOUT_MS_DEFAULT = (int) TimeUnit.SECONDS.toMillis(3);

  public static final String CREATE_INDICES_AT_START_CONFIG = "auto.create.indices.at.start";
  private static final String CREATE_INDICES_AT_START_DOC = "Auto create the Elasticsearch"
      + " indices at startup. This is useful when the indices are a direct mapping "
      + " of the Kafka topics.";
  private static final String CREATE_INDICES_AT_START_DISPLAY = "Create indices at startup";
  private static final boolean CREATE_INDICES_AT_START_DEFAULT = true;

  // Data Conversion configs
  public static final String TYPE_NAME_CONFIG = "type.name";
  private static final String TYPE_NAME_DOC = "The Elasticsearch type name to use when indexing.";
  private static final String TYPE_NAME_DISPLAY = "Type Name";

  public static final String IGNORE_KEY_TOPICS_CONFIG = "topic.key.ignore";
  public static final String IGNORE_SCHEMA_TOPICS_CONFIG = "topic.schema.ignore";

  public static final String IGNORE_KEY_CONFIG = "key.ignore";
  private static final String IGNORE_KEY_DOC =
      "Whether to ignore the record key for the purpose of forming the Elasticsearch document ID."
          + " When this is set to ``true``, document IDs will be generated as the record's "
          + "``topic+partition+offset``.\n Note that this is a global config that applies to all "
          + "topics, use ``" + IGNORE_KEY_TOPICS_CONFIG + "`` to override as ``true`` for specific "
          + "topics.";
  private static final String IGNORE_KEY_DISPLAY = "Ignore Key mode";
  private static final boolean IGNORE_KEY_DEFAULT = false;

  public static final String IGNORE_SCHEMA_CONFIG = "schema.ignore";
  private static final String IGNORE_SCHEMA_DOC =
      "Whether to ignore schemas during indexing. When this is set to ``true``, the record schema"
          + " will be ignored for the purpose of registering an Elasticsearch mapping."
          + " Elasticsearch will infer the mapping from the data (dynamic mapping needs to be"
          + " enabled by the user).\n Note that this is a global config that applies to all topics,"
          + " use ``" + IGNORE_SCHEMA_TOPICS_CONFIG + "`` to override as ``true`` for specific"
          + " topics.";
  private static final String IGNORE_SCHEMA_DISPLAY = "Ignore Schema mode";
  private static final boolean IGNORE_SCHEMA_DEFAULT = false;

  public static final String COMPACT_MAP_ENTRIES_CONFIG = "compact.map.entries";
  private static final String COMPACT_MAP_ENTRIES_DOC =
      "Defines how map entries with string keys within record values should be written to JSON. "
          + "When this is set to ``true``, these entries are written compactly as "
          + "``\"entryKey\": \"entryValue\"``. "
          + "Otherwise, map entries with string keys are written as a nested document "
          + "``{\"key\": \"entryKey\", \"value\": \"entryValue\"}``. "
          + "All map entries with non-string keys are always written as nested documents. "
          + "Prior to 3.3.0, this connector always wrote map entries as nested documents, "
          + "so set this to ``false`` to use that older behavior.";
  private static final String COMPACT_MAP_ENTRIES_DISPLAY = "Compact Map Entries";
  private static final boolean COMPACT_MAP_ENTRIES_DEFAULT = true;

  @Deprecated
  public static final String TOPIC_INDEX_MAP_CONFIG = "topic.index.map";
  private static final String TOPIC_INDEX_MAP_DOC =
      "This option is now deprecated. A future version may remove it completely. Please use "
          + "single message transforms, such as RegexRouter, to map topic names to index names.\n"
          + "A map from Kafka topic name to the destination Elasticsearch index, represented as "
          + "a list of ``topic:index`` pairs.";
  private static final String TOPIC_INDEX_MAP_DISPLAY = "Topic to Index Map";
  private static final String TOPIC_INDEX_MAP_DEFAULT = "";

  private static final String IGNORE_KEY_TOPICS_DOC =
      "List of topics for which ``" + IGNORE_KEY_CONFIG + "`` should be ``true``.";
  private static final String IGNORE_KEY_TOPICS_DISPLAY = "Topics for 'Ignore Key' mode";
  private static final String IGNORE_KEY_TOPICS_DEFAULT = "";

  private static final String IGNORE_SCHEMA_TOPICS_DOC =
      "List of topics for which ``" + IGNORE_SCHEMA_CONFIG + "`` should be ``true``.";
  private static final String IGNORE_SCHEMA_TOPICS_DISPLAY = "Topics for 'Ignore Schema' mode";
  private static final String IGNORE_SCHEMA_TOPICS_DEFAULT = "";

  public static final String DROP_INVALID_MESSAGE_CONFIG = "drop.invalid.message";
  private static final String DROP_INVALID_MESSAGE_DOC =
          "Whether to drop kafka message when it cannot be converted to output message.";
  private static final String DROP_INVALID_MESSAGE_DISPLAY = "Drop invalid messages";
  private static final boolean DROP_INVALID_MESSAGE_DEFAULT = false;

  public static final String BEHAVIOR_ON_NULL_VALUES_CONFIG = "behavior.on.null.values";
  private static final String BEHAVIOR_ON_NULL_VALUES_DOC = "How to handle records with a "
      + "non-null key and a null value (i.e. Kafka tombstone records). Valid options are "
      + "'ignore', 'delete', and 'fail'.";
  private static final String BEHAVIOR_ON_NULL_VALUES_DISPLAY = "Behavior for null-valued records";

  public static final String BEHAVIOR_ON_MALFORMED_DOCS_CONFIG = "behavior.on.malformed.documents";
  private static final String BEHAVIOR_ON_MALFORMED_DOCS_DOC = "How to handle records that "
      + "Elasticsearch rejects due to some malformation of the document itself, such as an index"
      + " mapping conflict or a field name containing illegal characters. Valid options are "
      + "'ignore', 'warn', and 'fail'.";
  private static final String BEHAVIOR_ON_MALFORMED_DOCS_DISPLAY =
      "Behavior on malformed documents";

  public static final String WRITE_METHOD_CONFIG = "write.method";
  private static final String WRITE_METHOD_DOC = "Method used for writing data to Elasticsearch,"
      + " and one of " + WriteMethod.INSERT.toString() + " or " + WriteMethod.UPSERT.toString()
      + ". The default method is " + WriteMethod.INSERT.toString() + ", in which the "
      + "connector constructs a document from the record value and inserts that document "
      + "into Elasticsearch, completely replacing any existing document with the same ID; "
      + "this matches previous behavior. The " + WriteMethod.UPSERT.toString()
      + " method will create a new document if one with the specified ID does not yet "
      + "exist, or will update an existing document with the same ID by adding/replacing "
      + "only those fields present in the record value. The " + WriteMethod.UPSERT.toString()
      + " method may require additional time and resources of Elasticsearch, so consider "
      + "increasing the " + FLUSH_TIMEOUT_MS_CONFIG + ", " + READ_TIMEOUT_MS_CONFIG
      + ", and decrease " + BATCH_SIZE_CONFIG + " configuration properties.";
  private static final String WRITE_METHOD_DISPLAY = "Write Method";

  // Ssl configs
  public static final String SSL_CONFIG_PREFIX = "elastic.https.";

  public static final String SECURITY_PROTOCOL_CONFIG = "elastic.security.protocol";
  private static final String SECURITY_PROTOCOL_DOC =
      "The security protocol to use when connecting to Elasticsearch. "
          + "Values can be `PLAINTEXT` or `SSL`. If `PLAINTEXT` is passed, "
          + "all configs prefixed by " + SSL_CONFIG_PREFIX + " will be ignored.";
  private static final String SECURITY_PROTOCOL_DISPLAY = "Security protocol";
  private static final String SECURITY_PROTOCOL_DEFAULT = SecurityProtocol.PLAINTEXT.name();

  private static final String CONNECTOR_GROUP = "Connector";
  private static final String DATA_CONVERSION_GROUP = "Data Conversion";
  private static final String SSL_GROUP = "Security";

  protected static ConfigDef baseConfigDef() {
    final ConfigDef configDef = new ConfigDef();
    addConnectorConfigs(configDef);
    addConversionConfigs(configDef);
    addSecurityConfigs(configDef);
    return configDef;
  }

  private static void addConnectorConfigs(ConfigDef configDef) {
    int order = 0;
    configDef
        .define(
            CONNECTION_URL_CONFIG,
            Type.LIST,
            ConfigDef.NO_DEFAULT_VALUE,
            new UrlListValidator(),
            Importance.HIGH,
            CONNECTION_URL_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.LONG,
            CONNECTION_URL_DISPLAY
        ).define(
            CONNECTION_USERNAME_CONFIG,
            Type.STRING,
            CONNECTION_USERNAME_DEFAULT,
            Importance.MEDIUM,
            CONNECTION_USERNAME_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            CONNECTION_USERNAME_DISPLAY
        ).define(
            CONNECTION_PASSWORD_CONFIG,
            Type.PASSWORD,
            CONNECTION_PASSWORD_DEFAULT,
            Importance.MEDIUM,
            CONNECTION_PASSWORD_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            CONNECTION_PASSWORD_DISPLAY
        ).define(
            BATCH_SIZE_CONFIG,
            Type.INT,
            BATCH_SIZE_DEFAULT,
            between(1, 1000000),
            Importance.MEDIUM,
            BATCH_SIZE_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            BATCH_SIZE_DISPLAY
        ).define(
            MAX_IN_FLIGHT_REQUESTS_CONFIG,
            Type.INT,
            MAX_IN_FLIGHT_REQUESTS_DEFAULT,
            between(1, 1000),
            Importance.MEDIUM,
            MAX_IN_FLIGHT_REQUESTS_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            MAX_IN_FLIGHT_REQUESTS_DISPLAY
        ).define(
            MAX_BUFFERED_RECORDS_CONFIG,
            Type.INT,
            MAX_BUFFERED_RECORDS_DEFAULT,
            between(1, Integer.MAX_VALUE),
            Importance.LOW,
            MAX_BUFFERED_RECORDS_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            MAX_BUFFERED_RECORDS_DISPLAY
        ).define(
            LINGER_MS_CONFIG,
            Type.LONG,
            LINGER_MS_DEFAULT,
            between(0, TimeUnit.DAYS.toMillis(7)),
            Importance.LOW,
            LINGER_MS_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            LINGER_MS_DISPLAY
        ).define(
            FLUSH_TIMEOUT_MS_CONFIG,
            Type.LONG,
            FLUSH_TIMEOUT_MS_DEFAULT,
            between(TimeUnit.SECONDS.toMillis(1), Long.MAX_VALUE),
            Importance.LOW,
            FLUSH_TIMEOUT_MS_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            FLUSH_TIMEOUT_MS_DISPLAY
        ).define(
            MAX_RETRIES_CONFIG,
            Type.INT,
            MAX_RETRIES_DEFAULT,
            between(0, Integer.MAX_VALUE),
            Importance.LOW,
            MAX_RETRIES_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            MAX_RETRIES_DISPLAY
        ).define(
            RETRY_BACKOFF_MS_CONFIG,
            Type.LONG,
            RETRY_BACKOFF_MS_DEFAULT,
            between(0, TimeUnit.DAYS.toMillis(1)),
            Importance.LOW,
            RETRY_BACKOFF_MS_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            RETRY_BACKOFF_MS_DISPLAY
        ).define(
            CONNECTION_COMPRESSION_CONFIG,
            Type.BOOLEAN,
            CONNECTION_COMPRESSION_DEFAULT,
            Importance.LOW,
            CONNECTION_COMPRESSION_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            CONNECTION_COMPRESSION_DISPLAY
        ).define(
            CONNECTION_TIMEOUT_MS_CONFIG,
            Type.INT,
            CONNECTION_TIMEOUT_MS_DEFAULT,
            between(0, TimeUnit.HOURS.toMillis(12)),
            Importance.LOW,
            CONNECTION_TIMEOUT_MS_CONFIG_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            CONNECTION_TIMEOUT_MS_DISPLAY
        ).define(
            READ_TIMEOUT_MS_CONFIG,
            Type.INT,
            READ_TIMEOUT_MS_DEFAULT,
            between(0, TimeUnit.DAYS.toMillis(7)),
            Importance.LOW,
            READ_TIMEOUT_MS_CONFIG_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            READ_TIMEOUT_MS_DISPLAY
        ).define(
            CREATE_INDICES_AT_START_CONFIG,
            Type.BOOLEAN,
            CREATE_INDICES_AT_START_DEFAULT,
            Importance.LOW,
            CREATE_INDICES_AT_START_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            CREATE_INDICES_AT_START_DISPLAY
    );
  }

  private static void addConversionConfigs(ConfigDef configDef) {
    int order = 0;
    configDef
        .define(
            TYPE_NAME_CONFIG,
            Type.STRING,
            Importance.HIGH,
            TYPE_NAME_DOC,
            DATA_CONVERSION_GROUP,
            ++order,
            Width.SHORT,
            TYPE_NAME_DISPLAY
        ).define(
            IGNORE_KEY_CONFIG,
            Type.BOOLEAN,
            IGNORE_KEY_DEFAULT,
            Importance.HIGH,
            IGNORE_KEY_DOC,
            DATA_CONVERSION_GROUP,
            ++order,
            Width.SHORT,
            IGNORE_KEY_DISPLAY
        ).define(
            IGNORE_SCHEMA_CONFIG,
            Type.BOOLEAN,
            IGNORE_SCHEMA_DEFAULT,
            Importance.LOW,
            IGNORE_SCHEMA_DOC,
            DATA_CONVERSION_GROUP,
            ++order,
            Width.SHORT,
            IGNORE_SCHEMA_DISPLAY
        ).define(
            COMPACT_MAP_ENTRIES_CONFIG,
            Type.BOOLEAN,
            COMPACT_MAP_ENTRIES_DEFAULT,
            Importance.LOW,
            COMPACT_MAP_ENTRIES_DOC,
            DATA_CONVERSION_GROUP,
            ++order,
            Width.SHORT,
            COMPACT_MAP_ENTRIES_DISPLAY
        ).define(
            TOPIC_INDEX_MAP_CONFIG,
            Type.LIST,
            TOPIC_INDEX_MAP_DEFAULT,
            Importance.LOW,
            TOPIC_INDEX_MAP_DOC,
            DATA_CONVERSION_GROUP,
            ++order,
            Width.LONG,
            TOPIC_INDEX_MAP_DISPLAY
        ).define(
            IGNORE_KEY_TOPICS_CONFIG,
            Type.LIST,
            IGNORE_KEY_TOPICS_DEFAULT,
            Importance.LOW,
            IGNORE_KEY_TOPICS_DOC,
            DATA_CONVERSION_GROUP,
            ++order,
            Width.LONG,
            IGNORE_KEY_TOPICS_DISPLAY
        ).define(
            IGNORE_SCHEMA_TOPICS_CONFIG,
            Type.LIST,
            IGNORE_SCHEMA_TOPICS_DEFAULT,
            Importance.LOW,
            IGNORE_SCHEMA_TOPICS_DOC,
            DATA_CONVERSION_GROUP,
            ++order,
            Width.LONG,
            IGNORE_SCHEMA_TOPICS_DISPLAY
        ).define(
            DROP_INVALID_MESSAGE_CONFIG,
            Type.BOOLEAN,
            DROP_INVALID_MESSAGE_DEFAULT,
            Importance.LOW,
            DROP_INVALID_MESSAGE_DOC,
            DATA_CONVERSION_GROUP,
            ++order,
            Width.LONG,
            DROP_INVALID_MESSAGE_DISPLAY
        ).define(
            BEHAVIOR_ON_NULL_VALUES_CONFIG,
            Type.STRING,
            BehaviorOnNullValues.DEFAULT.toString(),
            BehaviorOnNullValues.VALIDATOR,
            Importance.LOW,
            BEHAVIOR_ON_NULL_VALUES_DOC,
            DATA_CONVERSION_GROUP,
            ++order,
            Width.SHORT,
            BEHAVIOR_ON_NULL_VALUES_DISPLAY
        ).define(
            BEHAVIOR_ON_MALFORMED_DOCS_CONFIG,
            Type.STRING,
            BehaviorOnMalformedDoc.DEFAULT.toString(),
            BehaviorOnMalformedDoc.VALIDATOR,
            Importance.LOW,
            BEHAVIOR_ON_MALFORMED_DOCS_DOC,
            DATA_CONVERSION_GROUP,
            ++order,
            Width.SHORT,
            BEHAVIOR_ON_MALFORMED_DOCS_DISPLAY
        ).define(
            WRITE_METHOD_CONFIG,
            Type.STRING,
            WriteMethod.DEFAULT.toString(),
            WriteMethod.VALIDATOR,
            Importance.LOW,
            WRITE_METHOD_DOC,
            DATA_CONVERSION_GROUP,
            ++order,
            Width.SHORT,
            WRITE_METHOD_DISPLAY
    );
  }

  private static void addSecurityConfigs(ConfigDef configDef) {
    ConfigDef sslConfigDef = new ConfigDef();
    addClientSslSupport(sslConfigDef);
    int order = 0;
    configDef.define(
        SECURITY_PROTOCOL_CONFIG,
        Type.STRING,
        SECURITY_PROTOCOL_DEFAULT,
        CaseInsensitiveValidString.in(
            Arrays.stream(SecurityProtocol.values())
                .map(SecurityProtocol::name)
                .collect(Collectors.toList())
                .toArray(new String[SecurityProtocol.values().length])
        ),
        Importance.MEDIUM,
        SECURITY_PROTOCOL_DOC,
        SSL_GROUP,
        ++order,
        Width.SHORT,
        SECURITY_PROTOCOL_DISPLAY
    );
    configDef.embed(
        SSL_CONFIG_PREFIX, SSL_GROUP, configDef.configKeys().size() + 2, sslConfigDef
    );
  }

  public static final ConfigDef CONFIG = baseConfigDef();

  public ElasticsearchSinkConnectorConfig(Map<String, String> props) {
    super(CONFIG, props);
  }

  public boolean isAuthenticatedConnection() {
    return username() != null && password() != null;
  }

  public boolean secured() {
    SecurityProtocol securityProtocol = securityProtocol();
    return SecurityProtocol.SSL.equals(securityProtocol);
  }

  public boolean shouldDisableHostnameVerification() {
    String sslEndpointIdentificationAlgorithm =
        getString(SSL_CONFIG_PREFIX + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
    return sslEndpointIdentificationAlgorithm != null
        && sslEndpointIdentificationAlgorithm.isEmpty();
  }

  public int batchSize() {
    return getInt(BATCH_SIZE_CONFIG);
  }

  public BehaviorOnMalformedDoc behaviorOnMalformedDoc() {
    return BehaviorOnMalformedDoc.valueOf(
        getString(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG).toUpperCase()
    );
  }

  public BehaviorOnNullValues behaviorOnNullValues() {
    return BehaviorOnNullValues.valueOf(getString(BEHAVIOR_ON_NULL_VALUES_CONFIG).toUpperCase());
  }

  public boolean compression() {
    return getBoolean(CONNECTION_COMPRESSION_CONFIG);
  }

  public int connectionTimeoutMs() {
    return getInt(CONNECTION_TIMEOUT_MS_CONFIG);
  }

  public Set<String> connectionUrls() {
    return new HashSet<>(getList(CONNECTION_URL_CONFIG));
  }

  public boolean createIndicesAtStart() {
    return getBoolean(CREATE_INDICES_AT_START_CONFIG);
  }

  public boolean dropInvalidMessage() {
    return getBoolean(DROP_INVALID_MESSAGE_CONFIG);
  }

  public long flushTimeoutMs() {
    return getLong(FLUSH_TIMEOUT_MS_CONFIG);
  }

  public boolean ignoreKey() {
    return getBoolean(IGNORE_KEY_CONFIG);
  }

  public Set<String> ignoreKeyTopics() {
    return new HashSet<>(getList(IGNORE_KEY_TOPICS_CONFIG));
  }

  public boolean ignoreSchema() {
    return getBoolean(IGNORE_SCHEMA_CONFIG);
  }

  public Set<String> ignoreSchemaTopics() {
    return new HashSet<>(getList(IGNORE_SCHEMA_TOPICS_CONFIG));
  }

  public long lingerMs() {
    return getLong(LINGER_MS_CONFIG);
  }

  public int maxBufferedRecords() {
    return getInt(MAX_BUFFERED_RECORDS_CONFIG);
  }

  public int maxInFlightRequests() {
    return getInt(MAX_IN_FLIGHT_REQUESTS_CONFIG);
  }

  public int maxRetries() {
    return getInt(MAX_RETRIES_CONFIG);
  }

  public Password password() {
    return getPassword(CONNECTION_PASSWORD_CONFIG);
  }

  public int readTimeoutMs() {
    return getInt(READ_TIMEOUT_MS_CONFIG);
  }

  public long retryBackoffMs() {
    return getLong(RETRY_BACKOFF_MS_CONFIG);
  }

  private SecurityProtocol securityProtocol() {
    return SecurityProtocol.valueOf(getString(SECURITY_PROTOCOL_CONFIG));
  }

  public Map<String, Object> sslConfigs() {
    ConfigDef sslConfigDef = new ConfigDef();
    addClientSslSupport(sslConfigDef);
    return sslConfigDef.parse(originalsWithPrefix(SSL_CONFIG_PREFIX));
  }

  @Deprecated
  public Map<String, String> topicToIndexMap() {
    return parseMapConfig(getList(TOPIC_INDEX_MAP_CONFIG));
  }

  public String type() {
    return getString(TYPE_NAME_CONFIG);
  }

  public String username() {
    return getString(CONNECTION_USERNAME_CONFIG);
  }

  public boolean useCompactMapEntries() {
    return getBoolean(COMPACT_MAP_ENTRIES_CONFIG);
  }

  public WriteMethod writeMethod() {
    return WriteMethod.valueOf(getString(WRITE_METHOD_CONFIG).toUpperCase());
  }

  private Map<String, String> parseMapConfig(List<String> values) {
    Map<String, String> map = new HashMap<>();
    for (String value : values) {
      String[] parts = value.split(":");
      String topic = parts[0];
      String type = parts[1];
      map.put(topic, type);
    }

    return map;
  }

  private static class UrlListValidator implements Validator {

    @Override
    public void ensureValid(String name, Object value) {
      @SuppressWarnings("unchecked")
      List<String> urls = (List<String>) value;
      for (String url : urls) {
        try {
          new URI(url);
        } catch (URISyntaxException e) {
          throw new ConfigException(
              name, value, "The provided url '" + url + "' is not a valid url."
          );
        }
      }
    }

    @Override
    public String toString() {
      return "List of valid URIs.";
    }
  }

  public static void main(String[] args) {
    System.out.println(CONFIG.toEnrichedRst());
  }
}
