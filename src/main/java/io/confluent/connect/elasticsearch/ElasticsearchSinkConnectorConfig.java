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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;

import static io.confluent.connect.elasticsearch.jest.JestElasticsearchClient.WriteMethod;
import static io.confluent.connect.elasticsearch.DataConverter.BehaviorOnNullValues;
import static io.confluent.connect.elasticsearch.bulk.BulkProcessor.BehaviorOnMalformedDoc;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.addClientSslSupport;

public class ElasticsearchSinkConnectorConfig extends AbstractConfig {
  private static final String SSL_GROUP = "Security";

  public static final String CONNECTION_URL_CONFIG = "connection.url";
  private static final String CONNECTION_URL_DOC =
      "The comma-separated list of one or more Elasticsearch URLs, such as ``http://eshost1:9200,"
      + "http://eshost2:9200`` or ``https://eshost3:9200``. HTTPS is used for all connections "
      + "if any of the URLs starts with ``https:``. A URL without a protocol is treated as "
      + "``http``.";
  public static final String CONNECTION_USERNAME_CONFIG = "connection.username";
  private static final String CONNECTION_USERNAME_DOC =
      "The username used to authenticate with Elasticsearch. "
      + "The default is the null, and authentication will only be performed if "
      + " both the username and password are non-null.";
  public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
  private static final String CONNECTION_PASSWORD_DOC =
      "The password used to authenticate with Elasticsearch. "
      + "The default is the null, and authentication will only be performed if "
      + " both the username and password are non-null.";
  public static final String BATCH_SIZE_CONFIG = "batch.size";
  private static final String BATCH_SIZE_DOC =
      "The number of records to process as a batch when writing to Elasticsearch.";
  public static final String MAX_IN_FLIGHT_REQUESTS_CONFIG = "max.in.flight.requests";
  private static final String MAX_IN_FLIGHT_REQUESTS_DOC =
      "The maximum number of indexing requests that can be in-flight to Elasticsearch before "
      + "blocking further requests.";
  public static final String MAX_BUFFERED_RECORDS_CONFIG = "max.buffered.records";
  private static final String MAX_BUFFERED_RECORDS_DOC =
      "The maximum number of records each task will buffer before blocking acceptance of more "
      + "records. This config can be used to limit the memory usage for each task.";
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
  public static final String FLUSH_TIMEOUT_MS_CONFIG = "flush.timeout.ms";
  private static final String FLUSH_TIMEOUT_MS_DOC =
      "The timeout in milliseconds to use for periodic flushing, and when waiting for buffer "
      + "space to be made available by completed requests as records are added. If this timeout "
      + "is exceeded the task will fail.";
  public static final String MAX_RETRIES_CONFIG = "max.retries";
  private static final String MAX_RETRIES_DOC =
      "The maximum number of retries that are allowed for failed indexing requests. If the retry "
      + "attempts are exhausted the task will fail.";
  public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
  private static final String RETRY_BACKOFF_MS_DOC =
      "How long to wait in milliseconds before attempting the first retry of a failed indexing "
      + "request. Upon a failure, this connector may wait up to twice as long as the previous "
      + "wait, up to the maximum number of retries. "
      + "This avoids retrying in a tight loop under failure scenarios.";

  public static final String TYPE_NAME_CONFIG = "type.name";
  private static final String TYPE_NAME_DOC = "The Elasticsearch type name to use when indexing.";

  @Deprecated
  public static final String TOPIC_INDEX_MAP_CONFIG = "topic.index.map";
  private static final String TOPIC_INDEX_MAP_DOC =
      "This option is now deprecated. A future version may remove it completely. Please use "
          + "single message transforms, such as RegexRouter, to map topic names to index names.\n"
          + "A map from Kafka topic name to the destination Elasticsearch index, represented as "
          + "a list of ``topic:index`` pairs.";
  public static final String KEY_IGNORE_CONFIG = "key.ignore";
  public static final String TOPIC_KEY_IGNORE_CONFIG = "topic.key.ignore";
  public static final String VERSION_IGNORE_CONFIG = "version.ignore";
  public static final String SCHEMA_IGNORE_CONFIG = "schema.ignore";
  public static final String TOPIC_SCHEMA_IGNORE_CONFIG = "topic.schema.ignore";
  public static final String DROP_INVALID_MESSAGE_CONFIG = "drop.invalid.message";

  private static final String KEY_IGNORE_DOC =
      "Whether to ignore the record key for the purpose of forming the Elasticsearch document ID."
      + " When this is set to ``true``, document IDs will be generated as the record's "
      + "``topic+partition+offset``.\n Note that this is a global config that applies to all "
      + "topics, use ``" + TOPIC_KEY_IGNORE_CONFIG + "`` to override as ``true`` for specific "
      + "topics." ;
  private static final String TOPIC_KEY_IGNORE_DOC =
      "List of topics for which ``" + KEY_IGNORE_CONFIG + "`` should be ``true``.";
  private static final String SCHEMA_IGNORE_CONFIG_DOC =
      "Whether to ignore schemas during indexing. When this is set to ``true``, the record "
      + "schema will be ignored for the purpose of registering an Elasticsearch mapping. "
      + "Elasticsearch will infer the mapping from the data (dynamic mapping needs to be enabled "
      + "by the user).\n Note that this is a global config that applies to all topics. Use ``"
      + TOPIC_SCHEMA_IGNORE_CONFIG + "`` to override as ``true`` for specific topics.";
  private static final String TOPIC_SCHEMA_IGNORE_DOC =
      "List of topics for which ``" + SCHEMA_IGNORE_CONFIG + "`` should be ``true``.";
  private static final String DROP_INVALID_MESSAGE_DOC =
          "Whether to drop kafka message when it cannot be converted to output message.";

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

  public static final String CONNECTION_TIMEOUT_MS_CONFIG = "connection.timeout.ms";
  public static final String READ_TIMEOUT_MS_CONFIG = "read.timeout.ms";
  private static final String CONNECTION_TIMEOUT_MS_CONFIG_DOC = "How long to wait "
      + "in milliseconds when establishing a connection to the Elasticsearch server. "
      + "The task fails if the client fails to connect to the server in this "
      + "interval, and will need to be restarted.";
  private static final String READ_TIMEOUT_MS_CONFIG_DOC = "How long to wait in "
      + "milliseconds for the Elasticsearch server to send a response. The task fails "
      + "if any read operation times out, and will need to be restarted to resume "
      + "further operations.";

  public static final String CONNECTION_COMPRESSION_CONFIG = "connection.compression";
  private static final String CONNECTION_COMPRESSION_DOC = "Whether to use GZip compression on "
          + "HTTP connection to ElasticSearch. Valid options are ``true`` and ``false``. "
          + "Default is ``false``. To make this setting to work "
          + "the ``http.compression`` setting also needs to be enabled at the Elasticsearch nodes "
          + "or the load-balancer before using it.";

  public static final String BEHAVIOR_ON_NULL_VALUES_CONFIG = "behavior.on.null.values";
  private static final String BEHAVIOR_ON_NULL_VALUES_DOC = "How to handle records with a "
      + "non-null key and a null value (i.e. Kafka tombstone records). Valid options are "
      + "'ignore', 'delete', and 'fail'.";

  public static final String BEHAVIOR_ON_MALFORMED_DOCS_CONFIG = "behavior.on.malformed.documents";
  private static final String BEHAVIOR_ON_MALFORMED_DOCS_DOC = "How to handle records that "
      + "Elasticsearch rejects due to some malformation of the document itself, such as an index"
      + " mapping conflict or a field name containing illegal characters. Valid options are "
      + "'ignore', 'warn', and 'fail'.";
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

  public static final String CONNECTION_SSL_CONFIG_PREFIX = "elastic.https.";

  public static final String AUTO_CREATE_INDICES_AT_START_CONFIG = "auto.create.indices.at.start";
  private static final String AUTO_CREATE_INDICES_AT_START_DOC = "Auto create the Elasticsearch"
      + " indices at startup. This is useful when the indices are a direct mapping "
      + " of the Kafka topics.";

  private static final String ELASTICSEARCH_SECURITY_PROTOCOL_CONFIG = "elastic.security.protocol";
  private static final String ELASTICSEARCH_SECURITY_PROTOCOL_DOC =
      "The security protocol to use when connecting to Elasticsearch. "
          + "Values can be `PLAINTEXT` or `SSL`. If `PLAINTEXT` is passed, "
          + "all configs prefixed by " + CONNECTION_SSL_CONFIG_PREFIX + " will be ignored.";

  protected static ConfigDef baseConfigDef() {
    final ConfigDef configDef = new ConfigDef();
    addConnectorConfigs(configDef);
    addConversionConfigs(configDef);
    addSecurityConfigs(configDef);
    return configDef;
  }

  private static void addSecurityConfigs(ConfigDef configDef) {
    ConfigDef sslConfigDef = new ConfigDef();
    addClientSslSupport(sslConfigDef);
    int order = 0;
    configDef.define(
        ELASTICSEARCH_SECURITY_PROTOCOL_CONFIG,
        Type.STRING,
        SecurityProtocol.PLAINTEXT.name(),
        Importance.MEDIUM,
        ELASTICSEARCH_SECURITY_PROTOCOL_DOC,
        SSL_GROUP,
        ++order,
        Width.SHORT,
        "Security protocol"
    );
    configDef.embed(
        CONNECTION_SSL_CONFIG_PREFIX, SSL_GROUP,
        configDef.configKeys().size() + 2, sslConfigDef
    );
  }

  private static void addConnectorConfigs(ConfigDef configDef) {
    final String group = "Connector";
    int order = 0;
    configDef.define(
        CONNECTION_URL_CONFIG,
        Type.LIST,
        Importance.HIGH,
        CONNECTION_URL_DOC,
        group,
        ++order,
        Width.LONG,
        "Connection URLs"
    ).define(
        CONNECTION_USERNAME_CONFIG,
        Type.STRING,
        null,
        Importance.MEDIUM,
        CONNECTION_USERNAME_DOC,
        group,
        ++order,
        Width.SHORT,
        "Connection Username"
    ).define(
        CONNECTION_PASSWORD_CONFIG,
        Type.PASSWORD,
        null,
        Importance.MEDIUM,
        CONNECTION_PASSWORD_DOC,
        group,
        ++order,
        Width.SHORT,
        "Connection Password"
    ).define(
        BATCH_SIZE_CONFIG,
        Type.INT,
        2000,
        Importance.MEDIUM,
        BATCH_SIZE_DOC,
        group,
        ++order,
        Width.SHORT,
        "Batch Size"
    ).define(
        MAX_IN_FLIGHT_REQUESTS_CONFIG,
        Type.INT,
        5,
        Importance.MEDIUM,
        MAX_IN_FLIGHT_REQUESTS_DOC,
        group,
        5,
        Width.SHORT,
        "Max In-flight Requests"
    ).define(
        MAX_BUFFERED_RECORDS_CONFIG,
        Type.INT,
        20000,
        Importance.LOW,
        MAX_BUFFERED_RECORDS_DOC,
        group,
        ++order,
        Width.SHORT,
        "Max Buffered Records"
    ).define(
        LINGER_MS_CONFIG,
        Type.LONG,
        1L,
        Importance.LOW,
        LINGER_MS_DOC,
        group,
        ++order,
        Width.SHORT,
        "Linger (ms)"
    ).define(
        FLUSH_TIMEOUT_MS_CONFIG,
        Type.LONG,
        10000L,
        Importance.LOW,
        FLUSH_TIMEOUT_MS_DOC,
        group,
        ++order,
        Width.SHORT,
        "Flush Timeout (ms)"
    ).define(
        MAX_RETRIES_CONFIG,
        Type.INT,
        5,
        Importance.LOW,
        MAX_RETRIES_DOC,
        group,
        ++order,
        Width.SHORT,
        "Max Retries"
    ).define(
        RETRY_BACKOFF_MS_CONFIG,
        Type.LONG,
        100L,
        Importance.LOW,
        RETRY_BACKOFF_MS_DOC,
        group,
        ++order,
        Width.SHORT,
        "Retry Backoff (ms)"
      ).define(
        CONNECTION_COMPRESSION_CONFIG,
        Type.BOOLEAN,
        false,
        Importance.LOW,
        CONNECTION_COMPRESSION_DOC,
        group,
        ++order,
        Width.SHORT,
        "Compression"
      ).define(
        CONNECTION_TIMEOUT_MS_CONFIG, 
        Type.INT, 
        1000, 
        Importance.LOW, 
        CONNECTION_TIMEOUT_MS_CONFIG_DOC,
        group, 
        ++order, 
        Width.SHORT, 
        "Connection Timeout"
        ).define(
        READ_TIMEOUT_MS_CONFIG, 
        Type.INT, 
        3000, 
        Importance.LOW, 
        READ_TIMEOUT_MS_CONFIG_DOC,
        group,
        ++order, 
        Width.SHORT, 
        "Read Timeout"
    ).define(
        AUTO_CREATE_INDICES_AT_START_CONFIG,
        Type.BOOLEAN,
        true,
        Importance.LOW,
        AUTO_CREATE_INDICES_AT_START_DOC,
        group,
        ++order,
        Width.SHORT,
        "Create indices at startup"
    );
  }

  public boolean secured() {
    SecurityProtocol securityProtocol = securityProtocol();
    return SecurityProtocol.SSL.equals(securityProtocol);
  }

  private SecurityProtocol securityProtocol() {
    return SecurityProtocol.valueOf(getString(ELASTICSEARCH_SECURITY_PROTOCOL_CONFIG));
  }

  private static void addConversionConfigs(ConfigDef configDef) {
    final String group = "Data Conversion";
    int order = 0;
    configDef.define(
        TYPE_NAME_CONFIG,
        Type.STRING,
        Importance.HIGH,
        TYPE_NAME_DOC,
        group,
        ++order,
        Width.SHORT,
        "Type Name"
    ).define(
        KEY_IGNORE_CONFIG,
        Type.BOOLEAN,
        false,
        Importance.HIGH,
        KEY_IGNORE_DOC,
        group,
        ++order,
        Width.SHORT,
        "Ignore Key mode"
    ).define(
            VERSION_IGNORE_CONFIG,
            Type.BOOLEAN,
            false,
            Importance.HIGH,
            KEY_IGNORE_DOC,
            group,
            ++order,
            Width.SHORT,
            "Ignore Version mode"
    ).define(
        SCHEMA_IGNORE_CONFIG,
        Type.BOOLEAN,
        false,
        Importance.LOW,
        SCHEMA_IGNORE_CONFIG_DOC,
        group,
        ++order,
        Width.SHORT,
        "Ignore Schema mode"
    ).define(
        COMPACT_MAP_ENTRIES_CONFIG,
        Type.BOOLEAN,
        true,
        Importance.LOW,
        COMPACT_MAP_ENTRIES_DOC,
        group,
        ++order,
        Width.SHORT,
        "Compact Map Entries"
    ).define(
        TOPIC_INDEX_MAP_CONFIG,
        Type.LIST,
        "",
        Importance.LOW,
        TOPIC_INDEX_MAP_DOC,
        group,
        ++order,
        Width.LONG,
        "Topic to Index Map"
    ).define(
        TOPIC_KEY_IGNORE_CONFIG,
        Type.LIST,
        "",
        Importance.LOW,
        TOPIC_KEY_IGNORE_DOC,
        group,
        ++order,
        Width.LONG,
        "Topics for 'Ignore Key' mode"
    ).define(
        TOPIC_SCHEMA_IGNORE_CONFIG,
        Type.LIST,
        "",
        Importance.LOW,
        TOPIC_SCHEMA_IGNORE_DOC,
        group,
        ++order,
        Width.LONG,
        "Topics for 'Ignore Schema' mode"
    ).define(
        DROP_INVALID_MESSAGE_CONFIG,
        Type.BOOLEAN,
        false,
        Importance.LOW,
        DROP_INVALID_MESSAGE_DOC,
        group,
        ++order,
        Width.LONG,
        "Drop invalid messages"
    ).define(
        BEHAVIOR_ON_NULL_VALUES_CONFIG,
        Type.STRING,
        BehaviorOnNullValues.DEFAULT.toString(),
        BehaviorOnNullValues.VALIDATOR,
        Importance.LOW,
        BEHAVIOR_ON_NULL_VALUES_DOC,
        group,
        ++order,
        Width.SHORT,
        "Behavior for null-valued records"
    ).define(
        BEHAVIOR_ON_MALFORMED_DOCS_CONFIG,
        Type.STRING,
        BehaviorOnMalformedDoc.DEFAULT.toString(),
        BehaviorOnMalformedDoc.VALIDATOR,
        Importance.LOW,
        BEHAVIOR_ON_MALFORMED_DOCS_DOC,
        group,
        ++order,
        Width.SHORT,
        "Behavior on malformed documents"
    ).define(
        WRITE_METHOD_CONFIG,
        Type.STRING,
        WriteMethod.DEFAULT.toString(),
        WriteMethod.VALIDATOR,
        Importance.LOW,
        WRITE_METHOD_DOC,
        group,
        ++order,
        Width.SHORT,
        "Write method");
  }

  public static final ConfigDef CONFIG = baseConfigDef();

  public ElasticsearchSinkConnectorConfig(Map<String, String> props) {
    super(CONFIG, props);
  }

  public Map<String, Object> sslConfigs() {
    ConfigDef sslConfigDef = new ConfigDef();
    addClientSslSupport(sslConfigDef);
    return sslConfigDef.parse(originalsWithPrefix(CONNECTION_SSL_CONFIG_PREFIX));
  }

  public boolean shouldDisableHostnameVerification() {
    String sslEndpointIdentificationAlgorithm = getString(
            CONNECTION_SSL_CONFIG_PREFIX + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
    return sslEndpointIdentificationAlgorithm != null
            && sslEndpointIdentificationAlgorithm.isEmpty();
  }

  public static void main(String[] args) {
    System.out.println(CONFIG.toEnrichedRst());
  }
}
