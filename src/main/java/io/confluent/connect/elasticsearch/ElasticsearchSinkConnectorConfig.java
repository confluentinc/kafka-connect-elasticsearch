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

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.types.Password;
import org.elasticsearch.common.unit.ByteSizeValue;

import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
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

  public static final String BULK_SIZE_BYTES_CONFIG = "bulk.size.bytes";
  private static final String BULK_SIZE_BYTES_DOC =
      "The maximum size (in bytes) to be process as a batch when"
      + " writing records to Elasticsearch. Setting to '-1' will disable "
      + "this configuration. If the condition set by '" + BATCH_SIZE_CONFIG
      + "' is met first, it will be used instead.";
  private static final String BULK_SIZE_BYTES_DISPLAY = "Bulk Size (bytes)";
  private static final int BULK_SIZE_BYTES_DEFAULT = 5 * 1024 * 1024;

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
      "The timeout in milliseconds to use for periodic flushing, and when waiting for buffer space"
          + " to be made available by completed requests as records are added. If this timeout is"
          + " exceeded the task will fail.";
  private static final String FLUSH_TIMEOUT_MS_DISPLAY = "Flush Timeout (ms)";
  private static final int FLUSH_TIMEOUT_MS_DEFAULT = (int) TimeUnit.MINUTES.toMillis(3);

  public static final String FLUSH_SYNCHRONOUSLY_CONFIG = "flush.synchronously";
  private static final String FLUSH_SYNCHRONOUSLY_DOC =
          "True if flushes should wait for background processing to finish. This has a throughput"
            + " penalty and makes the connector less responsive but allows for topic-mutating SMTs"
            + " (e.g. RegexRouter or TimestampRouter)";
  private static final String FLUSH_SYNCHRONOUSLY_DISPLAY = "Flush synchronously";
  private static final boolean FLUSH_SYNCHRONOUSLY_DEFAULT = false;

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

  public static final String MAX_CONNECTION_IDLE_TIME_MS_CONFIG = "max.connection.idle.time.ms";
  private static final String MAX_CONNECTION_IDLE_TIME_MS_CONFIG_DOC = "How long to wait "
      + "in milliseconds before dropping an idle connection to prevent "
      + "a read timeout.";
  private static final String MAX_CONNECTION_IDLE_TIME_MS_DISPLAY = "Max Connection Idle Time";
  private static final int MAX_CONNECTION_IDLE_TIME_MS_DEFAULT = (int) TimeUnit.MINUTES.toMillis(1);

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

  // Data Conversion configs
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

  private static final String USE_AUTOGENERATED_IDS_DISPLAY = "Elasticsearch Generated IDs";
  private static final boolean USE_AUTOGENERATED_IDS_DEFAULT = false;

  public static final String BEHAVIOR_ON_NULL_VALUES_CONFIG = "behavior.on.null.values";
  private static final String BEHAVIOR_ON_NULL_VALUES_DOC = "How to handle records with a "
      + "non-null key and a null value (i.e. Kafka tombstone records). Valid options are "
      + "'ignore', 'delete', and 'fail'.";
  private static final String BEHAVIOR_ON_NULL_VALUES_DISPLAY = "Behavior for null-valued records";
  private static final BehaviorOnNullValues BEHAVIOR_ON_NULL_VALUES_DEFAULT =
      BehaviorOnNullValues.FAIL;

  public static final String BEHAVIOR_ON_MALFORMED_DOCS_CONFIG = "behavior.on.malformed.documents";
  private static final String BEHAVIOR_ON_MALFORMED_DOCS_DOC = "How to handle records that "
      + "Elasticsearch rejects due to some malformation of the document itself, such as an index"
      + " mapping conflict, a field name containing illegal characters, or a record with a missing"
      + " id. Valid options are ignore', 'warn', and 'fail'.";
  private static final String BEHAVIOR_ON_MALFORMED_DOCS_DISPLAY =
      "Behavior on malformed documents";
  private static final BehaviorOnMalformedDoc BEHAVIOR_ON_MALFORMED_DOCS_DEFAULT =
      BehaviorOnMalformedDoc.FAIL;

  public static final String EXTERNAL_VERSION_HEADER_CONFIG = "external.version.header";
  private static final String EXTERNAL_VERSION_HEADER_DOC = "Header name to pull value for"
          + " external versioning, defaults to using the kafka record offset.  Must have a numeric"
          + " value.";
  private static final String EXTERNAL_VERSION_HEADER_DISPLAY = "External Version Header Name";
  private static final String EXTERNAL_VERSION_HEADER_DEFAULT = "";

  public static final String WRITE_METHOD_CONFIG = "write.method";
  private static final String WRITE_METHOD_DOC = String.format(
      "Method used for writing data to Elasticsearch, and one of %s or %s. The default method is"
          + " %s, in which the connector constructs a document from the record value and inserts"
          + " that document into Elasticsearch, completely replacing any existing document with the"
          + " same ID; this matches previous behavior. The %s method will create a new document if"
          + " one with the specified ID does not yet exist, or will update an existing document"
          + " with the same ID by adding/replacing only those fields present in the record value."
          + " The %s method may require additional time and resources of Elasticsearch, so consider"
          + " increasing the %s and decreasing the %s configuration properties.",
      WriteMethod.INSERT,
      WriteMethod.UPSERT,
      WriteMethod.INSERT,
      WriteMethod.UPSERT,
      WriteMethod.UPSERT,
      READ_TIMEOUT_MS_CONFIG,
      BATCH_SIZE_CONFIG
  );
  private static final String WRITE_METHOD_DISPLAY = "Write Method";
  private static final String WRITE_METHOD_DEFAULT = WriteMethod.INSERT.name();

  // Proxy group
  public static final String PROXY_HOST_CONFIG = "proxy.host";
  private static final String PROXY_HOST_DISPLAY = "Proxy Host";
  private static final String PROXY_HOST_DOC = "The address of the proxy host to connect through. "
      + "Supports the basic authentication scheme only.";
  private static final String PROXY_HOST_DEFAULT = "";

  public static final String PROXY_PORT_CONFIG = "proxy.port";
  private static final String PROXY_PORT_DISPLAY = "Proxy Port";
  private static final String PROXY_PORT_DOC = "The port of the proxy host to connect through.";
  private static final Integer PROXY_PORT_DEFAULT = 8080;

  public static final String PROXY_USERNAME_CONFIG = "proxy.username";
  private static final String PROXY_USERNAME_DISPLAY = "Proxy Username";
  private static final String PROXY_USERNAME_DOC = "The username for the proxy host.";
  private static final String PROXY_USERNAME_DEFAULT = "";

  public static final String PROXY_PASSWORD_CONFIG = "proxy.password";
  private static final String PROXY_PASSWORD_DISPLAY = "Proxy Password";
  private static final String PROXY_PASSWORD_DOC = "The password for the proxy host.";
  private static final Password PROXY_PASSWORD_DEFAULT = null;

  // Ssl configs
  public static final String SSL_CONFIG_PREFIX = "elastic.https.";

  public static final String SECURITY_PROTOCOL_CONFIG = "elastic.security.protocol";
  private static final String SECURITY_PROTOCOL_DOC =
      "The security protocol to use when connecting to Elasticsearch. Values can be `PLAINTEXT` or"
          + " `SSL`. If `PLAINTEXT` is passed, all configs prefixed by " + SSL_CONFIG_PREFIX
          + " will be ignored.";
  private static final String SECURITY_PROTOCOL_DISPLAY = "Security protocol";
  private static final String SECURITY_PROTOCOL_DEFAULT = SecurityProtocol.PLAINTEXT.name();

  // Kerberos configs
  public static final String KERBEROS_PRINCIPAL_CONFIG = "kerberos.user.principal";
  private static final String KERBEROS_PRINCIPAL_DISPLAY = "Kerberos User Principal";
  private static final String KERBEROS_PRINCIPAL_DOC = "The Kerberos user principal the connector"
      + " may use to authenticate with Kerberos.";
  private static final String KERBEROS_PRINCIPAL_DEFAULT = null;

  public static final String KERBEROS_KEYTAB_PATH_CONFIG = "kerberos.keytab.path";
  private static final String KERBEROS_KEYTAB_PATH = "Kerberos Keytab File Path";
  private static final String KERBEROS_KEYTAB_PATH_DOC = "The path to the keytab file to use for"
      + " authentication with Kerberos.";
  private static final String KERBEROS_KEYTAB_PATH_DEFAULT = null;

  // Data stream configs
  public static final String DATA_STREAM_NAMESPACE_CONFIG = "data.stream.namespace";
  private static final String DATA_STREAM_NAMESPACE_DOC = 
      "Generic name describing user-configurable arbitrary grouping to be written to a data "
          + "stream. Can be any arbitrary string that is no longer than 100 characters, "
          + " is in all lowercase, and does not contain spaces or any of these special characters "
          + "``/\\*\"<>|,#:-``. Otherwise, no value indicates the connector will write to regular "
          + "indices instead. If set, this configuration will be used alongside "
          + "``data.stream.type`` and ``data.stream.dataset`` to construct the data stream name in"
          + "the form of {``data.stream.type``}-{``data.stream.dataset``}-{``"
          + DATA_STREAM_NAMESPACE_CONFIG + "``}. "
          + "Defaut value is ``${topic}``, that is to say the topic name.";
  private static final String DATA_STREAM_NAMESPACE_DISPLAY = "Data Stream Namespace";
  private static final String DATA_STREAM_NAMESPACE_DEFAULT = "${topic}";

  public static final String DATA_STREAM_DATASET_CONFIG = "data.stream.dataset";
  private static final String DATA_STREAM_DATASET_DOC =
      "Generic name describing data ingested and its structure to be written to a data stream. Can "
          + "be any arbitrary string that is no longer than 100 characters, is in all lowercase, "
          + "and does not contain spaces or any of these special characters ``/\\*\"<>|,#:-``. "
          + "Otherwise, no value indicates the connector will write to regular indices instead. "
          + "If set, this configuration will be used alongside ``data.stream.type`` and "
          + "``" + DATA_STREAM_NAMESPACE_CONFIG + "`` to construct the data stream name in "
          + "the form of {``data.stream.type``}-{``" + DATA_STREAM_DATASET_CONFIG + "``}-{``"
          + DATA_STREAM_NAMESPACE_CONFIG + "``}.";
  private static final String DATA_STREAM_DATASET_DISPLAY = "Data Stream Dataset";
  private static final String DATA_STREAM_DATASET_DEFAULT = "";
  public static final String USE_AUTO_GENERATED_IDS_CONFIG = "use.autogenerated.ids";
  private static final String USE_AUTO_GENERATED_IDS_DOC = String.format(
          "Whether to use auto-generated Elasticsearch document IDs for insertion requests. "
                  + "Note that this setting removes exactly once guarantees and message "
                  + "delivery will be at least once. Only applies if %s is set to %s."
                  + "When this is set to ``true``, ``%s`` option will also be ignored when "
                  + "sending data to Elasticsearch",
          WRITE_METHOD_CONFIG,
          WriteMethod.INSERT,
          IGNORE_KEY_CONFIG
  );
  public static final String DATA_STREAM_TYPE_CONFIG = "data.stream.type";
  private static final String DATA_STREAM_TYPE_DOC = String.format(
      "Generic type describing the data to be written to data stream. "
          + "The default is %s which indicates the connector will write "
          + "to regular indices instead. If set, this configuration will "
          + "be used alongside %s and %s to construct the data stream name in the form of "
          + "{``%s``}-{``%s``}-{``%s``}. Custom index templates defined in the destination"
          + " cluster are supported.",
      DataStreamType.NONE.name(),
      DATA_STREAM_DATASET_CONFIG,
      DATA_STREAM_NAMESPACE_CONFIG,
      DATA_STREAM_TYPE_CONFIG,
      DATA_STREAM_DATASET_CONFIG,
      DATA_STREAM_NAMESPACE_CONFIG
  );
  private static final String DATA_STREAM_TYPE_DISPLAY = "Data Stream Type";
  private static final DataStreamType DATA_STREAM_TYPE_DEFAULT = DataStreamType.NONE;

  public static final String DATA_STREAM_TIMESTAMP_CONFIG = "data.stream.timestamp.field";
  private static final String DATA_STREAM_TIMESTAMP_DOC = String.format(
      "The Kafka record field to use as the timestamp for the ``@timestamp`` field in documents "
          + "sent to a data stream.\n All documents sent to a data stream needs an ``@timestamp`` "
          + "field with values of type ``date`` or ``data_nanos``. Otherwise, the document "
          + "will not be sent. If multiple fields are provided, the first field listed that "
          + "also appears in the record will be used. If this configuration is left empty, "
          + "all of the documents will use the Kafka record timestamp as the ``@timestamp`` field "
          + "value. Note that ``@timestamp`` still needs to be explicitly listed if records "
          + "already contain this field. This configuration can only be set if ``%s`` and ``%s`` "
          + "are set.",
      DATA_STREAM_TYPE_CONFIG,
      DATA_STREAM_DATASET_CONFIG
  );
  private static final String DATA_STREAM_TIMESTAMP_DISPLAY = "Data Stream Timestamp Field";
  private static final String DATA_STREAM_TIMESTAMP_DEFAULT = "";

  // Resource mapping configs
  public static final String EXTERNAL_RESOURCE_USAGE_CONFIG = "external.resource.usage";
  private static final String EXTERNAL_RESOURCE_USAGE_DOC = String.format(
      "The type of resource the connector writes to, such as indices, datastreams or aliases. "
      + "Valid options are %s, %s, %s, %s, and %s. When set to %s, the connector will "
      + "auto-create indices or datastreams based on the topic name and datastream configurations",
      ExternalResourceUsage.INDEX,
      ExternalResourceUsage.DATASTREAM,
      ExternalResourceUsage.ALIAS_INDEX,
      ExternalResourceUsage.ALIAS_DATASTREAM,
      ExternalResourceUsage.DISABLED,
      ExternalResourceUsage.DISABLED
  );
  private static final String EXTERNAL_RESOURCE_USAGE_DISPLAY = "External Resource Usage";
  private static final String EXTERNAL_RESOURCE_USAGE_DEFAULT =
          ExternalResourceUsage.DISABLED.name();

  public static final String TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG =
          "topic.to.external.resource.mapping";
  private static final String TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_DOC = String.format(
          "A list of topic-to-resource mappings in the format 'topic:resource'. "
          + "If specified, the connector will use the provided resource name "
          + "(index, data stream, or alias) instead of the topic name for writing "
          + "to Elasticsearch. The resource must exist in Elasticsearch before "
          + "configuring the connector. The type of resource (index, data stream, "
          + "or alias) is determined by the '%s' configuration.",
      EXTERNAL_RESOURCE_USAGE_CONFIG
  );
  private static final String TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_DISPLAY =
          "Topic to External Resource Mapping";
  private static final String TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_DEFAULT = "";

  public static final String MAX_EXTERNAL_RESOURCE_MAPPINGS_CONFIG =
          "max.external.resource.mappings";
  private static final String MAX_EXTERNAL_RESOURCE_MAPPINGS_DOC =
          "The maximum number of topic-to-external-resource mappings allowed.";
  private static final String MAX_EXTERNAL_RESOURCE_MAPPINGS_DISPLAY =
          "Maximum External Resource Mappings";
  private static final int MAX_EXTERNAL_RESOURCE_MAPPINGS_DEFAULT = 15;

  // Error message constants for topic-to-resource mapping validation
  public static final String INVALID_MAPPING_FORMAT_ERROR = 
      "Invalid topic-to-resource mapping format. The expected format: topic:resource";
  
  public static final String DUPLICATE_TOPIC_MAPPING_ERROR_FORMAT =
          "Topic '%s' is mapped to multiple resources. "
          + "Each topic must be mapped to exactly one resource.";
  
  public static final String DUPLICATE_RESOURCE_MAPPING_ERROR_FORMAT = 
      "Resource '%s' is mapped from multiple topics. "
      + "Each resource must be mapped to exactly one topic.";

  public static final String TOO_MANY_MAPPINGS_ERROR_FORMAT =
          "Too many topic-to-external-resource mappings are configured (%d). "
                  + "Maximum allowed limit is %d.";

  private final String[] kafkaTopics;

  private static final String CONNECTOR_GROUP = "Connector";
  private static final String DATA_CONVERSION_GROUP = "Data Conversion";
  private static final String PROXY_GROUP = "Proxy";
  private static final String SSL_GROUP = "Security";
  private static final String KERBEROS_GROUP = "Kerberos";

  public enum BehaviorOnMalformedDoc {
    IGNORE,
    WARN,
    FAIL
  }

  public enum BehaviorOnNullValues {
    IGNORE,
    DELETE,
    FAIL
  }

  public enum DataStreamType {
    LOGS,
    METRICS,
    NONE
  }

  public enum ExternalResourceUsage {
    INDEX,
    DATASTREAM,
    ALIAS_INDEX,
    ALIAS_DATASTREAM,
    DISABLED
  }

  public enum SecurityProtocol {
    PLAINTEXT,
    SSL
  }

  public enum WriteMethod {
    INSERT,
    UPSERT
  }

  protected static ConfigDef baseConfigDef() {
    final ConfigDef configDef = new ConfigDef();
    addConnectorConfigs(configDef);
    addConversionConfigs(configDef);
    addProxyConfigs(configDef);
    addSslConfigs(configDef);
    addKerberosConfigs(configDef);
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
            EXTERNAL_RESOURCE_USAGE_CONFIG,
            Type.STRING,
            EXTERNAL_RESOURCE_USAGE_DEFAULT,
            new EnumRecommender<>(ExternalResourceUsage.class),
            Importance.HIGH,
            EXTERNAL_RESOURCE_USAGE_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            EXTERNAL_RESOURCE_USAGE_DISPLAY,
            new EnumRecommender<>(ExternalResourceUsage.class)
        ).define(
            TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG,
            Type.LIST,
            TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_DEFAULT,
            Importance.HIGH,
            TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.LONG,
            TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_DISPLAY
        ).define(
            MAX_EXTERNAL_RESOURCE_MAPPINGS_CONFIG,
            Type.INT,
            MAX_EXTERNAL_RESOURCE_MAPPINGS_DEFAULT,
            ConfigDef.Range.atLeast(1),
            Importance.MEDIUM,
            MAX_EXTERNAL_RESOURCE_MAPPINGS_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            MAX_EXTERNAL_RESOURCE_MAPPINGS_DISPLAY
        ).define(
            DATA_STREAM_TYPE_CONFIG,
            Type.STRING,
            DATA_STREAM_TYPE_DEFAULT.name(),
            Importance.LOW,
            DATA_STREAM_TYPE_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            DATA_STREAM_TYPE_DISPLAY,
            new EnumRecommender<>(DataStreamType.class)
        ).define(
            DATA_STREAM_DATASET_CONFIG,
            Type.STRING,
            DATA_STREAM_DATASET_DEFAULT,
            new DataStreamDatasetValidator(),
            Importance.LOW,
            DATA_STREAM_DATASET_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.MEDIUM,
            DATA_STREAM_DATASET_DISPLAY
        ).define(
            DATA_STREAM_NAMESPACE_CONFIG,
            Type.STRING,
            DATA_STREAM_NAMESPACE_DEFAULT,
            new DataStreamNamespaceValidator(),
            Importance.LOW,
            DATA_STREAM_NAMESPACE_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.MEDIUM,
            DATA_STREAM_NAMESPACE_DISPLAY
        ).define(
            DATA_STREAM_TIMESTAMP_CONFIG,
            Type.LIST,
            DATA_STREAM_TIMESTAMP_DEFAULT,
            Importance.LOW,
            DATA_STREAM_TIMESTAMP_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.LONG,
            DATA_STREAM_TIMESTAMP_DISPLAY
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
            BULK_SIZE_BYTES_CONFIG,
            Type.LONG,
            BULK_SIZE_BYTES_DEFAULT,
            between(-1, Integer.MAX_VALUE),
            Importance.LOW,
            BULK_SIZE_BYTES_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            BULK_SIZE_BYTES_DISPLAY
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
            between(TimeUnit.SECONDS.toMillis(1), TimeUnit.DAYS.toMillis(7)),
            Importance.LOW,
            FLUSH_TIMEOUT_MS_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            FLUSH_TIMEOUT_MS_DISPLAY
        ).define(
            FLUSH_SYNCHRONOUSLY_CONFIG,
            Type.BOOLEAN,
            FLUSH_SYNCHRONOUSLY_DEFAULT,
            Importance.LOW,
            FLUSH_SYNCHRONOUSLY_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            FLUSH_SYNCHRONOUSLY_DISPLAY
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
            MAX_CONNECTION_IDLE_TIME_MS_CONFIG,
            Type.INT,
            MAX_CONNECTION_IDLE_TIME_MS_DEFAULT,
            between(-1, TimeUnit.DAYS.toMillis(1)),
            Importance.LOW,
            MAX_CONNECTION_IDLE_TIME_MS_CONFIG_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            MAX_CONNECTION_IDLE_TIME_MS_DISPLAY
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
    );
  }

  private static void addConversionConfigs(ConfigDef configDef) {
    int order = 0;
    configDef
        .define(
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
            BEHAVIOR_ON_NULL_VALUES_DEFAULT.name(),
            new EnumRecommender<>(BehaviorOnNullValues.class),
            Importance.LOW,
            BEHAVIOR_ON_NULL_VALUES_DOC,
            DATA_CONVERSION_GROUP,
            ++order,
            Width.SHORT,
            BEHAVIOR_ON_NULL_VALUES_DISPLAY,
            new EnumRecommender<>(BehaviorOnNullValues.class)
        ).define(
            BEHAVIOR_ON_MALFORMED_DOCS_CONFIG,
            Type.STRING,
            BEHAVIOR_ON_MALFORMED_DOCS_DEFAULT.name(),
            new EnumRecommender<>(BehaviorOnMalformedDoc.class),
            Importance.LOW,
            BEHAVIOR_ON_MALFORMED_DOCS_DOC,
            DATA_CONVERSION_GROUP,
            ++order,
            Width.SHORT,
            BEHAVIOR_ON_MALFORMED_DOCS_DISPLAY,
            new EnumRecommender<>(BehaviorOnMalformedDoc.class)
        ).define(
            EXTERNAL_VERSION_HEADER_CONFIG,
            Type.STRING,
            EXTERNAL_VERSION_HEADER_DEFAULT,
            Importance.LOW,
            EXTERNAL_VERSION_HEADER_DOC,
            DATA_CONVERSION_GROUP,
            ++order,
            Width.SHORT,
            EXTERNAL_VERSION_HEADER_DISPLAY
        ).define(
            WRITE_METHOD_CONFIG,
            Type.STRING,
            WRITE_METHOD_DEFAULT,
            new EnumRecommender<>(WriteMethod.class),
            Importance.LOW,
            WRITE_METHOD_DOC,
            DATA_CONVERSION_GROUP,
            ++order,
            Width.SHORT,
            WRITE_METHOD_DISPLAY,
            new EnumRecommender<>(WriteMethod.class)
        ).define(
            USE_AUTO_GENERATED_IDS_CONFIG,
            Type.BOOLEAN,
            USE_AUTOGENERATED_IDS_DEFAULT,
            Importance.MEDIUM,
            USE_AUTO_GENERATED_IDS_DOC,
            CONNECTOR_GROUP,
            ++order,
            Width.SHORT,
            USE_AUTOGENERATED_IDS_DISPLAY
    );
  }

  private static void addProxyConfigs(ConfigDef configDef) {
    int orderInGroup = 0;
    configDef
        .define(
            PROXY_HOST_CONFIG,
            Type.STRING,
            PROXY_HOST_DEFAULT,
            Importance.LOW,
            PROXY_HOST_DOC,
            PROXY_GROUP,
            orderInGroup++,
            Width.LONG,
            PROXY_HOST_DISPLAY
        ).define(
            PROXY_PORT_CONFIG,
            Type.INT,
            PROXY_PORT_DEFAULT,
            between(1, 65535),
            Importance.LOW,
            PROXY_PORT_DOC,
            PROXY_GROUP,
            orderInGroup++,
            Width.LONG,
            PROXY_PORT_DISPLAY
        ).define(
            PROXY_USERNAME_CONFIG,
            Type.STRING,
            PROXY_USERNAME_DEFAULT,
            Importance.LOW,
            PROXY_USERNAME_DOC,
            PROXY_GROUP,
            orderInGroup++,
            Width.LONG,
            PROXY_USERNAME_DISPLAY
        ).define(
            PROXY_PASSWORD_CONFIG,
            Type.PASSWORD,
            PROXY_PASSWORD_DEFAULT,
            Importance.LOW,
            PROXY_PASSWORD_DOC,
            PROXY_GROUP,
            orderInGroup++,
            Width.LONG,
            PROXY_PASSWORD_DISPLAY
    );
  }

  private static void addSslConfigs(ConfigDef configDef) {
    ConfigDef sslConfigDef = new ConfigDef();
    addClientSslSupport(sslConfigDef);
    int order = 0;
    configDef.define(
        SECURITY_PROTOCOL_CONFIG,
        Type.STRING,
        SECURITY_PROTOCOL_DEFAULT,
        new EnumRecommender<>(SecurityProtocol.class),
        Importance.MEDIUM,
        SECURITY_PROTOCOL_DOC,
        SSL_GROUP,
        ++order,
        Width.SHORT,
        SECURITY_PROTOCOL_DISPLAY,
        new EnumRecommender<>(SecurityProtocol.class)
    );
    configDef.embed(SSL_CONFIG_PREFIX, SSL_GROUP, configDef.configKeys().size() + 2, sslConfigDef);
  }

  private static void addKerberosConfigs(ConfigDef configDef) {
    int orderInGroup = 0;
    configDef
        .define(
            KERBEROS_PRINCIPAL_CONFIG,
            Type.STRING,
            KERBEROS_PRINCIPAL_DEFAULT,
            Importance.LOW,
            KERBEROS_PRINCIPAL_DOC,
            KERBEROS_GROUP,
            orderInGroup++,
            Width.LONG,
            KERBEROS_PRINCIPAL_DISPLAY
        ).define(
            KERBEROS_KEYTAB_PATH_CONFIG,
            Type.STRING,
            KERBEROS_KEYTAB_PATH_DEFAULT,
            new FilePathValidator("keytab"),
            Importance.LOW,
            KERBEROS_KEYTAB_PATH_DOC,
            KERBEROS_GROUP,
            orderInGroup++,
            Width.LONG,
            KERBEROS_KEYTAB_PATH
    );
  }

  public static final ConfigDef CONFIG = baseConfigDef();

  protected ElasticsearchSinkConnectorConfig(ConfigDef config, Map<String, String> properties) {
    super(config, properties);
    this.kafkaTopics = getTopicArray(properties);
  }

  public ElasticsearchSinkConnectorConfig(Map<String, String> props) {
    super(CONFIG, props);
    this.kafkaTopics = getTopicArray(props);
  }

  private String[] getTopicArray(Map<?, ?> config) {
    Object obj = config.get("topics");
    return obj == null ? new String[0] : ((String) obj).trim().split("\\s*,\\s*");
  }

  /**
   * Parses and validates topic-to-resource mappings.
   *
   * @return Map of topic to resource names
   * @throws ConfigException if any mapping is invalid or has duplicates
   */
  public Map<String, String> getTopicToExternalResourceMap() {
    List<String> mappings = topicToExternalResourceMapping();
    Map<String, String> topicToExternalResourceMap = new HashMap<>();
    Set<String> seenResources = new HashSet<>();
    
    for (String mapping : mappings) {
      String[] parts = mapping.split(":");
      if (parts.length != 2) {
        throw new ConfigException(
          TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG,
          mapping,
          INVALID_MAPPING_FORMAT_ERROR
        );
      }

      String topic = parts[0].trim();
      String resource = parts[1].trim();

      // Check for duplicate topic mappings
      if (topicToExternalResourceMap.containsKey(topic)) {
        throw new ConfigException(
          TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG,
          mapping,
          String.format(DUPLICATE_TOPIC_MAPPING_ERROR_FORMAT, topic)
        );
      }

      // Check for duplicate resource mappings (enforce 1:1)
      if (seenResources.contains(resource)) {
        throw new ConfigException(
          TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG,
          mapping,
          String.format(DUPLICATE_RESOURCE_MAPPING_ERROR_FORMAT, resource)
        );
      }

      topicToExternalResourceMap.put(topic, resource);
      seenResources.add(resource);
    }

    // Check if the number of mappings exceeds the configured limit
    int maxMappings = maxExternalResourceMappings();
    if (topicToExternalResourceMap.size() > maxMappings) {
      throw new ConfigException(
              TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG,
              mappings.toString(),
              String.format(TOO_MANY_MAPPINGS_ERROR_FORMAT,
                      topicToExternalResourceMap.size(),
                      maxMappings)
      );
    }

    return topicToExternalResourceMap;
  }

  public boolean isAuthenticatedConnection() {
    return username() != null && password() != null;
  }

  public boolean isBasicProxyConfigured() {
    return !getString(PROXY_HOST_CONFIG).isEmpty();
  }

  /**
   * Determines if data streams are being used. Checks the external
   * resource usage approach first, then falls back to legacy data stream configs.
   * 
   * @return true if data streams are configured, false otherwise
   */
  public boolean isDataStream() {
    // Check if using new external resource usage approach
    if (isExternalResourceUsageEnabled()) {
      ExternalResourceUsage usage = externalResourceUsage();
      return usage == ExternalResourceUsage.DATASTREAM
              || usage == ExternalResourceUsage.ALIAS_DATASTREAM;
    }

    // Legacy data stream check
    return !dataStreamType().toUpperCase().equals(DataStreamType.NONE.name())
            && !dataStreamDataset().isEmpty();
  }

  public boolean isProxyWithAuthenticationConfigured() {
    return isBasicProxyConfigured()
        && !getString(PROXY_USERNAME_CONFIG).isEmpty()
        && getPassword(PROXY_PASSWORD_CONFIG) != null;
  }

  public boolean isKerberosEnabled() {
    return kerberosUserPrincipal() != null || keytabPath() != null;
  }

  public boolean isSslEnabled() {
    return SecurityProtocol.SSL == securityProtocol();
  }

  public boolean shouldDisableHostnameVerification() {
    String sslEndpointIdentificationAlgorithm =
        getString(SSL_CONFIG_PREFIX + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
    return sslEndpointIdentificationAlgorithm != null
        && sslEndpointIdentificationAlgorithm.isEmpty();
  }

  public boolean shouldIgnoreKey(String topic) {
    return ignoreKey() || ignoreKeyTopics().contains(topic)
            || (writeMethod().equals(WriteMethod.INSERT) && useAutogeneratedIds());
  }

  public boolean shouldIgnoreSchema(String topic) {
    return ignoreSchema() || ignoreSchemaTopics().contains(topic);
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

  public ByteSizeValue bulkSize() {
    return new ByteSizeValue(getLong(BULK_SIZE_BYTES_CONFIG));
  }

  public boolean compression() {
    return getBoolean(CONNECTION_COMPRESSION_CONFIG);
  }

  public int connectionTimeoutMs() {
    return getInt(CONNECTION_TIMEOUT_MS_CONFIG);
  }

  public Set<String> connectionUrls() {
    return getList(CONNECTION_URL_CONFIG)
        .stream().map(s -> s.endsWith("/") ? s.substring(0, s.length() - 1) : s)
        .collect(Collectors.toCollection(HashSet::new));
  }

  public boolean dropInvalidMessage() {
    return getBoolean(DROP_INVALID_MESSAGE_CONFIG);
  }

  public String dataStreamDataset() {
    return getString(DATA_STREAM_DATASET_CONFIG);
  }

  public String dataStreamNamespace() {
    return getString(DATA_STREAM_NAMESPACE_CONFIG);
  }

  public String dataStreamType() {
    return getString(DATA_STREAM_TYPE_CONFIG);
  }

  public List<String> dataStreamTimestampField() {
    return getList(DATA_STREAM_TIMESTAMP_CONFIG);
  }

  public long flushTimeoutMs() {
    return getLong(FLUSH_TIMEOUT_MS_CONFIG);
  }

  public boolean flushSynchronously() {
    return getBoolean(FLUSH_SYNCHRONOUSLY_CONFIG);
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

  public boolean useAutogeneratedIds() {
    return getBoolean(USE_AUTO_GENERATED_IDS_CONFIG);
  }

  public String kerberosUserPrincipal() {
    return getString(KERBEROS_PRINCIPAL_CONFIG);
  }

  public String keytabPath() {
    return getString(KERBEROS_KEYTAB_PATH_CONFIG);
  }

  public long lingerMs() {
    return getLong(LINGER_MS_CONFIG);
  }

  public int maxBufferedRecords() {
    return getInt(MAX_BUFFERED_RECORDS_CONFIG);
  }

  public int maxIdleTimeMs() {
    return getInt(MAX_CONNECTION_IDLE_TIME_MS_CONFIG);
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

  public String proxyHost() {
    return getString(PROXY_HOST_CONFIG);
  }

  public Password proxyPassword() {
    return getPassword(PROXY_PASSWORD_CONFIG);
  }

  public int proxyPort() {
    return getInt(PROXY_PORT_CONFIG);
  }

  public String proxyUsername() {
    return getString(PROXY_USERNAME_CONFIG);
  }

  public int readTimeoutMs() {
    return getInt(READ_TIMEOUT_MS_CONFIG);
  }

  public long retryBackoffMs() {
    return getLong(RETRY_BACKOFF_MS_CONFIG);
  }

  private SecurityProtocol securityProtocol() {
    return SecurityProtocol.valueOf(getString(SECURITY_PROTOCOL_CONFIG).toUpperCase());
  }

  public Map<String, Object> sslConfigs() {
    ConfigDef sslConfigDef = new ConfigDef();
    addClientSslSupport(sslConfigDef);
    Map<String, Object> sslConfigs = originalsWithPrefix(SSL_CONFIG_PREFIX);
    // if empty path is provided for the keystore/truststore location remove it and associated
    // passwords, otherwise it will lead to an exception while creating the ssl context
    if (sslConfigs.getOrDefault(SSL_KEYSTORE_LOCATION_CONFIG, "").toString().isEmpty()) {
      sslConfigs.remove(SSL_KEYSTORE_LOCATION_CONFIG);
      sslConfigs.remove(SSL_KEYSTORE_PASSWORD_CONFIG);
      sslConfigs.remove(SSL_KEY_PASSWORD_CONFIG);
      sslConfigs.remove(SSL_KEYSTORE_TYPE_CONFIG);
    }
    if (sslConfigs.getOrDefault(SSL_TRUSTSTORE_LOCATION_CONFIG, "").toString().isEmpty()) {
      sslConfigs.remove(SSL_TRUSTSTORE_LOCATION_CONFIG);
      sslConfigs.remove(SSL_TRUSTSTORE_PASSWORD_CONFIG);
      sslConfigs.remove(SSL_TRUSTSTORE_TYPE_CONFIG);
    }
    return sslConfigDef.parse(sslConfigs);
  }

  public String username() {
    return getString(CONNECTION_USERNAME_CONFIG);
  }

  public boolean useCompactMapEntries() {
    return getBoolean(COMPACT_MAP_ENTRIES_CONFIG);
  }

  public boolean hasExternalVersionHeader() {
    return !getString(EXTERNAL_VERSION_HEADER_CONFIG).isEmpty();
  }

  public String externalVersionHeader() {
    return getString(EXTERNAL_VERSION_HEADER_CONFIG);
  }

  public WriteMethod writeMethod() {
    return WriteMethod.valueOf(getString(WRITE_METHOD_CONFIG).toUpperCase());
  }

  public ExternalResourceUsage externalResourceUsage() {
    return ExternalResourceUsage.valueOf(getString(EXTERNAL_RESOURCE_USAGE_CONFIG).toUpperCase());
  }

  /**
   * Checks if external resource usage is enabled.
   * 
   * @return true if external resource usage is configured, false if DISABLED
   */
  public boolean isExternalResourceUsageEnabled() {
    return externalResourceUsage() != ExternalResourceUsage.DISABLED;
  }

  public List<String> topicToExternalResourceMapping() {
    return getList(TOPIC_TO_EXTERNAL_RESOURCE_MAPPING_CONFIG);
  }

  public String[] getKafkaTopics() {
    return this.kafkaTopics;
  }

  public int maxExternalResourceMappings() {
    return getInt(MAX_EXTERNAL_RESOURCE_MAPPINGS_CONFIG);
  }

  private static class DataStreamNamespaceValidator implements Validator {

    @Override
    @SuppressWarnings("unchecked")
    public void ensureValid(String name, Object value) {
      if (value == null) {
        return;
      }

      String namespace = (String) value;

      if (namespace.length() > 100) {
        throw new ConfigException(
            name, namespace, "The specified namespace must be no longer than 100 characters."
        );
      }

      if (!namespace.equals(namespace.toLowerCase())) {
        throw new ConfigException(
            name, namespace, "The specified namespace must be in all lowercase."
        );
      }

      if (namespace.matches(".*[\\\\\\/\\*\\?\\\"<>| ,#\\-:]+.*")) {
        throw new ConfigException(
            name, namespace,
            "The specified namespace must not contain any spaces or "
            + "invalid characters \\/*?\"<>|,#-:"
        );
      }
    }

    @Override
    public String toString() {
      return "A valid namespace name that is all lowercase, less than 100 characters, and "
          + "does not contain any spaces or invalid characters \\/*?\"<>|,#-:";
    }
  }

  private static class DataStreamDatasetValidator implements Validator {

    @Override
    @SuppressWarnings("unchecked")
    public void ensureValid(String name, Object value) {
      if (value == null) {
        return;
      }

      String dataset = (String) value;

      if (dataset.length() > 100) {
        throw new ConfigException(
            name, dataset, "The specified dataset must be no longer than 100 characters."
        );
      }

      if (!dataset.equals(dataset.toLowerCase())) {
        throw new ConfigException(
            name, dataset, "The specified dataset must be in all lowercase."
        );
      }

      if (dataset.matches(".*[\\\\\\/\\*\\?\\\"<>| ,#\\-:]+.*")) {
        throw new ConfigException(
            name, dataset,
            "The specified dataset must not contain any spaces or "
            + "invalid characters \\/*?\"<>|,#-:"
        );
      }
    }

    @Override
    public String toString() {
      return "A valid dataset name that is all lowercase, less than 100 characters, and "
          + "does not contain any spaces or invalid characters \\/*?\"<>|,#-:";
    }
  }

  private static class UrlListValidator implements Validator {

    @Override
    @SuppressWarnings("unchecked")
    public void ensureValid(String name, Object value) {
      if (value == null) {
        throw new ConfigException(name, value, "The config must be provided and non-null.");
      }

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

  private static class FilePathValidator implements Validator {

    private String extension;

    public FilePathValidator(String extension) {
      this.extension = extension;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void ensureValid(String name, Object value) {
      if (value == null) {
        return;
      }

      if (!((String) value).endsWith(extension)) {
        throw new ConfigException(name, value, "The specified file must end with ." + extension);
      }

      File file = new File((String) value);
      if (!file.exists()) {
        throw new ConfigException(name, value, "The specified file does not exist.");
      }
    }

    @Override
    public String toString() {
      return "Existing file with " + extension + " extension.";
    }
  }

  public static void main(String[] args) {
    System.out.println(CONFIG.toEnrichedRst());
  }
}
