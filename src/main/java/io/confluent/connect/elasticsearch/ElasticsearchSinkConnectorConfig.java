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

package io.confluent.connect.elasticsearch;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;

import static io.confluent.connect.elasticsearch.DataConverter.BehaviorOnNullValues;

public class ElasticsearchSinkConnectorConfig extends AbstractConfig {

  public static final String CONNECTION_URL_CONFIG = "connection.url";
  private static final String CONNECTION_URL_DOC =
      "List of Elasticsearch HTTP connection URLs e.g. ``http://eshost1:9200,"
      + "http://eshost2:9200``.";
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
      "How long to wait in milliseconds before attempting to retry a failed indexing request. "
      + "This avoids retrying in a tight loop under failure scenarios.";

  public static final String TYPE_NAME_CONFIG = "type.name";
  private static final String TYPE_NAME_DOC = "The Elasticsearch type name to use when indexing.";
  public static final String TOPIC_INDEX_MAP_CONFIG = "topic.index.map";
  private static final String TOPIC_INDEX_MAP_DOC =
      "A map from Kafka topic name to the destination Elasticsearch index, represented as a list "
      + "of ``topic:index`` pairs.";
  public static final String KEY_IGNORE_CONFIG = "key.ignore";
  public static final String TOPIC_KEY_IGNORE_CONFIG = "topic.key.ignore";
  public static final String SCHEMA_IGNORE_CONFIG = "schema.ignore";
  public static final String TOPIC_SCHEMA_IGNORE_CONFIG = "topic.schema.ignore";

  private static final String KEY_IGNORE_DOC =
      "Whether to ignore the record key for the purpose of forming the Elasticsearch document ID."
      + " When this is set to ``true``, document IDs will be generated as the record's "
      + "``topic+partition+offset``.\n Note that this is a global config that applies to all "
      + "topics, use ``" + TOPIC_KEY_IGNORE_CONFIG + "`` to override as ``true`` for specific "
      + "topics.";
  private static final String TOPIC_KEY_IGNORE_DOC =
      "List of topics for which ``" + KEY_IGNORE_CONFIG + "`` should be ``true``.";
  private static final String SCHEMA_IGNORE_CONFIG_DOC =
      "Whether to ignore schemas during indexing. When this is set to ``true``, the record "
      + "schema will be ignored for the purpose of registering an Elasticsearch mapping. "
      + "Elasticsearch will infer the mapping from the data (dynamic mapping needs to be enabled "
      + "by the user).\n Note that this is a global config that applies to all topics, use ``"
      + TOPIC_SCHEMA_IGNORE_CONFIG + "`` to override as ``true`` for specific topics.";
  private static final String TOPIC_SCHEMA_IGNORE_DOC =
      "List of topics for which ``" + SCHEMA_IGNORE_CONFIG + "`` should be ``true``.";

  public static final String COMPACT_MAP_ENTRIES_CONFIG = "compact.map.entries";
  private static final String COMPACT_MAP_ENTRIES_DOC = "Defines how map entries with string keys "
      + "within record values should be written to JSON. When this is set to ``true``, these "
      + "entries are written compactly as ``\"entryKey\": \"entryValue\"``. Otherwise, map "
      + "entries with string keys are written as a nested document "
      + "``{\"key\": \"entryKey\", \"value\": \"entryValue\"}``. "
      + "All map entries with non-string keys are always written as nested documents. "
      + "Prior to 3.3.0, this connector always wrote map entries as nested documents, so set this "
      + "to ``false`` to use that older behavior.";

  public static final String CONNECTION_TIMEOUT_MS_CONFIG = "connection.timeout.ms";
  public static final String READ_TIMEOUT_MS_CONFIG = "read.timeout.ms";
  private static final String CONNECTION_TIMEOUT_MS_CONFIG_DOC =  "How long to wait in "
      + "milliseconds when establishing a connection to the Elasticsearch server. The task fails "
      + "if the client fails to connect to the server in this interval, and will need to be "
      + "restarted.";
  private static final String READ_TIMEOUT_MS_CONFIG_DOC = "How long to wait in milliseconds for "
      + "the Elasticsearch server to send a response. The task fails if any read operation times "
      + "out, and will need to be restarted to resume further operations.";

  public static final String BEHAVIOR_ON_NULL_VALUES_CONFIG = "behavior.on.null.values";
  private static final String BEHAVIOR_ON_NULL_VALUES_DOC = "How to handle records with a "
      + "non-null key and a null value (i.e. Kafka tombstone records). "
      + "Valid options are 'ignore', 'delete', and 'fail'.";

  protected static ConfigDef baseConfigDef() {
    final ConfigDef configDef = new ConfigDef();
    addConnectorConfigs(configDef);
    addConversionConfigs(configDef);
    return configDef;
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
        "Read Timeout");
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
        BEHAVIOR_ON_NULL_VALUES_CONFIG,
        Type.STRING,
        BehaviorOnNullValues.DEFAULT.toString(),
        BehaviorOnNullValues.VALIDATOR,
        Importance.LOW,
        BEHAVIOR_ON_NULL_VALUES_DOC,
        group,
        ++order,
        Width.SHORT,
        "Behavior on null-valued records");
  }

  public static final ConfigDef CONFIG = baseConfigDef();

  public ElasticsearchSinkConnectorConfig(Map<String, String> props) {
    super(CONFIG, props);
  }

  public static void main(String[] args) {
    System.out.println(CONFIG.toEnrichedRst());
  }
}
