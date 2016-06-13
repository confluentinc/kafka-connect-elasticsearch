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

public class ElasticsearchSinkConnectorConfig extends AbstractConfig {

  private static final String ELASTICSEARCH_GROUP = "Elasticsearch";
  private static final String CONNECTOR_GROUP = "Connector";

  public static final String TRANSPORT_ADDRESSES_CONFIG = "transport.addresses";
  private static final String TRANSPORT_ADDRESSES_DOC = "The list of addresses to connect to Elasticsearch.";
  private static final String TRANSPORT_ADDRESSES_DISPLAY = "Transport Addresses";

  public static final String TYPE_NAME_CONFIG = "type.name";
  private static final String TYPE_NAME_DOC = "The type to use for each index.";
  private static final String TYPE_NAME_DISPLAY = "Type Name";

  public static final String KEY_IGNORE_CONFIG = "key.ignore";
  private static final String KEY_IGNORE_DOC =
      "Whether to ignore the key during indexing. When this is set to true, only the value from the message will be written to Elasticsearch."
      + "Note that this is a global config that applies to all topics. If this is set to true, "
      + "Use `topic.key.ignore` to config for different topics. This value will be overridden by the per topic configuration.";
  private static final boolean KEY_IGNORE_DEFAULT = false;
  private static final String KEY_IGNORE_DISPLAY = "Ignore Key";

  // TODO: remove thid config when single message transform is in
  public static final String TOPIC_INDEX_MAP_CONFIG = "topic.index.map";
  private static final String TOPIC_INDEX_MAP_DOC = "The map between Kafka topics and Elasticsearch indices.";
  private static final String TOPIC_INDEX_MAP_DEFAULT = "";
  private static final String TOPIC_INDEX_MAP_DISPLAY = "Topic to Type";

  public static final String TOPIC_KEY_IGNORE_CONFIG = "topic.key.ignore";
  private static final String TOPIC_KEY_IGNORE_DOC =
      "A list of topics to ignore key when indexing. In case that the key for a topic can be null, you should include the topic in this config "
      + "in order to generate a valid document id.";
  private static final String TOPIC_KEY_IGNORE_DEFAULT = "";
  private static final String TOPIC_KEY_IGNORE_DISPLAY = "Topics to Ignore Key";

  public static final String FLUSH_TIMEOUT_MS_CONFIG = "flush.timeout.ms";
  private static final String FLUSH_TIMEOUT_MS_DOC = "The timeout when flushing data to Elasticsearch.";
  private static final long FLUSH_TIMEOUT_MS_DEFAULT = 10000;
  private static final String FLUSH_TIMEOUT_MS_DISPLAY = "Flush Timeout (ms)";

  public static final String MAX_BUFFERED_RECORDS_CONFIG = "max.buffered.records";
  private static final String MAX_BUFFERED_RECORDS_DOC =
      "Approximately the max number of records each task will buffer. This config controls the memory usage for each task. When the number of "
      + "buffered records is larger than this value, the partitions assigned to this task will be paused.";
  private static final long MAX_BUFFERED_RECORDS_DEFAULT = 100000;
  private static final String MAX_BUFFERED_RECORDS_DISPLAY = "Max Number of Records to Buffer";

  public static final String BATCH_SIZE_CONFIG = "batch.size";
  private static final String BATCH_SIZE_DOC = "The number of requests to process as a batch when writing to Elasticsearch.";
  private static final long BATCH_SIZE_DEFAULT = 10000;
  private static final String BATCH_SIZE_DISPLAY = "Batch Size";

  public static final String SCHEMA_IGNORE_CONFIG = "schema.ignore";
  private static final String SCHEMA_IGNORE_DOC =
      "Whether to ignore schemas during indexing. When this is set to true, the schema in `SinkRecord` will be ignored and Elasticsearch will infer the mapping from data. "
      + "Note that this is a global config that applies to all topics."
      + "Use `topic.schema.ignore` to config for different topics. This value will be overridden by the per topic configuration.";
  private static final boolean SCHEMA_IGNORE_DEFAULT = false;
  private static final String SCHEMA_IGNORE_DISPLAY = "Ignore Schema";

  public static final String TOPIC_SCHEMA_IGNORE_CONFIG = "topic.schema.ignore";
  private static final String TOPIC_SCHEMA_IGNORE_DOC = "A list of topics to ignore schema.";
  private static final String TOPIC_SCHEMA_IGNORE_DEFAULT = "";
  private static final String TOPIC_SCHEMA_IGNORE_DISPLAY = "Topics to Ignore Schema";

  public static ConfigDef baseConfigDef() {
    return new ConfigDef()
        .define(TRANSPORT_ADDRESSES_CONFIG, Type.LIST, Importance.HIGH, TRANSPORT_ADDRESSES_DOC, ELASTICSEARCH_GROUP, 1, Width.LONG, TRANSPORT_ADDRESSES_DISPLAY)
        .define(TYPE_NAME_CONFIG, Type.STRING, Importance.HIGH, TYPE_NAME_DOC, ELASTICSEARCH_GROUP, 2, Width.SHORT, TYPE_NAME_DISPLAY)
        .define(KEY_IGNORE_CONFIG, Type.BOOLEAN, KEY_IGNORE_DEFAULT, Importance.HIGH, KEY_IGNORE_DOC, CONNECTOR_GROUP, 1, Width.SHORT, KEY_IGNORE_DISPLAY)
        .define(FLUSH_TIMEOUT_MS_CONFIG, Type.LONG, FLUSH_TIMEOUT_MS_DEFAULT, Importance.MEDIUM, FLUSH_TIMEOUT_MS_DOC, CONNECTOR_GROUP, 2, Width.SHORT, FLUSH_TIMEOUT_MS_DISPLAY)
        .define(MAX_BUFFERED_RECORDS_CONFIG, Type.LONG, MAX_BUFFERED_RECORDS_DEFAULT, Importance.MEDIUM, MAX_BUFFERED_RECORDS_DOC, CONNECTOR_GROUP, 3, Width.SHORT, MAX_BUFFERED_RECORDS_DISPLAY)
        .define(BATCH_SIZE_CONFIG, Type.LONG, BATCH_SIZE_DEFAULT, Importance.MEDIUM, BATCH_SIZE_DOC, CONNECTOR_GROUP, 4, Width.SHORT, BATCH_SIZE_DISPLAY)
        .define(TOPIC_INDEX_MAP_CONFIG, Type.LIST, TOPIC_INDEX_MAP_DEFAULT, Importance.LOW,
                TOPIC_INDEX_MAP_DOC, CONNECTOR_GROUP, 5, Width.LONG, TOPIC_INDEX_MAP_DISPLAY)
        .define(TOPIC_KEY_IGNORE_CONFIG, Type.LIST, TOPIC_KEY_IGNORE_DEFAULT, Importance.LOW, TOPIC_KEY_IGNORE_DOC, CONNECTOR_GROUP, 6, Width.LONG, TOPIC_KEY_IGNORE_DISPLAY)
        .define(SCHEMA_IGNORE_CONFIG, Type.BOOLEAN, SCHEMA_IGNORE_DEFAULT, Importance.LOW, SCHEMA_IGNORE_DOC, CONNECTOR_GROUP, 7, Width.SHORT, SCHEMA_IGNORE_DISPLAY)
        .define(TOPIC_SCHEMA_IGNORE_CONFIG, Type.LIST, TOPIC_SCHEMA_IGNORE_DEFAULT, Importance.LOW, TOPIC_SCHEMA_IGNORE_DOC, CONNECTOR_GROUP, 8, Width.LONG, TOPIC_SCHEMA_IGNORE_DISPLAY);
  }

  static ConfigDef config = baseConfigDef();

  public ElasticsearchSinkConnectorConfig(Map<String, String> props) {
    super(config, props);
  }

  public static void main(String[] args) {
    System.out.println(config.toRst());
  }
}
