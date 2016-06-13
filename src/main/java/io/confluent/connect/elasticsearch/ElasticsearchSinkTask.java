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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ElasticsearchSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);
  private ElasticsearchWriter writer;
  private Client client;
  private ElasticsearchWriter.Builder builder;
  private static Converter converter;

  static {
    // Config the JsonConverter
    converter = new JsonConverter();
    Map<String, String> configs = new HashMap<>();
    configs.put("schemas.enable", "false");
    converter.configure(configs, false);
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    start(props, null);
  }

  // public for testing
  public void start(Map<String, String> props, Client client) {
    try {
      log.info("Starting ElasticsearchSinkTask.");

      ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
      String type = config.getString(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG);
      boolean ignoreKey = config.getBoolean(ElasticsearchSinkConnectorConfig.KEY_IGNORE_CONFIG);
      boolean ignoreSchema = config.getBoolean(ElasticsearchSinkConnectorConfig.SCHEMA_IGNORE_CONFIG);

      List<String> topicIndex = config.getList(ElasticsearchSinkConnectorConfig.TOPIC_INDEX_MAP_CONFIG);
      List<String> topicIgnoreKey = config.getList(ElasticsearchSinkConnectorConfig.TOPIC_KEY_IGNORE_CONFIG);
      List<String> topicIgnoreSchema = config.getList(ElasticsearchSinkConnectorConfig.TOPIC_SCHEMA_IGNORE_CONFIG);
      Map<String, TopicConfig> topicConfigs = constructTopicConfig(topicIndex, topicIgnoreKey, topicIgnoreSchema);

      long flushTimeoutMs = config.getLong(ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG);
      long maxBufferedRecords = config.getLong(ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG);
      long batchSize = config.getLong(ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG);

      if (client != null) {
        this.client = client;
      } else {
        TransportClient transportClient = TransportClient.builder().build();
        List<InetSocketTransportAddress> addresses = parseAddress(config.getList(ElasticsearchSinkConnectorConfig.TRANSPORT_ADDRESSES_CONFIG));

        for (InetSocketTransportAddress address: addresses) {
          transportClient.addTransportAddress(address);
        }

        this.client = transportClient;
      }

      builder = new ElasticsearchWriter.Builder(this.client)
          .setType(type)
          .setIgnoreKey(ignoreKey)
          .setIgnoreSchema(ignoreSchema)
          .setTopicConfigs(topicConfigs)
          .setFlushTimoutMs(flushTimeoutMs)
          .setMaxBufferedRecords(maxBufferedRecords)
          .setBatchSize(batchSize)
          .setContext(context)
          .setConverter(converter);

    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start ElasticsearchSinkTask due to configuration error:", e);
    } catch (UnknownHostException e) {
      throw new ConnectException("Couldn't start ElasticsearchSinkTask due to unknown host exception:", e);
    }
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    log.debug("Opening the task for topic partitions: {}", partitions);
    writer = builder.build();
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    log.trace("Putting {} to Elasticsearch.", records);
    writer.write(records);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    log.trace("Flushing data to Elasticsearch with the following offsets: {}", offsets);
    writer.flush();
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    log.debug("Closing the task for topic partitions: {}", partitions);
    if (writer != null) {
      writer.close();
    }
  }

  @Override
  public void stop() throws ConnectException {
    log.info("Stopping ElasticsearchSinkTask.");
    if (client != null) {
      client.close();
    }
  }

  private Map<String, String> parseMapConfig(List<String> values) {
    Map<String, String> map = new HashMap<>();
    for (String value: values) {
      String[] parts = value.split(":");
      String topic = parts[0];
      String type = parts[1];
      map.put(topic, type);
    }
    return map;
  }

  private Map<String, TopicConfig> constructTopicConfig(List<String> topicType, List<String> topicIgnoreKey, List<String> topicIgnoreSchema) {
    Map<String, TopicConfig> topicConfigMap = new HashMap<>();
    Map<String, String> topicTypeMap = parseMapConfig(topicType);
    Set<String> topicIgnoreKeySet = new HashSet<>(topicIgnoreKey);
    Set<String> topicIgnoreSchemaSet = new HashSet<>(topicIgnoreSchema);

    for (String topic: topicTypeMap.keySet()) {
      String type = topicTypeMap.get(topic);
      TopicConfig topicConfig = new TopicConfig(type, topicIgnoreKeySet.contains(topic), topicIgnoreSchemaSet.contains(topic));
      topicConfigMap.put(topic, topicConfig);
    }
    return topicConfigMap;
  }

  private InetSocketTransportAddress parseAddress(String address) throws UnknownHostException {
    String[] parts = address.split(":");
    if (parts.length != 2) {
      throw new ConfigException("Not valid address: " + address);
    }
    String host = parts[0];
    int port;
    try {
      port = Integer.parseInt(parts[1]);
    } catch (NumberFormatException e) {
      throw new ConfigException("port is not a valid.", e);
    }
    return new InetSocketTransportAddress(InetAddress.getByName(host), port);
  }

  private List<InetSocketTransportAddress> parseAddress(List<String> addresses) throws UnknownHostException {
    List<InetSocketTransportAddress> transportAddresses = new LinkedList<>();
    for (String address: addresses) {
      transportAddresses.add(parseAddress(address));
    }
    return transportAddresses;
  }

  public static Converter getConverter() {
    return converter;
  }
}
