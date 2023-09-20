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

package io.confluent.connect.elasticsearch_2_4;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ElasticsearchSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);
  private volatile ElasticsearchWriter writer;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    try {
      log.info("Starting ElasticsearchSinkTask");

      ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);

      // Calculate the maximum possible backoff time ...
      long maxRetryBackoffMs =
          RetryUtil.computeRetryWaitTimeInMillis(config.maxRetries(), config.retryBackoffMs());
      if (maxRetryBackoffMs > RetryUtil.MAX_RETRY_TIME_MS) {
        log.warn("This connector uses exponential backoff with jitter for retries, "
                + "and using '{}={}' and '{}={}' results in an impractical but possible maximum "
                + "backoff time greater than {} hours.",
            ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG, config.maxRetries(),
            ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG, config.retryBackoffMs(),
            TimeUnit.MILLISECONDS.toHours(maxRetryBackoffMs));
      }

      ElasticsearchWriter.Builder builder = new ElasticsearchWriter.Builder(props)
          .setIgnoreKey(config.ignoreKey(), config.ignoreKeyTopics())
          .setIgnoreSchema(config.ignoreSchema(), config.ignoreSchemaTopics())
          .setCompactMapEntries(config.useCompactMapEntries())
          .setFlushTimoutMs(config.flushTimeoutMs())
          .setMaxBufferedRecords(config.maxBufferedRecords())
          .setMaxInFlightRequests(config.maxInFlightRequests())
          .setBatchSize(config.batchSize())
          .setLingerMs(config.lingerMs())
          .setRetryBackoffMs(config.retryBackoffMs())
          .setMaxRetry(config.maxRetries())
          .setDropInvalidMessage(config.dropInvalidMessage())
          .setBehaviorOnNullValues(config.behaviorOnNullValues())
          .setBehaviorOnMalformedDoc(config.behaviorOnMalformedDoc())
          .setIndexMapper(config.getIndexMapper())
          .setClusterMapper(config.getClusterMapper())
          .setTypeMapper(config.getTypeMapper())
          .setRouteMapper(config.getRouteMapper())
          .setParentMapper(config.getParentMapper());


      try {
        if (context.errantRecordReporter() == null) {
          log.info("Errant record reporter not configured.");
        }

        // may be null if DLQ not enabled
        builder.setErrantRecordReporter(context.errantRecordReporter());
      } catch (NoClassDefFoundError | NoSuchMethodError e) {
        // Will occur in Connect runtimes earlier than 2.6
        log.warn("AK versions prior to 2.6 do not support the errant record reporter");
      }

      writer = builder.build();
      log.info(
          "Started ElasticsearchSinkTask, will {} records with null values ('{}')",
          config.behaviorOnNullValues().name(),
          ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG
      );
    } catch (ConfigException e) {
      throw new ConnectException(
          "Couldn't start ElasticsearchSinkTask due to configuration error:",
          e
      );
    }
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    log.debug("Putting {} records to Elasticsearch", records.size());
    writer.write(records);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    if (writer != null) {
      log.debug("Flushing data to Elasticsearch with the following offsets: {}", offsets);
      writer.flush();
    } else {
      log.debug("Could not flush data to Elasticsearch because ESWriter already closed.");
    }
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    log.debug("Closing the task for topic partitions: {}", partitions);
  }

  @Override
  public void stop() throws ConnectException {
    log.info("Stopping ElasticsearchSinkTask");
    if (writer != null) {
      writer.stop();
      writer = null;
    }
  }
}
