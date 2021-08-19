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

package io.confluent.connect.elasticsearch.integration;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector;
import io.confluent.connect.elasticsearch.ElasticsearchSinkTask;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.*;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

public class ElasticsearchSinkTaskIT {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort(), false);

  protected static final String TOPIC = "test";
  protected static final int TASKS_MAX = 1;

  @Before
  public void setup() {
    stubFor(any(anyUrl()).atPriority(10).willReturn(ok()));
  }

  @Test
  public void testOffsetCommit() {
    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(ok().withFixedDelay(60_000)));

    Map<String, String> props = createProps();
    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(BATCH_SIZE_CONFIG, "1");

    ElasticsearchSinkTask task = new ElasticsearchSinkTask();

    SinkTaskContext context = Mockito.mock(SinkTaskContext.class);
    task.initialize(context);
    task.start(props);

    TopicPartition tp = new TopicPartition(TOPIC, 0);
    List<SinkRecord> records = ImmutableList.of(
            sinkRecord(tp, 0),
            sinkRecord(tp, 1));
    task.put(records);

    // Nothing should be committed at this point
    Map<TopicPartition, OffsetAndMetadata> currentOffsets =
            ImmutableMap.of(tp, new OffsetAndMetadata(2));
    assertThat(task.preCommit(currentOffsets)).isEmpty();
  }

  private SinkRecord sinkRecord(TopicPartition tp, long offset) {
    return sinkRecord(tp.topic(), tp.partition(), offset);
  }

  private SinkRecord sinkRecord(String topic, int partition, long offset) {
    return new SinkRecord(topic,
            partition,
            null,
            "testKey",
            null,
            "testValue" + offset,
            offset);
  }

  protected Map<String, String> createProps() {
    Map<String, String> props = new HashMap<>();

    // generic configs
    props.put(CONNECTOR_CLASS_CONFIG, ElasticsearchSinkConnector.class.getName());
    props.put(TOPICS_CONFIG, TOPIC);
    props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put("value.converter." + SCHEMAS_ENABLE_CONFIG, "false");

    // connector specific
    props.put(CONNECTION_URL_CONFIG, wireMockRule.url("/"));
    props.put(IGNORE_KEY_CONFIG, "true");
    props.put(IGNORE_SCHEMA_CONFIG, "true");

    return props;
  }
}
