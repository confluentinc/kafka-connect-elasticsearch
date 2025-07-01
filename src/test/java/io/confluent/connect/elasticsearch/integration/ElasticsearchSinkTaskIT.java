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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.confluent.connect.elasticsearch.ElasticsearchSinkTaskConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector;
import io.confluent.connect.elasticsearch.ElasticsearchSinkTask;


import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_KEY_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.IGNORE_SCHEMA_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WRITE_METHOD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WriteMethod;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.*;
import static io.confluent.connect.elasticsearch.helper.ElasticSearchMockUtil.basicEmptyOk;
import static io.confluent.connect.elasticsearch.integration.ElasticsearchConnectorNetworkIT.errorBulkResponse;
import static java.util.stream.Collectors.toList;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
@Category(IntegrationTest.class)
public class ElasticsearchSinkTaskIT {

  @Parameterized.Parameters(name = "{index}: syncFlush={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {true}, {false}
    });
  }

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(options()
          .dynamicPort()
          .extensions(BlockingTransformer.class.getName()), false);

  protected static final String TOPIC = "test";
  protected static final int TASKS_MAX = 1;

  private final boolean synchronousFlush;

  public ElasticsearchSinkTaskIT(boolean synchronousFlush) {
    this.synchronousFlush = synchronousFlush;
  }

  // Convenience drop-in replacement for static import of WireMock.ok()
  private static ResponseDefinitionBuilder ok() {
    return basicEmptyOk();
  }

  @Before
  public void setup() {
    stubFor(any(anyUrl()).atPriority(10).willReturn(ok()));
  }

  @Test
  public void testOffsetCommit() {
    wireMockRule.stubFor(post(urlPathEqualTo("/"))
        .willReturn(ok().withFixedDelay(10_000)));
    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(ok().withFixedDelay(60_000)));

    Map<String, String> props = createProps();
    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(BATCH_SIZE_CONFIG, "1");

    ElasticsearchSinkTask task = new ElasticsearchSinkTask();

    TopicPartition tp = new TopicPartition(TOPIC, 0);

    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));
    task.initialize(context);
    task.start(props);

    List<SinkRecord> records = ImmutableList.of(
            sinkRecord(tp, 0),
            sinkRecord(tp, 1));
    task.put(records);

    // Nothing should be committed at this point
    Map<TopicPartition, OffsetAndMetadata> currentOffsets =
            ImmutableMap.of(tp, new OffsetAndMetadata(2));
    assertThat(task.preCommit(currentOffsets)).isEmpty();
  }

  @Test
  public void testIndividualFailure() throws JsonProcessingException {
    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(okJson(ElasticsearchConnectorNetworkIT.errorBulkResponse(3,
                    "strict_dynamic_mapping_exception", 1))));

    Map<String, String> props = createProps();
    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    props.put(BATCH_SIZE_CONFIG, "3");
    props.put(LINGER_MS_CONFIG, "10000");
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "ignore");

    TopicPartition tp = new TopicPartition(TOPIC, 0);

    final ElasticsearchSinkTask task = new ElasticsearchSinkTask();
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));

    task.initialize(context);
    task.start(props);
    List<SinkRecord> records = IntStream.range(0, 6).boxed()
            .map(offset -> sinkRecord(tp, offset))
            .collect(toList());
    task.put(records);

    // All is safe to commit
    Map<TopicPartition, OffsetAndMetadata> currentOffsets =
            ImmutableMap.of(tp, new OffsetAndMetadata(6));
    assertThat(task.preCommit(currentOffsets))
            .isEqualTo(currentOffsets);

    // Now check that we actually fail and offsets are not past the failed record
    props.put(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "fail");
    task.initialize(context);
    task.start(props);

    assertThatThrownBy(() -> task.put(records))
            .isInstanceOf(ConnectException.class)
            .hasMessageContaining("Indexing record failed");
    currentOffsets = ImmutableMap.of(tp, new OffsetAndMetadata(0));
    assertThat(getOffsetOrZero(task.preCommit(currentOffsets), tp))
            .isLessThanOrEqualTo(1);
  }

  private long getOffsetOrZero(Map<TopicPartition, OffsetAndMetadata> offsetMap, TopicPartition tp) {
    OffsetAndMetadata offsetAndMetadata = offsetMap.get(tp);
    return offsetAndMetadata == null ? 0 : offsetAndMetadata.offset();
  }

  @Test
  public void testConvertDataException() throws JsonProcessingException {
    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(okJson(errorBulkResponse(3))));

    Map<String, String> props = createProps();
    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    props.put(BATCH_SIZE_CONFIG, "10");
    props.put(LINGER_MS_CONFIG, "10000");
    props.put(IGNORE_KEY_CONFIG, "false");
    props.put(DROP_INVALID_MESSAGE_CONFIG, "true");

    TopicPartition tp = new TopicPartition(TOPIC, 0);

    final ElasticsearchSinkTask task = new ElasticsearchSinkTask();
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));

    task.initialize(context);
    task.start(props);

    List<SinkRecord> records = ImmutableList.of(
            sinkRecord(tp, 0),
            sinkRecord(tp, 1, null, "value"), // this should throw a DataException
            sinkRecord(tp, 2));
    task.put(records);

    // All is safe to commit
    Map<TopicPartition, OffsetAndMetadata> currentOffsets =
            ImmutableMap.of(tp, new OffsetAndMetadata(3));
    assertThat(task.preCommit(currentOffsets))
            .isEqualTo(currentOffsets);

    // Now check that we actually fail and offsets are not past the failed record
    props.put(DROP_INVALID_MESSAGE_CONFIG, "false");
    task.initialize(context);
    task.start(props);
    task.open(ImmutableList.of(tp));

    assertThatThrownBy(() -> task.put(records))
            .isInstanceOf(DataException.class)
            .hasMessageContaining("Key is used as document id and can not be null");

    currentOffsets = ImmutableMap.of(tp, new OffsetAndMetadata(0));
    assertThat(task.preCommit(currentOffsets).get(tp).offset())
            .isLessThanOrEqualTo(1);
  }

  @Test
  public void testNullValue() throws JsonProcessingException {
    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(okJson(errorBulkResponse(3))));

    Map<String, String> props = createProps();
    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    props.put(BATCH_SIZE_CONFIG, "10");
    props.put(LINGER_MS_CONFIG, "10000");
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, "ignore");

    TopicPartition tp = new TopicPartition(TOPIC, 0);

    final ElasticsearchSinkTask task = new ElasticsearchSinkTask();
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));
    task.initialize(context);
    task.start(props);

    List<SinkRecord> records = ImmutableList.of(
            sinkRecord(tp, 0),
            sinkRecord(tp, 1, "testKey", null),
            sinkRecord(tp, 2));
    task.put(records);

    // All is safe to commit
    Map<TopicPartition, OffsetAndMetadata> currentOffsets =
            ImmutableMap.of(tp, new OffsetAndMetadata(3));
    assertThat(task.preCommit(currentOffsets))
            .isEqualTo(currentOffsets);

    // Now check that we actually fail and offsets are not past the failed record
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, "fail");
    task.initialize(context);
    task.start(props);

    assertThatThrownBy(() -> task.put(records))
            .isInstanceOf(DataException.class)
            .hasMessageContaining("has a null value ");
    currentOffsets = ImmutableMap.of(tp, new OffsetAndMetadata(0));
    assertThat(task.preCommit(currentOffsets).get(tp).offset())
            .isLessThanOrEqualTo(1);
  }

  /**
   * Verify things are handled correctly when we receive the same records in a new put call
   * (e.g. after a RetriableException)
   */
  @Test
  public void testPutRetry() throws Exception {
    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .withRequestBody(WireMock.containing("{\"doc_num\":0}"))
            .willReturn(okJson(ElasticsearchConnectorNetworkIT.errorBulkResponse())));

    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .withRequestBody(WireMock.containing("{\"doc_num\":1}"))
            .willReturn(aResponse().withFixedDelay(60_000)));

    Map<String, String> props = createProps();
    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(BATCH_SIZE_CONFIG, "1");

    ElasticsearchSinkTask task = new ElasticsearchSinkTask();

    TopicPartition tp = new TopicPartition(TOPIC, 0);
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));
    task.initialize(context);
    task.start(props);

    List<SinkRecord> records = ImmutableList.of(
            sinkRecord(tp, 0),
            sinkRecord(tp, 1));
    task.open(ImmutableList.of(tp));
    task.put(records);

    Map<TopicPartition, OffsetAndMetadata> currentOffsets =
            ImmutableMap.of(tp, new OffsetAndMetadata(2));
    if (!synchronousFlush) {
      await().untilAsserted(() ->
              assertThat(task.preCommit(currentOffsets))
                      .isEqualTo(ImmutableMap.of(tp, new OffsetAndMetadata(1))));
    }

    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(okJson(ElasticsearchConnectorNetworkIT.errorBulkResponse())));

    task.put(records);
    await().untilAsserted(() ->
            assertThat(task.preCommit(currentOffsets))
                    .isEqualTo(currentOffsets));
  }

  /**
   * Verify partitions are paused and resumed
   */
  @Test
  public void testOffsetsBackpressure() throws Exception {
    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .willReturn(okJson(ElasticsearchConnectorNetworkIT.errorBulkResponse())));

    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .withRequestBody(WireMock.containing("{\"doc_num\":0}"))
            .willReturn(okJson(ElasticsearchConnectorNetworkIT.errorBulkResponse())
                    .withTransformers(BlockingTransformer.NAME)));

    Map<String, String> props = createProps();
    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(BATCH_SIZE_CONFIG, "1");
    props.put(MAX_BUFFERED_RECORDS_CONFIG, "2");

    ElasticsearchSinkTask task = new ElasticsearchSinkTask();

    TopicPartition tp1 = new TopicPartition(TOPIC, 0);

    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp1));
    task.initialize(context);
    task.start(props);
    task.open(ImmutableList.of(tp1));

    List<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      records.add(sinkRecord(tp1, i));
    }
    task.put(records);

    if (!synchronousFlush) {
      verify(context).pause(tp1);
    }

    BlockingTransformer.getInstance(wireMockRule).release(1);

    await().untilAsserted(() -> {
      task.put(Collections.emptyList());
      if (!synchronousFlush) {
        verify(context).resume(tp1);
      }
    });
  }

  /**
   * Verify offsets are updated when partitions are closed/open
   */
  @Test
  public void testRebalance() throws Exception {
    assumeFalse(synchronousFlush);

    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .withRequestBody(WireMock.containing("{\"doc_num\":0}"))
            .willReturn(okJson(ElasticsearchConnectorNetworkIT.errorBulkResponse())));

    wireMockRule.stubFor(post(urlPathEqualTo("/_bulk"))
            .withRequestBody(WireMock.containing("{\"doc_num\":1}"))
            .willReturn(aResponse().withFixedDelay(60_000)));

    Map<String, String> props = createProps();
    props.put(READ_TIMEOUT_MS_CONFIG, "1000");
    props.put(MAX_RETRIES_CONFIG, "2");
    props.put(RETRY_BACKOFF_MS_CONFIG, "10");
    props.put(BATCH_SIZE_CONFIG, "1");

    ElasticsearchSinkTask task = new ElasticsearchSinkTask();

    TopicPartition tp1 = new TopicPartition(TOPIC, 0);
    TopicPartition tp2 = new TopicPartition(TOPIC, 1);
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp1, tp2));
    task.initialize(context);
    task.start(props);

    List<SinkRecord> records = ImmutableList.of(
            sinkRecord(tp1, 0),
            sinkRecord(tp1, 1),
            sinkRecord(tp2, 0));
    task.put(records);

    Map<TopicPartition, OffsetAndMetadata> currentOffsets =
            ImmutableMap.of(
                    tp1, new OffsetAndMetadata(2),
                    tp2, new OffsetAndMetadata(1));
    await().untilAsserted(() ->
            assertThat(task.preCommit(currentOffsets))
                    .isEqualTo(ImmutableMap.of(tp1, new OffsetAndMetadata(1),
                            tp2, new OffsetAndMetadata(1))));

    task.close(ImmutableList.of(tp1));
    task.open(ImmutableList.of(new TopicPartition(TOPIC, 2)));
    when(context.assignment()).thenReturn(ImmutableSet.of(tp2));
    await().untilAsserted(() ->
            assertThat(task.preCommit(currentOffsets))
                    .isEqualTo(ImmutableMap.of(tp2, new OffsetAndMetadata(1))));
  }

  private SinkRecord sinkRecord(TopicPartition tp, long offset) {
    return sinkRecord(tp.topic(), tp.partition(), offset);
  }

  private SinkRecord sinkRecord(TopicPartition tp, long offset, String key, Object value) {
    return sinkRecord(tp.topic(), tp.partition(), offset, key, value);
  }

  private SinkRecord sinkRecord(String topic, int partition, long offset) {
    return sinkRecord(topic, partition, offset, "testKey", ImmutableMap.of("doc_num", offset));
  }

  private SinkRecord sinkRecord(String topic, int partition, long offset, String key, Object value) {
    return new SinkRecord(topic,
            partition,
            null,
            key,
            null,
            value,
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
    props.put(WRITE_METHOD_CONFIG, WriteMethod.UPSERT.toString());
    props.put(FLUSH_SYNCHRONOUSLY_CONFIG, Boolean.toString(synchronousFlush));
    props.put(ElasticsearchSinkTaskConfig.TASK_ID_CONFIG, "1");

    return props;
  }
}
