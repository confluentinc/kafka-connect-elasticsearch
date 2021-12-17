/**
 * Copyright 2016 Confluent Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.elasticsearch.producer;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import java.util.List;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.connector.ConnectRecord;

import java.util.Properties;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.sink.SinkRecord;

public class DualWriteProducer<T extends ConnectRecord> {
  private Producer<Object, Object> passthroughProducer;
  private String outputTopic;
  private final boolean isEnabled;
  private final AvroData avroData;

  public DualWriteProducer(boolean enabled, Properties config, String outputTopic) {
    isEnabled = enabled;
    AvroDataConfig.Builder builder =
        new AvroDataConfig.Builder().with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false);
    this.avroData = new AvroData(builder.build());

    if (isEnabled) {
      this.passthroughProducer = new KafkaProducer<>(config);
      this.outputTopic = outputTopic;
    }
  }

  private ProducerRecord<Object, Object> convertSinkToProducerRecord(SinkRecord record) {
    Object value = null;
    if (record.valueSchema().type() != Type.STRUCT) {
      value = record.value();
    } else {
      value = avroData.fromConnectData(record.valueSchema(), record.value());
    }

    Object key = null;
    if (record.keySchema().type() != Type.STRUCT) {
      key = record.key();
    } else {
      key = avroData.fromConnectData(record.keySchema(), record.key());
    }

    return new ProducerRecord<>(outputTopic, null, record.timestamp(), key, value);
  }

  public void submit(SinkRecord record) {
    if (isEnabled) {
      passthroughProducer.send(convertSinkToProducerRecord(record));
    }
  }

  public void submitAll(List<SinkRecord> records) {
    if (isEnabled) {
      records.stream().forEach(sinkRecord ->
          passthroughProducer.send(convertSinkToProducerRecord(sinkRecord)));
    }
  }
}
