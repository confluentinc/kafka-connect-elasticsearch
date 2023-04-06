package io.confluent.connect.elasticsearch;

import org.apache.kafka.connect.sink.SinkRecord;

public interface SearchDataConverter {
    Object convertRecord(SinkRecord sinkRecord, String indexName);
}
