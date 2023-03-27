package io.confluent.connect.elasticsearch;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;

public class SearchClient{

    public String type;
    public Object version() {
        return null;
    }

    public void throwIfFailed() {
    }

    public void flush() {
    }

    public void close() {
    }

    public boolean hasMapping(String index) {
        return false;
    }

    public void createMapping(String index, Schema valueSchema) {
    }

    public void waitForInFlightRequests() {
    }

    public boolean isFailed() {
        return false;
    }

    public boolean createIndexOrDataStream(String index) {
        return false;
    }

    public void index(SinkRecord sinkRecord, DocWriteRequest<?> docWriteRequest, OffsetState offsetState) {
    }
}
