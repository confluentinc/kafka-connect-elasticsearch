package io.confluent.connect.elasticsearch;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchClient{

    public String type;
    private static final Logger log = LoggerFactory.getLogger(ElasticsearchClient.class);
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

    public void index(SinkRecord sinkRecord, Object docWriteRequest, OffsetState offsetState) {
        log.info("I am in search client....");
    }
}
