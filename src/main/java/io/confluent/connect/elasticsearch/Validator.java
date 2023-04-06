package io.confluent.connect.elasticsearch;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;

import java.util.LinkedList;
import java.util.List;

public class Validator {

    public Config validate() {
        return new Config(new LinkedList<>());
    }
}
