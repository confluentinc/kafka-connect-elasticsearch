package io.confluent.connect.elasticsearch.integration;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HostnameVerificationDisabledIT extends SecurityIT {
    private static final Logger log = LoggerFactory.getLogger(HostnameVerificationDisabledIT.class);

    @Test
    public void testSecureConnectionHostnameVerificationDisabled() throws Throwable {
        // Use 'localhost' here that is not in self-signed cert
        String address = container.getConnectionUrl();
        address = address.replace(container.getContainerIpAddress(), "localhost");
        log.info("Creating connector for {}", address);

        connect.kafka().createTopic(KAFKA_TOPIC, 1);

        Map<String, String> props = getProps();
        props.put("connection.url", address);
        props.put("elastic.security.protocol", "SSL");
        props.put("elastic.https.ssl.keystore.location", container.getKeystorePath());
        props.put("elastic.https.ssl.keystore.password", container.getKeystorePassword());
        props.put("elastic.https.ssl.key.password", container.getKeyPassword());
        props.put("elastic.https.ssl.truststore.location", container.getTruststorePath());
        props.put("elastic.https.ssl.truststore.password", container.getTruststorePassword());

        // disable hostname verification
        props.put("elastic.https.ssl.endpoint.identification.algorithm", "");

        // Start connector
        testSecureConnection(props);
    }
}
