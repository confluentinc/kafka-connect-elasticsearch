/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package io.confluent.connect.elasticsearch.integration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.Future;

import com.github.dockerjava.api.command.BuildImageCmd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.dockerfile.DockerfileBuilder;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

/**
 * A specialized TestContainer container for testing Elasticsearch, optionally with SSL support.
 */
public class ElasticsearchContainer
    extends org.testcontainers.elasticsearch.ElasticsearchContainer {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchContainer.class);

  /**
   * Default Elasticsearch Docker image name.
   */
  public static final String DEFAULT_DOCKER_IMAGE_NAME =
      "docker.elastic.co/elasticsearch/elasticsearch";

  /**
   * Default Elasticsearch version.
   */
  public static final String DEFAULT_ES_VERSION = "7.0.0";

  /**
   * Default Elasticsearch port.
   */
  public static final int ELASTICSEARCH_DEFAULT_PORT = 9200;

  /**
   * The hostname to use for the Elasticsearch container. This is fixed because the certificates
   * for SSL are generated with this hostname.
   */
  public static final String ELASTICSEARCH_HOST_NAME = "elasticsearch";

  /**
   * Path to the Elasticsearch configuration directory.
   */
  public static String CONFIG_PATH = "/usr/share/elasticsearch/config";

  /**
   * Path to the directory for the certificates and keystores.
   */
  public static String CONFIG_SSL_PATH = CONFIG_PATH + "/ssl";

  /**
   * Path to the Java keystore in the container.
   */
  public static String KEYSTORE_PATH = CONFIG_SSL_PATH + "/keystore.jks";

  /**
   * Path to the Java truststore in the container.
   */
  public static String TRUSTSTORE_PATH = CONFIG_SSL_PATH + "/truststore.jks";

  /**
   * Create an {@link ElasticsearchContainer} using the image name specified in the
   * {@code elasticsearch.image} system property or {@code ELASTICSEARCH_IMAGE} environment
   * variable, or defaulting to {@link #DEFAULT_DOCKER_IMAGE_NAME}, and the version specified in
   * the {@code elasticsearch.version} system property, {@code ELASTICSEARCH_VERSION} environment
   * variable, or defaulting to {@link #DEFAULT_ES_VERSION}.
   *
   * @return the unstarted container; never null
   */
  public static ElasticsearchContainer fromSystemProperties() {
    String imageName = getSystemOrEnvProperty(
        "elasticsearch.image",
        "ELASTICSEARCH_IMAGE",
        DEFAULT_DOCKER_IMAGE_NAME
    );
    String version = getSystemOrEnvProperty(
        "elasticsearch.version",
        "ELASTICSEARCH_VERSION",
        DEFAULT_ES_VERSION
    );
    return new ElasticsearchContainer(imageName + ":" + version);
  }

  private static final String KEY_PASSWORD = "asdfasdf";
  private static final String ELASTIC_PASSWORD = "elastic";
  private static final String KEYSTORE_PASSWORD = KEY_PASSWORD;
  private static final String TRUSTSTORE_PASSWORD = KEY_PASSWORD;
  private static final long TWO_GIGABYTES = 2L * 1024 * 1024 * 1024;

  private final String imageName;
  private boolean enableSsl;
  private String localKeystorePath;
  private String localTruststorePath;

  /**
   * Create an Elasticsearch container with the given image name with version qualifier.
   *
   * @param imageName the image name
   */
  public ElasticsearchContainer(String imageName) {
    super(imageName);
    this.imageName = imageName;
    withSharedMemorySize(TWO_GIGABYTES);
    withLogConsumer(this::containerLog);
  }

  public ElasticsearchContainer withSslEnabled(boolean enable) {
    setSslEnabled(enable);
    return this;
  }

  public void setSslEnabled(boolean enable) {
    enableSsl = enable;
    Future<String> image;
    if (enable) {
      // Because this is an secured Elasticsearch instance, we can't use HTTPS checks
      // because of the untrusted cert
      waitingFor(
          Wait.forLogMessage(".*(Security is enabled|license .* valid).*", 1)
              .withStartupTimeout(Duration.ofMinutes(5))
      );
      image = new ImageFromDockerfile()
          // Copy the Elasticsearch config file into the builder's context
          .withFileFromClasspath(
              "elasticsearch.yml",
              "/ssl/elasticsearch.yml"
          )
          // Copy the script to generate the certs into the builder's context
          .withFileFromClasspath(
              "instances.yml",
              "/ssl/instances.yml"
          )
          // Copy the script to generate the certs into the builder's context
          .withFileFromClasspath(
              "setup-elasticsearch.sh",
              "/ssl/setup-elasticsearch.sh"
          )
          .withDockerfileFromBuilder(this::build);
    } else {
      // Because this is an unsecured Elasticsearch instance, we can use HTTP checks
      waitingFor(
          Wait.forHttp("/")
              .forPort(ELASTICSEARCH_DEFAULT_PORT)
              .forStatusCodeMatching(status -> status == HTTP_OK || status == HTTP_UNAUTHORIZED)
              .withStartupTimeout(Duration.ofMinutes(2))
      );
      image = new RemoteDockerImage(imageName);
    }
    setImage(image);
  }

  public boolean isSslEnabled() {
    return enableSsl;
  }

  protected void build(DockerfileBuilder builder) {
    log.info("Building Elasticsearch image with SSL config file and generating certs");
    builder.from(imageName)
           .env("ELASTIC_PASSWORD", ELASTIC_PASSWORD)
           .env("STORE_PASSWORD", KEY_PASSWORD)
           // OpenSSL and Java's Keytool used to generate the certs, so install them
           .run("yum -y install openssl")
           // Copy the Elasticsearch configuration
           .copy("elasticsearch.yml", CONFIG_PATH +"/elasticsearch.yml")
           // Copy and run the script to generate the certs
           .copy("instances.yml", CONFIG_SSL_PATH + "/instances.yml")
           .copy("setup-elasticsearch.sh", CONFIG_SSL_PATH + "/setup-elasticsearch.sh")
           .run(CONFIG_SSL_PATH + "/setup-elasticsearch.sh");
  }

  /**
   * Get the Elasticsearch connection URL.
   *
   * <p>This can only be called once the container is started.
   *
   * @return the connection URL; never null
   */
  public String getConnectionUrl() {
    String protocol = isSslEnabled() ? "https" : "http";
    return String.format(
        "%s://%s:%d",
        protocol,
        getContainerIpAddress(),
        getMappedPort(ELASTICSEARCH_DEFAULT_PORT)
    );
  }

  /**
   * Get the {@link #getKeystorePath() Keystore} password.
   *
   * <p>This can only be called once the container is started.
   *
   * @return the password for the keystore; may be null if
   *         {@link #isSslEnabled() SSL is not enabled}
   */
  public String getKeystorePassword() {
    if (!isCreated()) {
      throw new IllegalStateException("getKeystorePassword can only be used when the Container is created.");
    }
    return isSslEnabled() ? KEYSTORE_PASSWORD : null;
  }

  /**
   * Get the certificate key password.
   *
   * <p>This can only be called once the container is started.
   *
   * @return the password for the keystore; may be null if
   *         {@link #isSslEnabled() SSL is not enabled}
   */
  public String getKeyPassword() {
    if (!isCreated()) {
      throw new IllegalStateException("getKeyPassword can only be used when the Container is created.");
    }
    return isSslEnabled() ? KEY_PASSWORD : null;
  }

  /**
   * Get the {@link #getKeystorePath() Keystore} password.
   *
   * <p>This can only be called once the container is started.
   *
   * @return the password for the keystore; may be null if
   *         {@link #isSslEnabled() SSL is not enabled}
   */
  public String getTruststorePassword() {
    if (!isCreated()) {
      throw new IllegalStateException("getTruststorePassword can only be used when the Container is created.");
    }
    return isSslEnabled() ? TRUSTSTORE_PASSWORD : null;
  }

  /**
   * Create a local temporary copy of the keystore generated by the Elasticsearch container and
   * used by Elasticsearch, and return the path to the file.
   *
   * <p>This method will always return the same path once the container is created.
   *
   * @return the path to the local keystore temporary file, or null if
   *         {@link #isSslEnabled() SSL is not used}
   */
  public String getKeystorePath() {
    if (!isCreated()) {
      throw new IllegalStateException("getKeystorePath can only be used when the Container is created.");
    }
    if (isSslEnabled() && localKeystorePath == null) {
      localKeystorePath = copyFileFromContainer(KEYSTORE_PATH, this::generateTemporaryFile);
    }
    return localKeystorePath;
  }

  /**
   * Create a local temporary copy of the truststore generated by the Elasticsearch container and
   * used by Elasticsearch, and return the path to the file.
   *
   * <p>This method will always return the same path once the container is created.
   *
   * @return the path to the local truststore temporary file, or null if
   *         {@link #isSslEnabled() SSL is not used}
   */
  public String getTruststorePath() {
    if (!isCreated()) {
      throw new IllegalStateException("getTruststorePath can only be used when the Container is created.");
    }
    if (isSslEnabled() && localTruststorePath == null) {
      localTruststorePath = copyFileFromContainer(TRUSTSTORE_PATH, this::generateTemporaryFile);
    }
    return localTruststorePath;
  }

  protected String generateTemporaryFile(InputStream inputStream) throws IOException {
    File file = File.createTempFile("ElasticsearchTestContainer", "jks");
    try (FileOutputStream outputStream = new FileOutputStream(file)) {
      IOUtils.copy(inputStream, outputStream);
    }
    return file.getAbsolutePath();
  }

  private static String getSystemOrEnvProperty(String sysPropName, String envPropName, String defaultValue) {
    String propertyValue = System.getProperty(sysPropName);
    if (null == propertyValue) {
      propertyValue = System.getenv(envPropName);
      if (null == propertyValue) {
        propertyValue = defaultValue;
      }
    }
    return propertyValue;
  }

  /**
   * Capture the container log by writing the container's standard output
   * to {@link System#out} (in yellow) and standard error to {@link System#err} (in red).
   *
   * @param logMessage the container log message
   */
  protected void containerLog(OutputFrame logMessage) {
    switch (logMessage.getType()) {
      case STDOUT:
        // Normal output in yellow
        System.out.print((char)27 + "[33m" + logMessage.getUtf8String());
        System.out.print((char)27 + "[0m"); // reset
        break;
      case STDERR:
        // Error output in red
        System.err.print((char)27 + "[31m" + logMessage.getUtf8String());
        System.out.print((char)27 + "[0m"); // reset
        break;
      case END:
        // End output in green
        System.err.print((char)27 + "[32m" + logMessage.getUtf8String());
        System.out.print((char)27 + "[0m"); // reset
        break;
      default:
        break;
    }
  }
}
