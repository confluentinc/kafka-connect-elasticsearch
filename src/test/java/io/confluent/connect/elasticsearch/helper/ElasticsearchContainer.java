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

package io.confluent.connect.elasticsearch.helper;

import io.confluent.connect.elasticsearch.ElasticsearchClient;
import io.confluent.connect.elasticsearch.RetryUtil;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.test.TestUtils;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.dockerfile.DockerfileBuilder;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SecurityProtocol;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SECURITY_PROTOCOL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.SSL_CONFIG_PREFIX;

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
  public static final String DEFAULT_ES_VERSION = "8.15.2";

  /**
   * Default Elasticsearch port.
   */
  public static final int ELASTICSEARCH_DEFAULT_PORT = 9200;

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

  public static ElasticsearchContainer withESVersion(String ESVersion) {
    String imageName = getSystemOrEnvProperty(
        "elasticsearch.image",
        "ELASTICSEARCH_IMAGE",
        DEFAULT_DOCKER_IMAGE_NAME
    );
    return new ElasticsearchContainer(imageName + ":" + ESVersion);
  }

  private static final String KEY_PASSWORD = "asdfasdf";
  // Super user that has superuser role. Should not be used by connector
  private static final String ELASTIC_SUPERUSER_NAME = "elastic";
  private static final String ELASTIC_SUPERUSER_PASSWORD = "elastic";

  private static final String KEYSTORE_PASSWORD = KEY_PASSWORD;
  private static final String TRUSTSTORE_PASSWORD = KEY_PASSWORD;
  private static final long TWO_GIGABYTES = 2L * 1024 * 1024 * 1024;

  private final String imageName;
  private boolean enableSsl = false;
  private String keytabPath;
  private List<Role> rolesToCreate;
  private Map<User, String> usersToCreate;
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

  @Override
  public void start() {
    super.start();

    String address;
    if (isBasicAuthEnabled()) {
      Map<String, String> props = new HashMap<>();
      props.put(CONNECTION_USERNAME_CONFIG, ELASTIC_SUPERUSER_NAME);
      props.put(CONNECTION_PASSWORD_CONFIG, ELASTIC_SUPERUSER_PASSWORD);
      if (isSslEnabled()) {
        addSslProps(props);
        address = this.getConnectionUrl(false);
      } else {
        address = this.getConnectionUrl();
      }
      props.put(CONNECTION_URL_CONFIG, address);
      ElasticsearchHelperClient helperClient = getHelperClient(props);
      helperClient.waitForConnection(30000);
      createUsersAndRoles(helperClient);
    }
  }

  public void addSslProps(Map<String, String> props) {
    props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, this.getKeystorePath());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, this.getKeystorePassword());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, this.getTruststorePath());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, this.getTruststorePassword());
    props.put(SSL_CONFIG_PREFIX + SslConfigs.SSL_KEY_PASSWORD_CONFIG, this.getKeyPassword());
  }

  private void createUsersAndRoles(ElasticsearchHelperClient helperClient ) {
    try {
      for (Role role: this.rolesToCreate) {
        helperClient.createRole(role);
      }
      for (Map.Entry<User,String> userToPassword: this.usersToCreate.entrySet()) {
        helperClient.createUser(userToPassword);
      }
    } catch (IOException e) {
      throw new ContainerLaunchException("Container startup failed", e);
    }
  }

  public ElasticsearchContainer withSslEnabled(boolean enable) {
    enableSsl(enable);
    return this;
  }

  public ElasticsearchContainer withKerberosEnabled(String keytab) {
    enableKerberos(keytab);
    return this;
  }

  public ElasticsearchContainer withBasicAuth(Map<User, String> users, List<Role> roles) {
    enableBasicAuth(users, roles);
    return this;
  }

  /**
   * Set whether the Elasticsearch instance should use SSL.
   *
   * <p>This can only be called <em>before</em> the container is started.
   *
   * @param enable true if SSL is to be enabled, or false otherwise
   */
  public void enableSsl(boolean enable) {
    if (isCreated()) {
      throw new IllegalStateException(
          "enableSsl can only be used before the Container is created."
      );
    }
    enableSsl = enable;
  }

  /**
   * Get whether the Elasticsearch instance is configured to use SSL.
   *
   * @return true if SSL is enabled, or false otherwise
   */
  public boolean isSslEnabled() {
    return enableSsl;
  }

  /**
   * Set whether the Elasticsearch instance should use Kerberos.
   *
   * <p>This can only be called <em>before</em> the container is started.
   *
   * @param keytab non-null keytab path if Kerberos is enabled
   */
  public void enableKerberos(String keytab) {
    if (isCreated()) {
      throw new IllegalStateException(
          "enableKerberos can only be used before the container is created."
      );
    }
    if (isBasicAuthEnabled()) {
      throw new IllegalStateException(
          "basic auth and Kerberos are mutually exclusive."
      );
    }
    keytabPath = keytab;
  }

  /**
   * Get whether the Elasticsearch instance is configured to use Kerberos.
   *
   * @return true if Kerberos is enabled, or false otherwise
   */
  public boolean isKerberosEnabled() {
    return keytabPath != null;
  }

  private void enableBasicAuth(Map<User, String> users, List<Role> roles) {
    if (isCreated()) {
      throw new IllegalStateException(
          "enableBasicAuth can only be used before the container is created."
      );
    }
    if (isKerberosEnabled()) {
      throw new IllegalStateException(
          "basic auth and Kerberos are mutually exclusive."
      );
    }
    this.usersToCreate = users;
    this.rolesToCreate = roles;
  }

  public boolean isBasicAuthEnabled() {
    return usersToCreate != null && !this.usersToCreate.isEmpty();
  }

  private String getFullResourcePath(String resourceName) {
    if (isSslEnabled() && isKerberosEnabled()) {
      return "/both/" + resourceName;
    } else if (isSslEnabled()) {
      return "/ssl/" + resourceName;
    } else if (isKerberosEnabled()) {
      return "/kerberos/" + resourceName;
    } else if (isBasicAuthEnabled()) {
      return "/basic/" + resourceName;
    } else {
      return "/none/" + resourceName;
    }
  }

  @Override
  protected void configure() {
    super.configure();

    waitingFor(
        Wait.forLogMessage(".*(Security is enabled|license .* valid).*", 1)
            .withStartupTimeout(Duration.ofMinutes(5))
    );

    ImageFromDockerfile image = new ImageFromDockerfile()
        // Copy the Elasticsearch config file
        .withFileFromClasspath("elasticsearch.yml", getFullResourcePath("elasticsearch.yml"))
        // Copy the network definitions
        .withFileFromClasspath("instances.yml", getFullResourcePath("instances.yml"))
        .withDockerfileFromBuilder(this::buildImage);

    // Kerberos and basic auth are mutually exclusive authentication options
    if (isBasicAuthEnabled()) {
      log.info("Setting up basic authentication in a Docker image");
      withEnv("ELASTICSEARCH_USERNAME", ELASTIC_SUPERUSER_NAME);
      withEnv("ELASTIC_PASSWORD", ELASTIC_SUPERUSER_PASSWORD);
    } else if (isKerberosEnabled()) {
      log.info("Creating Kerberized Elasticsearch image.");
      image.withFileFromFile("es.keytab", new File(keytabPath));
    }
    if (isSslEnabled()) {
      log.info("Extending Docker image to generate certs and enable SSL");
      withEnv("ELASTIC_PASSWORD", ELASTIC_SUPERUSER_PASSWORD);
      withEnv("STORE_PASSWORD", KEY_PASSWORD);
      withEnv("IP_ADDRESS", hostMachineIpAddress());

      image
          // Copy the script to generate the certs and start Elasticsearch
          .withFileFromClasspath("start-elasticsearch.sh",
              getFullResourcePath("start-elasticsearch.sh"));
    }
    setImage(image);
  }

  private ArrayList<Integer> getImageVersion() {
    ArrayList<Integer> versions = new ArrayList<>(3);
    String[] tokens = imageName.split(":");
    String versionString = tokens[tokens.length - 1];
    String[] versionTokens = versionString.split("\\.");
    for (String v : versionTokens) {
      try {
        versions.add(Integer.parseInt(v));
      } catch (Exception e) {
        // Maybe ends with garbage like -rc7894237
        versions.add(Integer.parseInt(v.substring(0, v.indexOf('-'))));
      }
    }
    // Pad with zeros to at least three positions
    while (versions.size() < 3) {
      versions.add(0);
    }
    return versions;
  }

  private void buildImage(DockerfileBuilder builder) {
    builder
        .from(imageName)
        // Copy the Elasticsearch configuration
        .copy("elasticsearch.yml", CONFIG_PATH + "/elasticsearch.yml");

    if (isSslEnabled()) {
      ArrayList<Integer> versionsInt = getImageVersion();
      log.info("Building Elasticsearch image with SSL configuration");
      builder
          .user("root")
          .copy("instances.yml", CONFIG_SSL_PATH + "/instances.yml")
          .copy("start-elasticsearch.sh", CONFIG_SSL_PATH + "/start-elasticsearch.sh");
      if (versionsInt.get(0) == 8 || (versionsInt.get(0) == 7 && versionsInt.get(1) >= 15)) {
        // Install keytool from java 1.8 since our connector is built with
        // java 1.8 and the cert algoritm's won;t be compatible when using the newer
        // java version on the container
        // Also note that 7.16.3 test container uses Ubuntu now instead of CentOS
        builder.run("apt update");
        builder.run("apt install -y openjdk-8-jre-headless");
      }
      builder
          .run("chmod +x " + CONFIG_SSL_PATH + "/start-elasticsearch.sh")
          .entryPoint(CONFIG_SSL_PATH + "/start-elasticsearch.sh");
    }

    if (isKerberosEnabled()) {
      log.info("Building Elasticsearch image with Kerberos configuration.");
      builder.copy("es.keytab", CONFIG_PATH + "/es.keytab");
      if (!isSslEnabled()) {
        builder.copy("instances.yml", CONFIG_PATH + "/instances.yml");
      }
    }
  }

  public String hostMachineIpAddress() {
    String dockerHost = System.getenv("DOCKER_HOST");
    if (dockerHost != null && !dockerHost.trim().isEmpty()) {
      try {
        URI url = new URI(dockerHost);
        dockerHost = url.getHost();
        log.info("Including DOCKER_HOST address {} in Elasticsearch certs", dockerHost);
        return dockerHost;
      } catch (URISyntaxException e) {
        log.info("DOCKER_HOST={} could not be parsed into a URL: {}", dockerHost, e.getMessage(), e);
      }
    }
    try {
      String hostAddress = InetAddress.getLocalHost().getHostAddress();
      log.info("Including test machine address {} in Elasticsearch certs", hostAddress);
      return hostAddress;
    } catch (IOException e) {
      return "";
    }
  }

  /**
   * @see ElasticsearchContainer#getConnectionUrl(boolean)
   */
  public String getConnectionUrl() {
    return getConnectionUrl(true);
  }

  /**
   * Get the Elasticsearch connection URL.
   *
   * <p>This can only be called once the container is started.
   *
   * @param useContainerIpAddress use container IP if true, host machine's IP otherwise
   *
   * @return the connection URL; never null
   */
  public String getConnectionUrl(boolean useContainerIpAddress) {
    String protocol = isSslEnabled() ? "https" : "http";
    return String.format(
        "%s://%s:%d",
        protocol,
        useContainerIpAddress ? getContainerIpAddress() : hostMachineIpAddress(),
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

  public ElasticsearchHelperClient getHelperClient(Map<String, String> props) {
    // copy properties so that original properties are not affected
    Map<String, String> superUserProps = new HashMap<>(props);
    superUserProps.put(CONNECTION_USERNAME_CONFIG, ELASTIC_SUPERUSER_NAME);
    superUserProps.put(CONNECTION_PASSWORD_CONFIG, ELASTIC_SUPERUSER_PASSWORD);
    ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(superUserProps);
    ElasticsearchHelperClient client = new ElasticsearchHelperClient(props.get(CONNECTION_URL_CONFIG), config,
        shouldStartClientInCompatibilityMode());
    return client;
  }

  /**
   * For high level rest client v7.17 api compatibility mode must be turned on for working with
   * ES 8.
   * @return true if the major version of image used is 8 i.e (ES 8.x.x)
   */
  public boolean shouldStartClientInCompatibilityMode() {
    return esMajorVersion() == 8;
  }

  public int esMajorVersion() {
    return getImageVersion().get(0);
  }
}
