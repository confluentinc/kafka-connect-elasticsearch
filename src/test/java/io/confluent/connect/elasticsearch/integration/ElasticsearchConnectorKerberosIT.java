package io.confluent.connect.elasticsearch.integration;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.KEYTAB_FILE_PATH_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.USER_PRINCIPAL_CONFIG;

import io.confluent.connect.elasticsearch.ElasticsearchClient;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import io.confluent.connect.elasticsearch.helper.ElasticsearchHelperClient;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ElasticsearchConnectorKerberosIT extends ElasticsearchConnectorBaseIT {

  private static File baseDir;
  private static MiniKdc kdc;
  private static String esPrincipal;
  private static String esKeytab;
  private static String userPrincipal;
  private static String userKeytab;

  @BeforeClass
  public static void setupBeforeAll() throws Exception {
    initKdc();

    container = ElasticsearchContainer.fromSystemProperties().withKerberosEnabled(esKeytab);
    container.start();
  }

  /**
   * Shuts down the KDC and cleans up files.
   */
  @AfterClass
  public static void cleanupAfterAll() {
    container.close();
    closeKdc();
  }

  @Test
  public void testKerberos() throws Exception {
    addKerberosConfigs(props);
    helperClient = new ElasticsearchHelperClient(new ElasticsearchClient(new ElasticsearchSinkConnectorConfig(props), null).client());
    runSimpleTest(props);
  }

  private static void closeKdc() {
    if (kdc != null) {
      kdc.stop();
    }

    if (baseDir.exists()) {
      deleteDirectory(baseDir.toPath());
    }
  }

  private static void deleteDirectory(Path directoryPath) {
    try {
      Files.walk(directoryPath)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  private static void initKdc() throws Exception {
    baseDir = new File(System.getProperty("test.build.dir", "target/test-dir"));
    if (baseDir.exists()) {
      deleteDirectory(baseDir.toPath());
    }

    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();

    String es = "es";
    File keytabFile = new File(baseDir, es + ".keytab");
    esKeytab = keytabFile.getAbsolutePath();
    kdc.createPrincipal(keytabFile, es + "/localhost", "HTTP/localhost");
    esPrincipal = es + "/localhost@" + kdc.getRealm();

    String user = "connect-es";
    keytabFile = new File(baseDir, user + ".keytab");
    userKeytab = keytabFile.getAbsolutePath();
    kdc.createPrincipal(keytabFile, user + "/localhost");
    userPrincipal = user + "/localhost@" + kdc.getRealm();
  }

  private static void addKerberosConfigs(Map<String, String> props) {
    props.put(USER_PRINCIPAL_CONFIG, userPrincipal);
    props.put(KEYTAB_FILE_PATH_CONFIG, userKeytab);
  }
}
