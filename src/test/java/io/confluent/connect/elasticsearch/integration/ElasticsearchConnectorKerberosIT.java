package io.confluent.connect.elasticsearch.integration;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.KERBEROS_KEYTAB_PATH_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.KERBEROS_PRINCIPAL_CONFIG;

import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.kafka.connect.errors.ConnectException;
import io.confluent.common.utils.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ElasticsearchConnectorKerberosIT extends ElasticsearchConnectorBaseIT {

  private static File baseDir;
  private static MiniKdc kdc;
  protected static String esKeytab;
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
    helperClient = container.getHelperClient(props);
    helperClient.waitForConnection(60000);
    runSimpleTest(props);
  }

  protected static void initKdc() throws Exception {
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

    String user = "connect-es";
    keytabFile = new File(baseDir, user + ".keytab");
    userKeytab = keytabFile.getAbsolutePath();
    kdc.createPrincipal(keytabFile, user + "/localhost");
    userPrincipal = user + "/localhost@" + kdc.getRealm();
  }

  protected static void addKerberosConfigs(Map<String, String> props) {
    props.put(KERBEROS_PRINCIPAL_CONFIG, userPrincipal);
    props.put(KERBEROS_KEYTAB_PATH_CONFIG, userKeytab);
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
}
