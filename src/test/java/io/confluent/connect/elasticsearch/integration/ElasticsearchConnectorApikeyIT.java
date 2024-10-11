package io.confluent.connect.elasticsearch.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.Role;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_APIKEY_CONFIG;

@Category(IntegrationTest.class)
public class ElasticsearchConnectorApikeyIT extends ElasticsearchConnectorBaseIT {

  @BeforeClass
  public static void setupBeforeAll() throws Exception {
    Map<User, String> users = getUsers();
    List<Role> roles = getRoles();
    container = ElasticsearchContainer.fromSystemProperties().withApikey(users, roles);
    container.start();
  }

  @AfterClass
  public static void cleanupAfterAll() {
    container.close();
  }

  @Test
  public void testApikey() throws Exception {
    addApikeyConfigConfigs(props);
    helperClient = container.getHelperClient(props);
    helperClient.waitForConnection(60000);
    runSimpleTest(props);
  }

  protected static void addApikeyConfigConfigs(Map<String, String> props) {
    String apikey = container.getAPIkeys().get(ELASTIC_MINIMAL_PRIVILEGES_NAME);
    props.put(CONNECTION_APIKEY_CONFIG, apikey);
  }
}
