package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ALLOW_CLUSTER_WIPE;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_DELAY_FACTOR;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.authentication.AuthenticationService;
import com.linkedin.venice.authentication.jwt.ClientAuthenticationProviderToken;
import com.linkedin.venice.authentication.jwt.TokenAuthenticationService;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.authorization.SimpleAuthorizerService;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestJWTAuthenticationEndToEnd {
  private static final int TEST_TIMEOUT = 30 * Time.MS_PER_SECOND;

  String clusterName;
  VeniceClusterWrapper venice;

  @BeforeClass
  public void setUp() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(LOCAL_REGION_NAME, "dc-0");
    properties.setProperty(ALLOW_CLUSTER_WIPE, "true");
    properties.setProperty(TOPIC_CLEANUP_DELAY_FACTOR, "0");
    AuthenticationService authenticationService = new TokenAuthenticationService();
    Properties authProperties = new Properties();
    authProperties.setProperty("authentication.service.class", TokenAuthenticationService.class.getName());
    authProperties.setProperty("authentication.jwt.secretKey", "jDdra78Vo1+RVMGY2easnWe0sAFrEa2581ra5YMotbE=");
    authenticationService.initialise(new VeniceProperties(authProperties));

    AuthorizerService authorizerService = new SimpleAuthorizerService();

    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(0)
        .numberOfRouters(0)
        .replicationFactor(1)
        .partitionSize(1)
        .minActiveReplica(1)
        .authenticationService(authenticationService)
        .authorizerService(authorizerService)
        .build();

    venice = ServiceFactory.getVeniceCluster(options);
    clusterName = venice.getClusterName();
  }

  @AfterClass
  public void cleanUp() {
    venice.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNewStore() throws Exception {
    String storeName = Utils.getUniqueString("test");
    String token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.DBjI5MJuVyCa6oncrP5eEP329Pmixk6SX4UG-HS0P7g";
    String schema = "{\"name\": \"key\",\"type\": \"string\"}";
    try (ControllerClient controllerClient = ControllerClient
        .constructClusterControllerClient(clusterName, venice.getAllControllersURLs(), Optional.empty(), null);) {
      controllerClient.createNewStore(storeName, "dev", schema, schema);
      Store store = venice.getLeaderVeniceController().getVeniceAdmin().getStore(clusterName, storeName);
      assertNull(store);
    }

    try (ControllerClient controllerClient = ControllerClient.constructClusterControllerClient(
        clusterName,
        venice.getAllControllersURLs(),
        Optional.empty(),
        ClientAuthenticationProviderToken.TOKEN(token));) {
      controllerClient.createNewStore(storeName, "dev", schema, schema);
      Store store = venice.getLeaderVeniceController().getVeniceAdmin().getStore(clusterName, storeName);
      assertNotNull(store);
    }
  }
}
