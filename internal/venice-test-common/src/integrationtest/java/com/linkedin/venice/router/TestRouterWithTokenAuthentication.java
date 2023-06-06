package com.linkedin.venice.router;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.authentication.AuthenticationService;
import com.linkedin.venice.authentication.jwt.ClientAuthenticationProviderToken;
import com.linkedin.venice.authentication.jwt.TokenAuthenticationService;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.authorization.SimpleAuthorizerService;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestRouterWithTokenAuthentication {
  private static final String KEY_SCHEMA_STR = "\"string\"";

  private static final String VALUE_SCHEMA_STR =
      "{\n" + "\"type\": \"record\",\n" + "\"name\": \"test_value_schema\",\n" + "\"fields\": [\n" + "  {\"name\": \""
          + "int_field\", \"type\": \"int\"}]\n" + "}";
  private VeniceClusterWrapper veniceCluster;
  private String storeName;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    veniceCluster = ServiceFactory.getVeniceCluster(1, 1, 0, 1, 100, false, false);

    Properties tokenProperties = new Properties();
    tokenProperties.put("authentication.jwt.secretKey", "jDdra78Vo1+RVMGY2easnWe0sAFrEa2581ra5YMotbE=");

    // we need to create the instances there due to some classpath problems
    AuthenticationService tokenAuthenticationService = new TokenAuthenticationService();
    tokenAuthenticationService.initialise(new VeniceProperties(tokenProperties));
    AuthorizerService simpleAuthorizerService = new SimpleAuthorizerService();

    Properties routerProperties = new Properties();
    routerProperties.put("AuthenticationService", tokenAuthenticationService);
    routerProperties.put("AuthorizerService", simpleAuthorizerService);
    veniceCluster.addVeniceRouter(routerProperties);

    // Create test store
    storeName = Utils.getUniqueString("store");

    try (ControllerClient controllerClient =
        new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs());) {
      assertFalse(controllerClient.createNewStore(storeName, "dev", KEY_SCHEMA_STR, VALUE_SCHEMA_STR).isError());
      assertFalse(controllerClient.emptyPush(storeName, "xxx", 1).isError());
    }
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void smokeTest() throws Exception {
    List<VeniceRouterWrapper> routers = veniceCluster.getVeniceRouters();
    assertEquals(1, routers.size(), "There should be only one router in this cluster");

    // No token
    try (AvroGenericStoreClient<Object, Object> storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL("http://" + routers.get(0).getAddress()))) {
      storeClient.get("myKey").get();
    } catch (ExecutionException ok) {
      VeniceClientHttpException veniceClientHttpException = (VeniceClientHttpException) ok.getCause();
      assertEquals(401, veniceClientHttpException.getHttpStatus());
    }

    // bad token
    try (AvroGenericStoreClient<Object, Object> storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL("http://" + routers.get(0).getAddress())
            .setAuthenticationProvider(ClientAuthenticationProviderToken.TOKEN("xxx")))) {
      storeClient.get("myKey").get();
    } catch (ExecutionException ok) {
      VeniceClientHttpException veniceClientHttpException = (VeniceClientHttpException) ok.getCause();
      assertEquals(401, veniceClientHttpException.getHttpStatus());
    }

    // good token
    try (AvroGenericStoreClient<Object, Object> storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL("http://" + routers.get(0).getAddress())
            .setAuthenticationProvider(
                ClientAuthenticationProviderToken
                    .TOKEN("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.DBjI5MJuVyCa6oncrP5eEP329Pmixk6SX4UG-HS0P7g")))) {
      assertNull(storeClient.get("myKey").get());
    }
  }
}
