package com.linkedin.venice.client.store;

import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestRouterBasedStoreMetadataFetcher {
  private static final int TEST_TIMEOUT_MS = 60_000;

  private VeniceClusterWrapper veniceCluster;
  private String storeName1;
  private String storeName2;
  private AvroGenericStoreClient<String, String> client1;
  private AvroGenericStoreClient<String, String> client2;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    veniceCluster = ServiceFactory.getVeniceCluster(new VeniceClusterCreateOptions.Builder().build());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(client1);
    Utils.closeQuietlyWithErrorLogged(client2);
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testGetAllStoreNamesReturnsBothStores() throws Exception {
    // Step 1: Create 2 stores and wait until they are queryable
    storeName1 = Utils.getUniqueString("test-store-1");
    storeName2 = Utils.getUniqueString("test-store-2");
    String routerUrl = veniceCluster.getRandomRouterURL();
    ControllerClient controllerClient = veniceCluster.getControllerClient();

    veniceCluster.getNewStore(storeName1);
    veniceCluster.getNewStore(storeName2);

    client1 = ClientFactory
        .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName1).setVeniceURL(routerUrl));
    client2 = ClientFactory
        .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName2).setVeniceURL(routerUrl));

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertNotNull(controllerClient.getStore(storeName1).getStore(), storeName1 + " is not available yet");
      Assert.assertNotNull(controllerClient.getStore(storeName2).getStore(), storeName2 + " is not available yet");
    });

    // Step 2: Create StoreMetadataFetcher and verify getAllStoreNames returns both stores
    try (StoreMetadataFetcher fetcher =
        new RouterBasedStoreMetadataFetcher(new HttpTransportClient(routerUrl, 10, 10))) {
      Set<String> storeNames = fetcher.getAllStoreNames();
      Assert.assertTrue(storeNames.contains(storeName1), "Missing store: " + storeName1);
      Assert.assertTrue(storeNames.contains(storeName2), "Missing store: " + storeName2);
    }
  }
}
