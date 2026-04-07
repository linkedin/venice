package com.linkedin.venice.client.store;

import static com.linkedin.venice.utils.TestUtils.*;

import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
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
  public void testGetAllStoreNames() throws Exception {
    // Step 1: Create 2 stores and wait until they are queryable
    storeName1 = Utils.getUniqueString("test-store");
    storeName2 = Utils.getUniqueString("test-store");
    String routerUrl = veniceCluster.getRandomRouterURL();

    NewStoreResponse storeResponse1 = assertCommand(veniceCluster.getNewStore(storeName1));
    veniceCluster.useControllerClient(
        controllerClient -> assertCommand(
            controllerClient.sendEmptyPushAndWait(
                storeResponse1.getName(),
                Utils.getUniqueString(),
                100,
                30 * Time.MS_PER_SECOND)));

    NewStoreResponse storeResponse2 = assertCommand(veniceCluster.getNewStore(storeName2));
    veniceCluster.useControllerClient(
        controllerClient -> assertCommand(
            controllerClient.sendEmptyPushAndWait(
                storeResponse2.getName(),
                Utils.getUniqueString(),
                100,
                30 * Time.MS_PER_SECOND)));

    client1 = ClientFactory
        .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName1).setVeniceURL(routerUrl));
    client2 = ClientFactory
        .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName2).setVeniceURL(routerUrl));

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertNotNull(
          veniceCluster.getControllerClient().getStore(storeName1).getStore(),
          storeName1 + " is not available yet");
      Assert.assertNotNull(
          veniceCluster.getControllerClient().getStore(storeName2).getStore(),
          storeName2 + " is not available yet");
    });

    // Step 2: Create StoreMetadataFetcher and verify getAllStoreNames returns both stores
    // The router's store config repository is updated asynchronously via Helix ZK watches,
    // so we retry until the router reflects the newly created stores.
    try (StoreMetadataFetcher fetcher =
        ClientFactory.createStoreMetadataFetcher(new ClientConfig<>().setVeniceURL(routerUrl))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Set<String> storeNames = fetcher.getAllStoreNames();
        Assert.assertTrue(storeNames.contains(storeName1), "Missing store: " + storeName1);
        Assert.assertTrue(storeNames.contains(storeName2), "Missing store: " + storeName2);
      });
    }
  }
}
