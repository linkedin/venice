package com.linkedin.venice.client.store;

import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestRouterBasedStoreMetadataFetcher {
  private static final int TEST_TIMEOUT_MS = 120_000;

  private VeniceTwoLayerMultiRegionMultiClusterWrapper venice;
  private D2Client d2Client;
  private String storeName1;
  private String storeName2;
  private ControllerClient parentControllerClient;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    Properties parentControllerProps = new Properties();
    Properties childControllerProps = new Properties();
    venice = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .parentControllerProperties(parentControllerProps)
            .childControllerProperties(childControllerProps)
            .serverProperties(serverProperties)
            .build());

    VeniceControllerWrapper parentController = venice.getParentControllers().get(0);
    parentControllerClient = new ControllerClient(venice.getClusterNames()[0], parentController.getControllerUrl());
    d2Client = IntegrationTestPushUtils.getD2Client(venice.getChildRegions().get(0).getZkServerWrapper().getAddress());
    D2ClientUtils.startClient(d2Client);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    D2ClientUtils.shutdownClient(d2Client);
    venice.close();
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testGetAllStoreNames() throws Exception {
    // Step 1: Create 2 stores and wait until they are queryable
    storeName1 = Utils.getUniqueString("test-store");
    storeName2 = Utils.getUniqueString("test-store");

    parentControllerClient.createNewStore(storeName1, "owner", STRING_SCHEMA.toString(), STRING_SCHEMA.toString());
    parentControllerClient.createNewStore(storeName2, "owner", STRING_SCHEMA.toString(), STRING_SCHEMA.toString());

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(parentControllerClient.getStore(storeName1).getStore().getName(), storeName1);
      Assert.assertEquals(parentControllerClient.getStore(storeName2).getStore().getName(), storeName2);
    });

    // Step 2: Create StoreMetadataFetcher and verify getAllStoreNames returns both stores
    // The router's store config repository is updated asynchronously via Helix ZK watches,
    // so we retry until the router reflects the newly created stores.
    try (StoreMetadataFetcher fetcher = ClientFactory.createStoreMetadataFetcher(
        new ClientConfig<>().setD2Client(d2Client)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME))) {
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        Set<String> storeNames = fetcher.getAllStoreNames();
        Assert.assertEquals(storeNames.size(), 2, "Expected exactly 2 stores, but got: " + storeNames);
        Assert.assertTrue(storeNames.contains(storeName1), "Missing store: " + storeName1);
        Assert.assertTrue(storeNames.contains(storeName2), "Missing store: " + storeName2);
      });
    }
  }
}
