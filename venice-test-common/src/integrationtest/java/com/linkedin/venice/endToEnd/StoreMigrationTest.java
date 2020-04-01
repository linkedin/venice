package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.controllerapi.TrackableControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class StoreMigrationTest {
  private VeniceMultiClusterWrapper multiClusterWrapper;

  @BeforeClass
  public void setup() {
    multiClusterWrapper = ServiceFactory.getVeniceMultiClusterWrapper(2, 2, 1, 1);
  }

  @AfterClass
  public void cleanup() {
    IOUtils.closeQuietly(multiClusterWrapper);
  }

  @Test
  public void testD2ClientAfterStoreMigration() throws Exception {
    String[] clusterNames = multiClusterWrapper.getClusterNames();
    String srcClusterName = clusterNames[0];
    String destClusterName = clusterNames[1];
    String storeName = "test-store";
    VeniceClusterWrapper srcCluster = multiClusterWrapper.getClusters().get(srcClusterName);
    VeniceClusterWrapper destCluster = multiClusterWrapper.getClusters().get(destClusterName);
    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, srcCluster.getRandomRouterURL());
    ControllerClient destControllerClient = new ControllerClient(destClusterName, destCluster.getRandomRouterURL());
    Admin srcAdmin = multiClusterWrapper.getMasterController(srcClusterName).getVeniceAdmin();

    srcAdmin.addStore(srcClusterName, storeName, "tester", "\"string\"", "\"string\"");
    srcAdmin.updateStore(srcClusterName, storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));

    D2Client d2Client = D2TestUtils.getAndStartD2Client(srcCluster.getZk().getAddress());
    AvroGenericStoreClient<String, String> client = ClientFactory.getAndStartGenericAvroClient(ClientConfig
        .defaultGenericClientConfig(storeName)
        .setD2ServiceName(D2TestUtils.DEFAULT_TEST_SERVICE_NAME)
        .setD2Client(d2Client));

    try {
      client.get("key").get();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("There is no version for store"));
    }

    // Migrate store from src to dest cluster
    StoreMigrationResponse storeMigrationResponse = destControllerClient.migrateStore(storeName, srcCluster.getClusterName());
    Assert.assertFalse(storeMigrationResponse.isError(), storeMigrationResponse.getError());

    TestUtils.waitForNonDeterministicAssertion(300, TimeUnit.SECONDS, true, () -> {
      // Store discovery should point to the new cluster after migration
      ControllerResponse discoveryResponse = destControllerClient.discoverCluster(storeName);
      String newCluster = discoveryResponse.getCluster();
      Assert.assertNotNull(newCluster);
      Assert.assertNotEquals(newCluster, srcCluster.getClusterName(),
          "The newCluster returned by the cluster discovery endpoint should be different from the srcClusterName.");
    });

    ControllerResponse updateStoreResponse1 = srcControllerClient.updateStore(storeName,
        new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
    Assert.assertFalse(updateStoreResponse1.isError(), updateStoreResponse1.getError());

    TrackableControllerResponse deleteStoreResponse = srcControllerClient.deleteStore(storeName);
    Assert.assertFalse(deleteStoreResponse.isError(), deleteStoreResponse.getError());

    ControllerResponse updateStoreResponse2 = destControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setStoreMigration(false));
    Assert.assertFalse(updateStoreResponse2.isError(), updateStoreResponse2.getError());

    // Expect no version exception instead of no store exception
    try {
      client.get("key").get();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("There is no version for store"));
    }

    destControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
    destControllerClient.deleteStore(storeName);
    IOUtils.closeQuietly(srcAdmin);
  }
}
