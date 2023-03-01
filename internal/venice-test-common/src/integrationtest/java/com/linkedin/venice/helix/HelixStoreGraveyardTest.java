package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import java.util.Arrays;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HelixStoreGraveyardTest {
  private HelixStoreGraveyard graveyard;
  private ZkServerWrapper zkServerWrapper;
  private ZkClient zkClient;
  private String[] clusterNames =
      new String[] { "HelixStoreGraveyardTest1", "HelixStoreGraveyardTest2", "HelixStoreGraveyardTest3" };
  private String storeName = "HelixStoreGraveyardTestStore";

  @BeforeMethod
  public void setUp() {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkClient = ZkClientFactory.newZkClient(zkServerWrapper.getAddress());
    graveyard = new HelixStoreGraveyard(zkClient, new HelixAdapterSerializer(), Arrays.asList(clusterNames));
  }

  @AfterMethod
  public void cleanUp() {
    zkClient.close();
    zkServerWrapper.close();
  }

  @Test
  public void testPutStoreIntoGraveyard() {
    int largestUsedVersionNumber = 100;
    Store store = TestUtils.createTestStore(storeName, "", System.currentTimeMillis());
    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
    graveyard.putStoreIntoGraveyard(clusterNames[0], store);
    largestUsedVersionNumber++;
    // put the second cluster.
    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
    graveyard.putStoreIntoGraveyard(clusterNames[1], store);
    Assert.assertEquals(
        graveyard.getLargestUsedVersionNumber(storeName),
        largestUsedVersionNumber,
        "Store should be put in to graveyard with the updated largestUsedVersionNumber.");

    // Store already exists in graveyard.
    largestUsedVersionNumber++;
    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
    graveyard.putStoreIntoGraveyard(clusterNames[0], store);
    Assert.assertEquals(
        graveyard.getLargestUsedVersionNumber(storeName),
        largestUsedVersionNumber,
        "Store should be put in to graveyard with the updated largestUsedVersionNumber.");

    // Store already exists and the largestUsedVersionNumber is smaller than the one in graveyard.
    largestUsedVersionNumber--;
    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
    try {
      graveyard.putStoreIntoGraveyard(clusterNames[0], store);
      Assert.fail("Invalid largestUsedVersionNumber, put operation should fail.");
    } catch (VeniceException e) {
      // expected
    }
  }

  @Test
  public void testGetLargestUsedVersionNumber() {
    Assert.assertEquals(
        graveyard.getLargestUsedVersionNumber(storeName),
        Store.NON_EXISTING_VERSION,
        "Store has not been deleted. This method should return 0.");
    int largestUsedVersionNumber = 100;
    Store store = TestUtils.createTestStore(storeName, "", System.currentTimeMillis());
    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
    graveyard.putStoreIntoGraveyard(clusterNames[0], store);
    Assert.assertEquals(
        graveyard.getLargestUsedVersionNumber(storeName),
        largestUsedVersionNumber,
        "Store should be put in to graveyard with the updated largestUsedVersionNumber.");
  }
}
