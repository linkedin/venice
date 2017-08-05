package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import org.apache.helix.manager.zk.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HelixStoreGraveyardTest {
  private HelixStoreGraveyard graveyard;
  private ZkServerWrapper zkServerWrapper;
  private ZkClient zkClient;
  private String clusterName = "HelixStoreGraveyardTest";
  private String storeName = "HelixStoreGraveyardTestStore";

  @BeforeMethod
  public void setup() {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkClient = new ZkClient(zkServerWrapper.getAddress());
    graveyard = new HelixStoreGraveyard(zkClient, new HelixAdapterSerializer(), clusterName);
  }

  @AfterMethod
  public void cleanup() {
    zkClient.close();
    zkServerWrapper.close();
  }

  @Test
  public void testPutStoreIntoGraveyard() {
    int largestUsedVersionNumber = 100;
    Store store = TestUtils.createTestStore(storeName, "", System.currentTimeMillis());
    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
    graveyard.putStoreIntoGraveyard(store);
    Assert.assertEquals(graveyard.getLargestUsedVersionNumber(storeName), largestUsedVersionNumber,
        "Store should be put in to graveyard with the updated largestUsedVersionNumber.");

    // Store already exists in graveyard.
    largestUsedVersionNumber++;
    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
    graveyard.putStoreIntoGraveyard(store);
    Assert.assertEquals(graveyard.getLargestUsedVersionNumber(storeName), largestUsedVersionNumber,
        "Store should be put in to graveyard with the updated largestUsedVersionNumber.");

    // Store already exists and the largestUsedVersionNumber is smaller than the one in graveyard.
    largestUsedVersionNumber--;
    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
    try {
      graveyard.putStoreIntoGraveyard(store);
      Assert.fail("Invalid largestUsedVersionNumber, put operation should fail.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testGetLargestUsedVersionNumber() {
    Assert.assertEquals(graveyard.getLargestUsedVersionNumber(storeName), Store.NON_EXISTING_VERSION,
        "Store has not been deleted. This method should return 0.");
    int largestUsedVersionNumber = 100;
    Store store = TestUtils.createTestStore(storeName, "", System.currentTimeMillis());
    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
    graveyard.putStoreIntoGraveyard(store);
    Assert.assertEquals(graveyard.getLargestUsedVersionNumber(storeName), largestUsedVersionNumber,
        "Store should be put in to graveyard with the updated .largestUsedVersionNumber.");
  }

  @Test
  public void testGetLastDeletionTime() {
    long start = System.currentTimeMillis();
    Store store = TestUtils.createTestStore(storeName, "", System.currentTimeMillis());
    graveyard.putStoreIntoGraveyard(store);

    long lastDeletionTime = graveyard.getLastDeletionTime(storeName);
    long now = System.currentTimeMillis();
    Assert.assertTrue(lastDeletionTime > start && lastDeletionTime < now, "Store deletion time is wrong.");
  }
}
