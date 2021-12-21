package com.linkedin.venice.controller;

import com.linkedin.venice.controller.server.AbstractTestAdminSparkServer;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.utils.Utils;

import java.util.Optional;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.meta.VeniceUserStoreType.*;


public class TestBatchUpdateCommandForLeaderFollowerModel extends AbstractTestAdminSparkServer {
  @BeforeClass
  public void setUp() {
    super.setUp(false, Optional.empty(), new Properties());
  }

  @AfterClass
  public void tearDown() {
    super.tearDown();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanSetLeaderFollowerModelInTheCluster() {
    /**
     * Create a batch-only store
     */
    String batchOnlyStoreName = Utils.getUniqueString("batch-only");
    controllerClient.createNewStore(batchOnlyStoreName, "test", "\"string\"", "\"string\"");

    /**
     * Create a hybrid store
     */
    String hybridStoreName = Utils.getUniqueString("hybrid-store");
    controllerClient.createNewStore(hybridStoreName, "test", "\"string\"", "\"string\"");
    controllerClient.updateStore(hybridStoreName, new UpdateStoreQueryParams()
        .setHybridOffsetLagThreshold(1000l)
        .setHybridRewindSeconds(1000l));

    /**
     * Create an incremental push enabled store
     */
    String incrementalPushStoreName = Utils.getUniqueString("incremental-push-store");
    controllerClient.createNewStore(incrementalPushStoreName, "test", "\"string\"", "\"string\"");
    controllerClient.updateStore(incrementalPushStoreName, new UpdateStoreQueryParams().setIncrementalPushEnabled(true));

    try {
      /**
       * First test: migration all batch-only stores in a cluster to L/F state model
       */
      controllerClient.enableLFModel(true, BATCH_ONLY.toString());
      String[] storesWithLFEnabled = controllerClient.listLFStores().getStores();
      // The expectation is that only the batch-only stores are updated
      Assert.assertEquals(batchOnlyStoreName, storesWithLFEnabled[0]);
      Assert.assertEquals(1, storesWithLFEnabled.length);

      /**
       * Second test: migration all hybrid stores in a cluster to L/F state model
       *
       * Revert the cluster to previous state first
       */
      controllerClient.enableLFModel(false, ALL.toString());
      Assert.assertEquals(0, controllerClient.listLFStores().getStores().length);
      controllerClient.enableLFModel(true, HYBRID_ONLY.toString());
      storesWithLFEnabled = controllerClient.listLFStores().getStores();
      // The expectation is that only the hybrid stores are updated
      Assert.assertEquals(hybridStoreName, storesWithLFEnabled[0]);
      Assert.assertEquals(1, storesWithLFEnabled.length);

      /**
       * Third test: migration all incremental push enabled stores in a cluster to L/F state model
       *
       * Revert the cluster to previous state first
       */
      controllerClient.enableLFModel(false, ALL.toString());
      Assert.assertEquals(0, controllerClient.listLFStores().getStores().length);
      controllerClient.enableLFModel(true, INCREMENTAL_PUSH.toString());
      storesWithLFEnabled = controllerClient.listLFStores().getStores();
      // The expectation is that only the incremental push enabled stores are updated
      Assert.assertEquals(incrementalPushStoreName, storesWithLFEnabled[0]);
      Assert.assertEquals(1, storesWithLFEnabled.length);

      /**
       * Fourth test: migration all hybrid and incremental stores in a cluster to L/F state model
       *
       * Revert the cluster to previous state first
       */
      controllerClient.enableLFModel(false, ALL.toString());
      Assert.assertEquals(0, controllerClient.listLFStores().getStores().length);
      controllerClient.enableLFModel(true, HYBRID_OR_INCREMENTAL.toString());
      storesWithLFEnabled = controllerClient.listLFStores().getStores();
      Assert.assertEquals(2, storesWithLFEnabled.length);

      /**
       * Fifth test: migration all stores in a cluster to L/F state model
       *
       * Revert the cluster to previous state first
       */
      controllerClient.enableLFModel(false, ALL.toString());
      Assert.assertEquals(0, controllerClient.listLFStores().getStores().length);
      controllerClient.enableLFModel(true, ALL.toString());
      storesWithLFEnabled = controllerClient.listLFStores().getStores();
      Assert.assertEquals(3, storesWithLFEnabled.length);
    } finally {
      deleteStore(batchOnlyStoreName);
      deleteStore(hybridStoreName);
      deleteStore(incrementalPushStoreName);
    }
  }

  private void deleteStore(String storeName) {
    controllerClient.enableStoreReadWrites(storeName, false);
    controllerClient.deleteStore(storeName);
  }
}
