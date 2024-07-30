package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestClusterLevelConfigForActiveActiveReplication {
  private static final long TEST_TIMEOUT = 120 * Time.MS_PER_SECOND;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Utils.thisIsLocalhost();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testClusterLevelActiveActiveReplicationConfigForNewHybridStores() throws IOException {
    Properties parentControllerProps = getActiveActiveControllerProperties(true, false);
    try (
        VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper =
            ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
                new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
                    .numberOfParentControllers(1)
                    .numberOfChildControllers(1)
                    .numberOfRouters(1)
                    .numberOfServers(1)
                    .parentControllerProperties(parentControllerProps)
                    .build());
        ControllerClient parentControllerClient = new ControllerClient(
            multiRegionMultiClusterWrapper.getClusterNames()[0],
            multiRegionMultiClusterWrapper.getControllerConnectString())) {
      String storeName = Utils.getUniqueString("test-store-hybrid");
      String pushJobId1 = "test-push-job-id-1";
      parentControllerClient.createNewStore(storeName, "test-owner", "\"string\"", "\"string\"");
      parentControllerClient.emptyPush(storeName, pushJobId1, 1);

      // Version 1 should exist.
      StoreInfo store = assertCommand(parentControllerClient.getStore(storeName)).getStore();
      assertEquals(store.getVersions().size(), 1);

      // Check store level Active/Active is enabled or not
      assertFalse(store.isActiveActiveReplicationEnabled());

      // Convert to hybrid store
      assertCommand(
          parentControllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setHybridRewindSeconds(1000L).setHybridOffsetLagThreshold(1000L)));
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertTrue(parentControllerClient.getStore(storeName).getStore().isActiveActiveReplicationEnabled());
      });

      // Reverting hybrid configs disables A/A mode
      assertCommand(
          parentControllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setHybridRewindSeconds(-1).setHybridOffsetLagThreshold(-1)));
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertFalse(parentControllerClient.getStore(storeName).getStore().isActiveActiveReplicationEnabled());
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testClusterLevelActiveActiveReplicationConfigForNewIncrementalPushStores() throws IOException {
    Properties parentControllerProps = getActiveActiveControllerProperties(true, false);
    try (
        VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper =
            ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
                new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
                    .numberOfParentControllers(1)
                    .numberOfChildControllers(1)
                    .numberOfRouters(1)
                    .numberOfServers(1)
                    .parentControllerProperties(parentControllerProps)
                    .build());
        ControllerClient parentControllerClient = new ControllerClient(
            multiRegionMultiClusterWrapper.getClusterNames()[0],
            multiRegionMultiClusterWrapper.getControllerConnectString())) {
      String storeName = Utils.getUniqueString("test-store-incremental");
      String pushJobId1 = "test-push-job-id-1";
      parentControllerClient.createNewStore(storeName, "test-owner", "\"string\"", "\"string\"");
      parentControllerClient.emptyPush(storeName, pushJobId1, 1);

      // Version 1 should exist.
      StoreInfo store = assertCommand(parentControllerClient.getStore(storeName)).getStore();
      assertEquals(store.getVersions().size(), 1);
      assertFalse(store.isIncrementalPushEnabled());
      assertFalse(store.isActiveActiveReplicationEnabled());

      // Disabling incremental push on a store that has inc push disabled should not have any side effects
      assertCommand(
          parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setIncrementalPushEnabled(false)));
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertEquals(parentControllerClient.getStore(storeName).getStore().getVersions().size(), 1);
      });
      store = parentControllerClient.getStore(storeName).getStore();
      assertFalse(store.isIncrementalPushEnabled());
      assertFalse(store.isActiveActiveReplicationEnabled());

      // Enable inc-push
      assertCommand(
          parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setIncrementalPushEnabled(true)));
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertTrue(parentControllerClient.getStore(storeName).getStore().isIncrementalPushEnabled());
      });
      store = parentControllerClient.getStore(storeName).getStore();
      assertTrue(store.isActiveActiveReplicationEnabled());

      // After inc push is disabled, even though default A/A config for pure hybrid store is false,
      // the store's A/A config is retained.
      assertCommand(
          parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setIncrementalPushEnabled(false)));
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertFalse(parentControllerClient.getStore(storeName).getStore().isIncrementalPushEnabled());
      });
      store = parentControllerClient.getStore(storeName).getStore();
      assertTrue(store.isActiveActiveReplicationEnabled());
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testClusterLevelActiveActiveReplicationConfigForNewBatchOnlyStores() throws IOException {
    Properties parentControllerProps = getActiveActiveControllerProperties(false, true);
    try (
        VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper =
            ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
                new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
                    .numberOfParentControllers(1)
                    .numberOfChildControllers(1)
                    .numberOfRouters(1)
                    .numberOfServers(1)
                    .parentControllerProperties(parentControllerProps)
                    .build());
        ControllerClient parentControllerClient = new ControllerClient(
            multiRegionMultiClusterWrapper.getClusterNames()[0],
            multiRegionMultiClusterWrapper.getControllerConnectString())) {
      String storeName = Utils.getUniqueString("test-store-batch-only");
      String pushJobId1 = "test-push-job-id-1";
      parentControllerClient.createNewStore(storeName, "test-owner", "\"string\"", "\"string\"");
      parentControllerClient.emptyPush(storeName, pushJobId1, 1);

      // Version 1 should exist.
      StoreInfo store = assertCommand(parentControllerClient.getStore(storeName)).getStore();
      assertEquals(store.getVersions().size(), 1);

      // Check store level Active/Active is enabled or not
      assertTrue(store.isActiveActiveReplicationEnabled());

      // After updating the store to have incremental push enabled, it's A/A is still enabled
      assertCommand(
          parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setIncrementalPushEnabled(true)));
      store = parentControllerClient.getStore(storeName).getStore();
      assertTrue(parentControllerClient.getStore(storeName).getStore().isIncrementalPushEnabled());
      assertTrue(store.isActiveActiveReplicationEnabled());

      // Let's disable the A/A config for the store.
      assertCommand(
          parentControllerClient
              .updateStore(storeName, new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(false)));
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertFalse(parentControllerClient.getStore(storeName).getStore().isActiveActiveReplicationEnabled());
      });

      // After updating the store back to a batch-only store, it's A/A becomes enabled again
      assertCommand(
          parentControllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setIncrementalPushEnabled(false)
                  .setHybridRewindSeconds(-1)
                  .setHybridOffsetLagThreshold(-1)));
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertTrue(parentControllerClient.getStore(storeName).getStore().isActiveActiveReplicationEnabled());
      });

      // After updating the store to be a hybrid store, it's A/A should still be enabled.
      assertCommand(
          parentControllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setHybridRewindSeconds(1000L).setHybridOffsetLagThreshold(1000L)));
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertTrue(parentControllerClient.getStore(storeName).getStore().isActiveActiveReplicationEnabled());
      });
    }
  }

  private Properties getActiveActiveControllerProperties(
      boolean enableActiveActiveForHybrid,
      boolean enableActiveActiveForBatchOnly) {
    Properties props = new Properties();
    props.setProperty(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY, "true");
    // Enable Active/Active replication for hybrid stores through cluster-level config
    props.setProperty(
        ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE,
        Boolean.toString(enableActiveActiveForHybrid));
    // Enable Active/Active replication for batch-only stores through cluster-level config
    props.setProperty(
        ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY_STORE,
        Boolean.toString(enableActiveActiveForBatchOnly));
    return props;
  }
}
