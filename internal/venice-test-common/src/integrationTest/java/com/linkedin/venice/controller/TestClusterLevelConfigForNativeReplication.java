package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static org.testng.Assert.assertEquals;
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
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestClusterLevelConfigForNativeReplication {
  private static final long TEST_TIMEOUT = 60 * Time.MS_PER_SECOND;

  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private ControllerClient parentControllerClient;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties parentControllerProps = new Properties();
    // enable native replication for batch-only stores through cluster-level config
    parentControllerProps.setProperty(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY, "true");
    parentControllerProps.setProperty(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES, "dc-batch");
    parentControllerProps.setProperty(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES, "dc-hybrid");
    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfRouters(1)
            .numberOfServers(1)
            .parentControllerProperties(parentControllerProps)
            .build());
    parentControllerClient = new ControllerClient(
        multiRegionMultiClusterWrapper.getClusterNames()[0],
        multiRegionMultiClusterWrapper.getControllerConnectString());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testClusterLevelNativeReplicationConfigForNewStores() {
    String storeName = Utils.getUniqueString("test-store");
    String pushJobId1 = "test-push-job-id-1";
    parentControllerClient.createNewStore(storeName, "test-owner", "\"string\"", "\"string\"");
    parentControllerClient.emptyPush(storeName, pushJobId1, 1);

    // Version 1 should exist.
    StoreInfo store = assertCommand(parentControllerClient.getStore(storeName)).getStore();
    assertEquals(store.getVersions().size(), 1);
    // native replication should be enabled by cluster-level config
    assertTrue(store.isNativeReplicationEnabled());
    assertEquals(store.getNativeReplicationSourceFabric(), "dc-batch");
    assertCommand(
        parentControllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(1L).setHybridOffsetLagThreshold(1L)));
    TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(
          parentControllerClient.getStore(storeName).getStore().getNativeReplicationSourceFabric(),
          "dc-hybrid");
    });
    assertCommand(
        parentControllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(-1L).setHybridOffsetLagThreshold(-1L)));
    TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(
          parentControllerClient.getStore(storeName).getStore().getNativeReplicationSourceFabric(),
          "dc-batch");
    });
    assertCommand(
        parentControllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setIncrementalPushEnabled(true)
                .setHybridRewindSeconds(1L)
                .setHybridOffsetLagThreshold(10)));
    TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(
          parentControllerClient.getStore(storeName).getStore().getNativeReplicationSourceFabric(),
          "dc-hybrid");
    });
  }
}
