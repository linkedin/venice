package com.linkedin.venice.controller;

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
import com.linkedin.venice.utils.Utils;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestClusterLevelConfigForNativeReplication {
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private ControllerClient parentControllerClient;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties childControllerProps = new Properties();
    // enable native replication for batch-only stores through cluster-level config
    childControllerProps.setProperty(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES, "dc-batch");
    childControllerProps.setProperty(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES, "dc-hybrid");

    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfRouters(1)
            .numberOfServers(1)
            .parentControllerProperties(childControllerProps)
            .build());
    String clusterName = multiRegionMultiClusterWrapper.getClusterNames()[0];
    parentControllerClient =
        new ControllerClient(clusterName, multiRegionMultiClusterWrapper.getControllerConnectString());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test
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
    Assert.assertEquals(store.getNativeReplicationSourceFabric(), "dc-batch");

    // Convert to hybrid store
    assertCommand(
        parentControllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(1000L).setHybridOffsetLagThreshold(1000L)));
    store = assertCommand(parentControllerClient.getStore(storeName)).getStore();
    assertTrue(store.isNativeReplicationEnabled());
    Assert.assertEquals(store.getNativeReplicationSourceFabric(), "dc-hybrid");

    // Reverting hybrid configs reverts NR source region
    assertCommand(
        parentControllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(-1).setHybridOffsetLagThreshold(-1)));
    store = assertCommand(parentControllerClient.getStore(storeName)).getStore();
    assertTrue(store.isNativeReplicationEnabled());
    Assert.assertEquals(store.getNativeReplicationSourceFabric(), "dc-batch");
  }
}
