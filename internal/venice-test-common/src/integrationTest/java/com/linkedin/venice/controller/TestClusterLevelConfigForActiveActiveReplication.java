package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicPushCompletion;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestClusterLevelConfigForActiveActiveReplication {
  private static final long TEST_TIMEOUT = 120 * Time.MS_PER_SECOND;

  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private ControllerClient parentControllerClient;

  private static final String PARENT_VENICE_ZK_BASE_PATH = "/test-venice-parent";
  private static final String CHILD_VENICE_ZK_BASE_PATH = "/test-venice-child";

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties parentControllerProps = new Properties();
    parentControllerProps
        .setProperty(ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE, Boolean.toString(true));
    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfRouters(1)
            .numberOfServers(1)
            .parentControllerProperties(parentControllerProps)
            .parentVeniceZkBasePath(PARENT_VENICE_ZK_BASE_PATH)
            .childVeniceZkBasePath(CHILD_VENICE_ZK_BASE_PATH)
            .build());
    String clusterName = multiRegionMultiClusterWrapper.getClusterNames()[0];
    parentControllerClient =
        new ControllerClient(clusterName, multiRegionMultiClusterWrapper.getControllerConnectString());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testClusterLevelActiveActiveReplicationConfigForNewHybridStores() throws IOException {
    String storeName = Utils.getUniqueString("test-store-hybrid");
    String pushJobId1 = "test-push-job-id-1";
    parentControllerClient.createNewStore(storeName, "test-owner", "\"string\"", "\"string\"");
    parentControllerClient.emptyPush(storeName, pushJobId1, 1);

    // Version 1 should exist.
    StoreInfo store = assertCommand(parentControllerClient.getStore(storeName)).getStore();
    assertEquals(store.getVersions().size(), 1);

    // Check store level Active/Active is enabled or not
    assertFalse(store.isActiveActiveReplicationEnabled());

    waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(storeName, 1),
        parentControllerClient,
        30,
        TimeUnit.SECONDS);

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
