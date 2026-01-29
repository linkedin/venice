package com.linkedin.venice.controller;

import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicPushCompletion;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test cluster level config for native replication.
 */
public class TestClusterLevelConfigForNativeReplication {
  private static final long TEST_TIMEOUT = 60 * Time.MS_PER_SECOND;

  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private ControllerClient parentControllerClient;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties parentControllerProps = new Properties();
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
    String dcName = multiRegionMultiClusterWrapper.getChildRegionNames().get(0);
    parentControllerClient.createNewStore(storeName, "test-owner", "\"string\"", "\"string\"");
    parentControllerClient.emptyPush(storeName, pushJobId1, 1);
    waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(storeName, 1),
        parentControllerClient,
        30,
        TimeUnit.SECONDS);
    // Version 1 should exist.
    StoreInfo store = assertCommand(parentControllerClient.getStore(storeName)).getStore();
    assertEquals(store.getVersions().size(), 1);
    // native replication should be enabled by cluster-level config
    assertTrue(store.isNativeReplicationEnabled());
    assertEquals(store.getNativeReplicationSourceFabric(), "dc-0");
    assertCommand(
        parentControllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(1L).setHybridOffsetLagThreshold(1L)));
    TestUtils.waitForNonDeterministicAssertion(
        TEST_TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(
            parentControllerClient.getStore(storeName).getStore().getNativeReplicationSourceFabric(),
            dcName));
    assertCommand(
        parentControllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(-1L).setHybridOffsetLagThreshold(-1L)));
    TestUtils.waitForNonDeterministicAssertion(
        TEST_TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(
            parentControllerClient.getStore(storeName).getStore().getNativeReplicationSourceFabric(),
            dcName));
    assertCommand(
        parentControllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setIncrementalPushEnabled(true)
                .setActiveActiveReplicationEnabled(true)
                .setHybridRewindSeconds(1L)
                .setHybridOffsetLagThreshold(10)));
    TestUtils.waitForNonDeterministicAssertion(
        TEST_TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(
            parentControllerClient.getStore(storeName).getStore().getNativeReplicationSourceFabric(),
            dcName));
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testConvertHybridDuringPushjob() {
    String storeName = Utils.getUniqueString("test-store");
    parentControllerClient.createNewStore(storeName, "test-owner", "\"string\"", "\"string\"");
    parentControllerClient.requestTopicForWrites(
        storeName,
        1000,
        Version.PushType.BATCH,
        Version.numberBasedDummyPushId(1),
        true,
        true,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.of("dc-1"),
        false,
        -1);

    ControllerResponse response = parentControllerClient.updateStore(
        storeName,
        new UpdateStoreQueryParams().setHybridRewindSeconds(1L).setHybridOffsetLagThreshold(1L));
    Assert.assertTrue(response.isError());
    Assert.assertTrue(response.getError().contains("Cannot convert to hybrid as there is already a pushjob running"));
    parentControllerClient.killOfflinePushJob(Version.composeKafkaTopic(storeName, 1));
  }

}
