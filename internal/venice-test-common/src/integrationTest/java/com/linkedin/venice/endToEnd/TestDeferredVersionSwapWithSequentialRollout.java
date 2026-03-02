package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS;
import static com.linkedin.venice.ConfigKeys.DEFERRED_VERSION_SWAP_FOR_EMPTY_PUSH_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFERRED_VERSION_SWAP_REGION_ROLL_FORWARD_ORDER;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_LIST;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP;
import static org.testng.Assert.assertFalse;

import com.linkedin.venice.controller.MockStoreLifecycleHooks;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.meta.LifecycleHooksRecord;
import com.linkedin.venice.meta.LifecycleHooksRecordImpl;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestDeferredVersionSwapWithSequentialRollout extends AbstractMultiRegionTest {
  private static final String REGION1 = "dc-0";
  private static final String REGION2 = "dc-1";
  private static final String REGION3 = "dc-2";
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 2).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  private static final int TEST_TIMEOUT = 180_000;

  @Override
  protected int getNumberOfRegions() {
    return 3;
  }

  @Override
  protected int getNumberOfClusters() {
    return 2;
  }

  @Override
  protected int getNumberOfServers() {
    return 1;
  }

  @Override
  protected int getReplicationFactor() {
    return 1;
  }

  @Override
  protected Properties getExtraControllerProperties() {
    Properties controllerProps = new Properties();
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS, 100);
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED, true);
    controllerProps.put(DEFERRED_VERSION_SWAP_REGION_ROLL_FORWARD_ORDER, REGION1 + "," + REGION2 + "," + REGION3);
    controllerProps.put(DEFERRED_VERSION_SWAP_FOR_EMPTY_PUSH_ENABLED, true);
    return controllerProps;
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDeferredVersionSwap() throws Exception {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwap");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    List<LifecycleHooksRecord> lifecycleHooks = new ArrayList<>();
    Map<String, String> lifecycleHooksParams = new HashMap<>();
    lifecycleHooksParams.put("outcome", StoreVersionLifecycleEventOutcome.PROCEED.toString());
    lifecycleHooks.add(new LifecycleHooksRecordImpl(MockStoreLifecycleHooks.class.getName(), lifecycleHooksParams));
    UpdateStoreQueryParams storeParms =
        new UpdateStoreQueryParams().setStoreLifecycleHooks(lifecycleHooks).setTargetRegionSwapWaitTime(60);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAMES[0], keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();

      // Start push job with target region push enabled (in background since VPJ blocks until swap)
      props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      props.put(TARGETED_REGION_PUSH_LIST, REGION1);
      Thread vpjThread = new Thread(() -> IntegrationTestPushUtils.runVPJ(props));
      vpjThread.setDaemon(true);
      vpjThread.start();
      try {
        TestUtils.waitForNonDeterministicPushCompletion(
            Version.composeKafkaTopic(storeName, 1),
            parentControllerClient,
            60,
            TimeUnit.SECONDS);

        // Validate deferred swap stages: target region swaps first, others follow after release
        assertDeferredSwapStages(parentControllerClient, storeName, 1, REGION1);

        vpjThread.join(30_000);
      } finally {
        if (vpjThread.isAlive()) {
          vpjThread.interrupt();
          vpjThread.join(5_000);
        }
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDeferredVersionSwapWithFailedValidationSequentialRollout() throws Exception {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwap");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    List<LifecycleHooksRecord> lifecycleHooks = new ArrayList<>();
    Map<String, String> lifecycleHooksParams = new HashMap<>();
    lifecycleHooksParams.put("outcome", StoreVersionLifecycleEventOutcome.ROLLBACK.toString());
    lifecycleHooks.add(new LifecycleHooksRecordImpl(MockStoreLifecycleHooks.class.getName(), lifecycleHooksParams));
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setStoreLifecycleHooks(lifecycleHooks);
    storeParms.setTargetRegionSwapWaitTime(60);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAMES[0], keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();

      // First push (non-targeted) runs synchronously
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Start push job with target region push enabled (in background since VPJ blocks until swap)
      props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      props.put(TARGETED_REGION_PUSH_LIST, REGION1);
      Thread vpjThread = new Thread(() -> IntegrationTestPushUtils.runVPJ(props));
      vpjThread.setDaemon(true);
      vpjThread.start();
      try {
        TestUtils.waitForNonDeterministicPushCompletion(
            Version.composeKafkaTopic(storeName, 2),
            parentControllerClient,
            60,
            TimeUnit.SECONDS);

        // Wait for target region to swap to v2
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          Map<String, Integer> versions = getColoVersions(parentControllerClient, storeName);
          Assert.assertEquals((int) versions.get(REGION1), 2, "Target region should have v2");
        });

        // Release wait time so DeferredVersionSwapService fires -> lifecycle hooks return ROLLBACK
        parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setTargetRegionSwapWaitTime(0));

        // Verify version 2 is killed (rollback)
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
          Optional<Version> v2 = parentStore.getVersion(2);
          Assert.assertTrue(v2.isPresent(), "Version 2 should exist");
          Assert.assertEquals(v2.get().getStatus(), VersionStatus.KILLED);
        });

        // Verify all regions remain on v1 after rollback
        Map<String, Integer> coloVersions = getColoVersions(parentControllerClient, storeName);
        coloVersions.forEach((colo, version) -> {
          Assert.assertEquals((int) version, 1, "Region " + colo + " should still be on v1 after rollback");
        });

        vpjThread.join(30_000);
      } finally {
        if (vpjThread.isAlive()) {
          vpjThread.interrupt();
          vpjThread.join(5_000);
        }
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDeferredVersionSwapForEmptyPush() throws Exception {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwap");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setTargetRegionSwapWaitTime(60);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAMES[0], keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();

      parentControllerClient.emptyPush(storeName, "test", 100000);

      // Release the swap wait time so DeferredVersionSwapService fires as soon as ingestion completes
      parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setTargetRegionSwapWaitTime(0));

      // Wait for all regions to converge to version 1
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          Assert.assertEquals((int) version, 1);
        });
      });

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse parentStore = parentControllerClient.getStore(storeName);
        Assert.assertNotNull(parentStore);
        StoreInfo storeInfo = parentStore.getStore();
        Assert.assertEquals(storeInfo.getVersion(1).get().getStatus(), VersionStatus.ONLINE);
      });

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        // Verify that we can create a new version
        VersionCreationResponse versionCreationResponse = parentControllerClient.requestTopicForWrites(
            storeName,
            1000,
            Version.PushType.BATCH,
            Version.guidBasedDummyPushId(),
            true,
            true,
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            false,
            -1);
        assertFalse(versionCreationResponse.isError());
      });
    }
  }

  /**
   * Validates deferred swap stages:
   * 1. Target region(s) have new version (push completed there)
   * 2. Non-target regions still on previous version (swap is deferred)
   * 3. After releasing wait time, all regions converge to new version
   */
  private void assertDeferredSwapStages(
      ControllerClient parentControllerClient,
      String storeName,
      int expectedVersion,
      String targetRegions) {
    int previousVersion = expectedVersion - 1;
    Set<String> targetSet = RegionUtils.parseRegionsFilterList(targetRegions);
    Set<String> allRegions = new HashSet<>(Arrays.asList(REGION1, REGION2, REGION3));

    // Stage 1: target region(s) should have new version
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Map<String, Integer> versions = getColoVersions(parentControllerClient, storeName);
      for (String r: targetSet) {
        Assert.assertEquals(
            (int) versions.get(r),
            expectedVersion,
            "Target region " + r + " should have v" + expectedVersion);
      }
    });

    // Stage 2: non-target regions should still be on previous version
    Map<String, Integer> versions = getColoVersions(parentControllerClient, storeName);
    for (String r: allRegions) {
      if (!targetSet.contains(r)) {
        Assert.assertEquals(
            (int) versions.get(r),
            previousVersion,
            "Non-target region " + r + " should still be on v" + previousVersion);
      }
    }

    // Stage 3: release the swap
    ControllerResponse resp =
        parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setTargetRegionSwapWaitTime(0));
    Assert.assertFalse(resp.isError(), "Failed to update targetRegionSwapWaitTime: " + resp.getError());

    // Stage 4: all regions converge
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Map<String, Integer> v = getColoVersions(parentControllerClient, storeName);
      for (String r: allRegions) {
        Assert.assertEquals(
            (int) v.get(r),
            expectedVersion,
            "Region " + r + " should have v" + expectedVersion + " after swap");
      }
    });
  }

  private Map<String, Integer> getColoVersions(ControllerClient client, String storeName) {
    return client.getStore(storeName).getStore().getColoToCurrentVersions();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testVersionStatusOnControllerRestart() throws Exception {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testVersionStatusOnControllerRestart");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setTargetRegionSwapWaitTime(5);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAMES[0], keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();

      // Start push job in separate thread so we don't wait until version is swapped
      props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      Thread runVpjThread = new Thread(() -> {
        IntegrationTestPushUtils.runVPJ(props);
      });
      runVpjThread.start();

      // Wait for ingestion to finish
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Get the dc-1 cluster and controller
      List<VeniceMultiClusterWrapper> childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
      VeniceMultiClusterWrapper dc1Region = childDatacenters.get(1);
      VeniceClusterWrapper dc1Cluster = dc1Region.getClusters().get(CLUSTER_NAMES[0]);
      String dc1ControllerUrl = dc1Region.getControllerConnectString();

      // Create a controller client for dc-1 to check version status
      try (ControllerClient dc1ControllerClient = new ControllerClient(CLUSTER_NAMES[0], dc1ControllerUrl)) {

        // Verify version status is PUSHED in dc-1 BEFORE controller restart
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          StoreResponse storeResponse = dc1ControllerClient.getStore(storeName);
          Assert.assertFalse(storeResponse.isError(), "Failed to get store: " + storeResponse.getError());
          StoreInfo storeInfo = storeResponse.getStore();
          Optional<Version> versionOpt = storeInfo.getVersion(1);
          Assert.assertTrue(versionOpt.isPresent(), "Version 1 should exist");
          VersionStatus statusBeforeRestart = versionOpt.get().getStatus();
          Assert.assertEquals(
              statusBeforeRestart,
              VersionStatus.PUSHED,
              "Version status in dc-1 should be PUSHED before controller restart");
        });

        // Restart the dc-1 controller to trigger ZK refresh
        VeniceControllerWrapper dc1Controller = dc1Cluster.getLeaderVeniceController();
        int controllerPort = dc1Controller.getPort();
        dc1Cluster.stopVeniceController(controllerPort);
        dc1Cluster.restartVeniceController(controllerPort);

        // Wait for controller to be ready
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          StoreResponse storeResponse =
              new ControllerClient(CLUSTER_NAMES[0], dc1Region.getControllerConnectString()).getStore(storeName);
          Assert.assertFalse(storeResponse.isError(), "Controller not ready: " + storeResponse.getError());
        });

        // Verify version status is still PUSHED in dc-1 AFTER controller restart
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          StoreResponse storeResponse =
              new ControllerClient(CLUSTER_NAMES[0], dc1Region.getControllerConnectString()).getStore(storeName);
          Assert.assertFalse(storeResponse.isError(), "Failed to get store: " + storeResponse.getError());
          StoreInfo storeInfo = storeResponse.getStore();
          Optional<Version> versionOpt = storeInfo.getVersion(1);
          Assert.assertTrue(versionOpt.isPresent(), "Version 1 should exist");
          VersionStatus statusAfterRestart = versionOpt.get().getStatus();
          Assert.assertEquals(statusAfterRestart, VersionStatus.PUSHED);
        });

      }
    }
  }
}
