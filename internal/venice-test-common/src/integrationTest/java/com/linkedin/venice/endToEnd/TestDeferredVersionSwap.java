package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_LIST;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.MockStoreLifecycleHooks;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.meta.LifecycleHooksRecord;
import com.linkedin.venice.meta.LifecycleHooksRecordImpl;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * This test class is used to test the deferred version swap feature with targeted region push enabled
 * using the {@link com.linkedin.venice.controller.DeferredVersionSwapService}
 */
public class TestDeferredVersionSwap extends AbstractMultiRegionTest {
  private static final String REGION1 = "dc-0";
  private static final String REGION2 = "dc-1";
  private static final String REGION3 = "dc-2";
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 2).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  private static final int TEST_TIMEOUT = 120_000;

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
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS, 1000);
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED, true);
    return controllerProps;
  }

  @DataProvider(name = "regionsProvider")
  public Object[][] regionsProvider() {
    Set<String> singleRegion = new HashSet<>();
    singleRegion.add(REGION1);

    Set<String> twoRegions = new HashSet<>();
    twoRegions.add(REGION1);
    twoRegions.add(REGION2);

    return new Object[][] { { RegionUtils.composeRegionList(singleRegion) },
        { RegionUtils.composeRegionList(twoRegions) }, };
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "regionsProvider")
  public void testDeferredVersionSwap(String targetRegions) throws Exception {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwap");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms =
        new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true).setTargetRegionSwapWaitTime(60);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreWithRetry(CLUSTER_NAMES[0], keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms);

      props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      props.put(TARGETED_REGION_PUSH_LIST, targetRegions);
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
        assertDeferredSwapStages(parentControllerClient, storeName, 1, targetRegions);

        // Wait for VPJ to complete naturally after swap release
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
  public void testDeferredVersionSwapWithVersionMismatch() throws Exception {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwap");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true);
    storeParms.setTargetRegionSwapWaitTime(60);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreWithRetry(CLUSTER_NAMES[0], keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms);

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

        // Forcibly mark parent version and child status as ONLINE to confirm that version swap will still happen if
        // non-target regions are not swapped yet
        Admin admin =
            multiRegionMultiClusterWrapper.getLeaderParentControllerWithRetries(CLUSTER_NAMES[0]).getVeniceAdmin();
        updateVersionStatus(admin, storeName, VersionStatus.ONLINE, parentControllerClient);

        for (VeniceMultiClusterWrapper childDatacenter: childDatacenters) {
          VeniceHelixAdmin childAdmin = childDatacenter.getLeaderController(CLUSTER_NAMES[0]).getVeniceHelixAdmin();
          ControllerClient childControllerClient =
              new ControllerClient(CLUSTER_NAMES[0], childDatacenter.getControllerConnectString());
          updateVersionStatus(childAdmin, storeName, VersionStatus.ONLINE, childControllerClient);
        }

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

  @DataProvider(name = "validationsProvider")
  public Object[][] validationsProvider() {
    return new Object[][] { { StoreVersionLifecycleEventOutcome.PROCEED.toString(), 2 },
        { StoreVersionLifecycleEventOutcome.ROLLBACK.toString(), 1 } };
  }

  @Test(timeOut = TEST_TIMEOUT * 2, dataProvider = "validationsProvider")
  public void testDeferredVersionSwapWithValidation(String validationOutcome, int targetVersion) throws Exception {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwap");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    List<LifecycleHooksRecord> lifecycleHooks = new ArrayList<>();
    Map<String, String> lifecycleHooksParams = new HashMap<>();
    lifecycleHooksParams.put("outcome", validationOutcome);
    lifecycleHooks.add(new LifecycleHooksRecordImpl(MockStoreLifecycleHooks.class.getName(), lifecycleHooksParams));
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setStoreLifecycleHooks(lifecycleHooks);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreWithRetry(CLUSTER_NAMES[0], keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms);

      // Run first push in background to avoid hanging the test thread if controller is slow
      runVPJInBackground(props, storeName, 1, parentControllerClient);

      // Set targetRegionSwapWaitTime after first push so it doesn't interfere with non-targeted push
      parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setTargetRegionSwapWaitTime(60));

      // Start push job with target region push enabled (in background since VPJ blocks until swap)
      props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      props.put(TARGETED_REGION_PUSH_LIST, REGION1);
      Thread vpjThread = new Thread(() -> IntegrationTestPushUtils.runVPJ(props));
      vpjThread.setDaemon(true);
      vpjThread.start();
      try {
        if (targetVersion == 2) {
          // PROCEED path: wait for push completion then validate deferred swap stages
          TestUtils.waitForNonDeterministicPushCompletion(
              Version.composeKafkaTopic(storeName, 2),
              parentControllerClient,
              60,
              TimeUnit.SECONDS);
          assertDeferredSwapStages(parentControllerClient, storeName, 2, REGION1);
        } else {
          // ROLLBACK path: wait for push data ingestion to complete
          TestUtils.waitForNonDeterministicPushCompletion(
              Version.composeKafkaTopic(storeName, 2),
              parentControllerClient,
              60,
              TimeUnit.SECONDS);

          // Wait for target region to swap to v2 (DeferredVersionSwapService processes target first)
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
            Map<String, Integer> versions = getColoVersions(parentControllerClient, storeName);
            Assert.assertEquals((int) versions.get(REGION1), 2, "Target region should have v2");
          });

          // Release wait time so DeferredVersionSwapService fires immediately
          // -> lifecycle hooks return ROLLBACK -> version killed
          parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setTargetRegionSwapWaitTime(0));

          TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
            StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
            Optional<Version> v2 = parentStore.getVersion(2);
            Assert.assertTrue(v2.isPresent(), "Version 2 should exist");
            Assert.assertEquals(v2.get().getStatus(), VersionStatus.KILLED);
          });

          Map<String, Integer> coloVersions = getColoVersions(parentControllerClient, storeName);
          coloVersions.forEach((colo, version) -> {
            Assert.assertEquals((int) version, 1, "Region " + colo + " should still be on v1 after rollback");
          });
        }

        vpjThread.join(30_000);
      } finally {
        if (vpjThread.isAlive()) {
          vpjThread.interrupt();
          vpjThread.join(5_000);
        }
      }
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

  /**
   * Retries store creation to handle transient controller unavailability between tests.
   * The embedded controller may still be processing background operations (topic cleanup,
   * metadata sync) from a previous test when the next test tries to create a store.
   */
  private void createStoreWithRetry(
      String clusterName,
      String keySchemaStr,
      String valueSchemaStr,
      Properties props,
      UpdateStoreQueryParams storeParams) {
    TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, () -> {
      try {
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParams).close();
      } catch (Exception e) {
        throw new AssertionError("Store creation failed, retrying: " + e.getMessage(), e);
      }
    });
  }

  private void runVPJInBackground(
      Properties props,
      String storeName,
      int version,
      ControllerClient parentControllerClient) throws Exception {
    Thread vpjThread = new Thread(() -> IntegrationTestPushUtils.runVPJ(props));
    vpjThread.setDaemon(true);
    vpjThread.start();
    try {
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, version),
          parentControllerClient,
          90,
          TimeUnit.SECONDS);
      vpjThread.join(30_000);
    } finally {
      if (vpjThread.isAlive()) {
        vpjThread.interrupt();
        vpjThread.join(5_000);
      }
    }
  }

  private void updateVersionStatus(
      Admin admin,
      String storeName,
      VersionStatus status,
      ControllerClient controllerClient) {
    ReadWriteStoreRepository storeRepository =
        admin.getHelixVeniceClusterResources(CLUSTER_NAMES[0]).getStoreMetadataRepository();
    Store store = storeRepository.getStore(storeName);
    store.updateVersionStatus(1, status);
    storeRepository.updateStore(store);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      StoreInfo parentStore = controllerClient.getStore(storeName).getStore();
      Assert.assertEquals(parentStore.getVersion(1).get().getStatus(), status);
    });
  }

}
