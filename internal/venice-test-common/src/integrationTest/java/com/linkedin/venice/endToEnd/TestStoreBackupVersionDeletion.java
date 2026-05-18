package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONCURRENT_PUSH_DETECTION_STRATEGY;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_DELETION_SLEEP_MS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_MIN_CLEANUP_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_REPLICA_REDUCTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_RETENTION_BASED_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_LIST;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.controller.StoreBackupVersionCleanupService;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestStoreBackupVersionDeletion extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT = 120_000; // ms

  private VeniceHelixAdmin veniceHelixAdmin;
  private ControllerClient childControllerClient;

  @Override
  protected int getNumberOfRegions() {
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
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 1);
    controllerProps.put(DEFAULT_PARTITION_SIZE, 10);
    controllerProps
        .setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    controllerProps.put(CONTROLLER_BACKUP_VERSION_DELETION_SLEEP_MS, 10);
    controllerProps.put(CONTROLLER_BACKUP_VERSION_MIN_CLEANUP_DELAY_MS, 10);
    controllerProps.put(CONTROLLER_BACKUP_VERSION_RETENTION_BASED_CLEANUP_ENABLED, "true");
    controllerProps.put(CONTROLLER_BACKUP_VERSION_REPLICA_REDUCTION_ENABLED, "true");
    // Required for the prior-current preservation test which uses a target-region push w/ deferred swap.
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS, 100);
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED, true);
    // Use parent-version-status-based concurrent-push detection so a KILLED prior version
    // releases the future-version guard at VeniceParentHelixAdmin:1964. The default TOPIC_BASED_ONLY
    // strategy always treats target-region-deferred-swap topics as "in flight" regardless of KILLED
    // status (VeniceParentHelixAdmin:1481-1487), which would block the v3 push in this scenario.
    controllerProps.put(CONCURRENT_PUSH_DETECTION_STRATEGY, "PARENT_VERSION_STATUS_ONLY");
    return controllerProps;
  }

  @Override
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    StoreBackupVersionCleanupService.setWaitTimeDeleteRepushSourceVersion(10);
    super.setUp();
    veniceHelixAdmin =
        (VeniceHelixAdmin) childDatacenters.get(0).getControllers().values().iterator().next().getVeniceAdmin();
    childControllerClient = new ControllerClient(CLUSTER_NAME, childDatacenters.get(0).getControllerConnectString());
  }

  @Override
  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(childControllerClient);
    super.cleanUp();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRepushBackupVersionDeletion() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAME, keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
      props.put(SOURCE_KAFKA, "true");
      IntegrationTestPushUtils.runVPJ(props);
      // Wait for push completion on the CHILD controller — this ensures the child has promoted v3 to current.
      // Using the parent controller here is insufficient: the parent may report completion before the child
      // promotes v3, and the backup cleanup service (running on the child) needs currentVersion=3 to use
      // the short repush retention (10ms) instead of the default 7-day retention.
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 3),
          childControllerClient,
          30,
          TimeUnit.SECONDS);
      // repush sourced from v2, so the backup cleanup service should delete v2.
      // v1 may also be cleaned up depending on timing, so only assert v2 is gone and v3 (current) remains.
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        Store store = veniceHelixAdmin.getStore(CLUSTER_NAME, storeName);
        assertNull(store.getVersion(2), "Version 2 should be deleted (repush source). " + describeStore(store));
        assertEquals(store.getCurrentVersion(), 3, "Current version should be 3. " + describeStore(store));
      });
    }
  }

  private static String describeStore(Store store) {
    return "currentVersion=" + store.getCurrentVersion() + ", versions="
        + store.getVersions().stream().map(v -> "v" + v.getNumber()).collect(java.util.stream.Collectors.joining(","));
  }

  /**
   * Reproduces VENG-12676 end-to-end: push v1, push v2 with target-region={@code dc-0} and deferred
   * swap, kill v2 before dc-1 swaps, push v3, verify dc-1 cleanup preserves v1 (prior current) and
   * reaps the lingering v2. dc-1's child kill handler hits the bootstrap-completed early-return at
   * VeniceHelixAdmin.killOfflinePush:8649 and leaves v2 at PUSHED — the upstream bug that produces
   * the broken state. If that bug is fixed in a separate change, the v2-still-PUSHED-after-kill
   * assertion below will fail and this test will need to inject the lingering state differently.
   */
  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testCleanupPreservesPriorCurrentVersionAcrossDeferredSwapKill() throws IOException {
    final String targetRegion = "dc-0";
    final String testRegion = "dc-1";
    int targetIdx = -1;
    int testIdx = -1;
    for (int i = 0; i < childDatacenters.size(); i++) {
      if (targetRegion.equals(childDatacenters.get(i).getRegionName())) {
        targetIdx = i;
      } else if (testRegion.equals(childDatacenters.get(i).getRegionName())) {
        testIdx = i;
      }
    }
    Assert.assertTrue(targetIdx >= 0 && testIdx >= 0, "Expected dc-0 and dc-1 in childDatacenters");
    VeniceHelixAdmin testRegionAdmin =
        (VeniceHelixAdmin) childDatacenters.get(testIdx).getControllers().values().iterator().next().getVeniceAdmin();

    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms =
        new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true).setTargetRegionSwapWaitTime(60);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAME, keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();

      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Store store = testRegionAdmin.getStore(CLUSTER_NAME, storeName);
        assertEquals(store.getCurrentVersion(), 1, "v1 should be current in " + testRegion);
      });

      Properties v2Props = (Properties) props.clone();
      v2Props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      v2Props.put(TARGETED_REGION_PUSH_LIST, targetRegion);
      AtomicReference<Throwable> v2VpjError = new AtomicReference<>();
      Thread v2Thread = new Thread(() -> {
        try {
          IntegrationTestPushUtils.runVPJ(v2Props);
        } catch (Throwable t) {
          v2VpjError.set(t);
        }
      });
      v2Thread.setDaemon(true);
      v2Thread.start();

      try {
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          Store store = testRegionAdmin.getStore(CLUSTER_NAME, storeName);
          Version v2 = store.getVersion(2);
          assertNotNull(v2, "v2 should exist in " + testRegion + ". " + describeStore(store));
          Assert.assertEquals(
              v2.getStatus(),
              VersionStatus.PUSHED,
              "v2 should be PUSHED (deferred swap) in " + testRegion + ". " + describeStore(store));
          assertEquals(
              store.getCurrentVersion(),
              1,
              "current should still be v1 in " + testRegion + " (deferred). " + describeStore(store));
        });

        parentControllerClient.killOfflinePushJob(Version.composeKafkaTopic(storeName, 2));

        // Parent honors KILL; waiting for KILLED at parent releases the future-version guard at
        // VeniceParentHelixAdmin:1964 so the subsequent v3 push can proceed.
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          com.linkedin.venice.meta.StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
          Assert.assertEquals(parentStore.getVersion(2).get().getStatus(), VersionStatus.KILLED);
        });

        // Child kill no-op (the upstream bug): v2 stays PUSHED in dc-1 because the resource
        // already finished bootstrapping. If this fails, the kill handler has been fixed and the
        // test needs to inject the lingering state differently.
        TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
          Store store = testRegionAdmin.getStore(CLUSTER_NAME, storeName);
          Version v2 = store.getVersion(2);
          assertNotNull(v2, "v2 should still exist in " + testRegion + " after kill. " + describeStore(store));
          Assert.assertEquals(
              v2.getStatus(),
              VersionStatus.PUSHED,
              "v2 should remain PUSHED in " + testRegion + " after kill. " + describeStore(store));
        });
      } finally {
        v2Thread.join(5_000);
        if (v2Thread.isAlive()) {
          v2Thread.interrupt();
          v2Thread.join(5_000);
        }
      }

      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        Store store = testRegionAdmin.getStore(CLUSTER_NAME, storeName);
        Version v3 = store.getVersion(3);
        assertNotNull(v3, "v3 should exist in " + testRegion + ". " + describeStore(store));
        assertEquals(
            store.getCurrentVersion(),
            3,
            "v3 should be current in " + testRegion + ". " + describeStore(store));
        // v2 never became current in dc-1, so v3.previousCurrentVersion auto-stamps to 1.
        assertEquals(
            v3.getPreviousCurrentVersion(),
            1,
            "v3.previousCurrentVersion should be 1 in " + testRegion + ". " + describeStore(store));
      });

      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        Store store = testRegionAdmin.getStore(CLUSTER_NAME, storeName);
        assertNotNull(
            store.getVersion(1),
            "v1 (prior current) must be preserved in " + testRegion + ". " + describeStore(store));
        assertNull(
            store.getVersion(2),
            "v2 (stale PUSHED lingering) should be cleaned up in " + testRegion + ". " + describeStore(store));
        assertNotNull(
            store.getVersion(3),
            "v3 (current) must be preserved in " + testRegion + ". " + describeStore(store));
        assertEquals(store.getCurrentVersion(), 3, "current should still be v3. " + describeStore(store));
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBackupVersionReplicaReduction() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAME, keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          childControllerClient,
          30,
          TimeUnit.SECONDS);
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          childControllerClient,
          30,
          TimeUnit.SECONDS);
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 3),
          childControllerClient,
          30,
          TimeUnit.SECONDS);
      TestUtils.waitForNonDeterministicCompletion(
          30,
          TimeUnit.SECONDS,
          () -> veniceHelixAdmin.getIdealState(CLUSTER_NAME, Version.composeKafkaTopic(storeName, 2))
              .getMinActiveReplicas() == 2);
    }
  }

}
