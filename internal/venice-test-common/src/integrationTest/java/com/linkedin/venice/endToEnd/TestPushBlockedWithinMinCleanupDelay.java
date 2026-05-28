package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_MIN_CLEANUP_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.controller.HelixVeniceClusterResources;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


/**
 * Integration test for {@link com.linkedin.venice.controller.versionlifecycle.VersionLifecyclePolicy#checkBackupVersionCleanupCapacityForNewPush}.
 *
 * <p>The capacity guard rejects a new push when one or more backup versions are in a deletable
 * state (e.g., {@code KILLED} / {@code ERROR}) and the store is still within the configured
 * min backup version cleanup delay. This test sets up that scenario deterministically by
 * marking the backup version (v1) as {@code KILLED} via direct repository writes (rather than
 * relying on a real push failure or rollback, both of which interact with other guards). With
 * v2 as current and v1 as a KILLED backup, the guard fires on v1 as a backup pending deletion.
 *
 * <p>The min cleanup delay is set to 1 minute at class level so the guard reliably fires within
 * the test's lifetime. This is in its own test class so other classes don't inherit the delay.
 */
public class TestPushBlockedWithinMinCleanupDelay extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT = 180_000; // ms

  @Override
  protected int getNumberOfRegions() {
    return 1;
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
    // Override VeniceControllerWrapper's default (0) with a real value so the capacity guard fires.
    controllerProps.put(CONTROLLER_BACKUP_VERSION_MIN_CLEANUP_DELAY_MS, TimeUnit.MINUTES.toMillis(1));
    return controllerProps;
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testPushBlockedWhenBackupKilledWithinMinCleanupDelay() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("pushBlockedByMinCleanupDelay");

    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    try (ControllerClient parentControllerClient = createStoreForJob(
        CLUSTER_NAME,
        "\"string\"",
        NAME_RECORD_V3_SCHEMA.toString(),
        props,
        new UpdateStoreQueryParams())) {
      // Push v1 and v2 via VPJ so the store has two real versions.
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

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = parentControllerClient.getStore(storeName);
        assertFalse(storeResponse.isError(), "getStore error: " + storeResponse.getError());
        assertEquals(storeResponse.getStore().getCurrentVersion(), 2);
      });

      // Mark v1 as KILLED on every controller. KILLED is in canDelete-set, so retrieveVersionsToDelete
      // returns it, which is what the min-cleanup-delay guard uses to decide whether the store has
      // any backup version pending deletion. This simulates the operator-killed-the-old-push scenario
      // without depending on rollback (which produces ROLLED_BACK and is governed by a separate
      // dedicated retention guard).
      VeniceParentHelixAdmin parentAdmin = (VeniceParentHelixAdmin) parentController.getVeniceAdmin();
      markBackupVersionKilled(
          parentAdmin.getVeniceHelixAdmin().getHelixVeniceClusterResources(CLUSTER_NAME),
          storeName,
          1);
      for (VeniceControllerWrapper childController: childDatacenters.get(0).getControllers().values()) {
        markBackupVersionKilled(
            childController.getVeniceHelixAdmin().getHelixVeniceClusterResources(CLUSTER_NAME),
            storeName,
            1);
      }
      // Wait for the parent's getStore to reflect the KILLED status before triggering the v3 push.
      TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
        StoreResponse resp = parentControllerClient.getStore(storeName);
        assertFalse(resp.isError(), "getStore error: " + resp.getError());
        assertTrue(
            resp.getStore().getVersion(1).isPresent()
                && resp.getStore().getVersion(1).get().getStatus() == VersionStatus.KILLED,
            "Expected v1 to be KILLED, got: " + resp.getStore().getVersion(1));
      });

      // Attempt a real VPJ v3 push. The capacity guard should reject it because v1 (KILLED) is
      // pending deletion and latestVersionPromoteToCurrentTimestamp is recent (v2 was just promoted).
      try (VenicePushJob job = new VenicePushJob("venice-push-job-v3-" + storeName, props)) {
        job.run();
        fail("Expected VPJ to fail — capacity guard should block v3 push while a KILLED backup is within min delay");
      } catch (Exception e) {
        // VPJ wraps the controller error message into its own exception message verbatim.
        String message = e.getMessage();
        assertTrue(
            message != null && message.contains("pending deletion") && message.contains("min cleanup delay"),
            "Expected capacity-guard message, got: " + message);
      }

      // Sanity: the capacity guard rejected before any v3 version was created.
      StoreResponse finalStoreResponse = parentControllerClient.getStore(storeName);
      assertFalse(finalStoreResponse.isError());
      assertFalse(
          finalStoreResponse.getStore().getVersion(3).isPresent(),
          "Version 3 should NOT have been created after capacity-guard rejection");
    }
  }

  private static void markBackupVersionKilled(
      HelixVeniceClusterResources resources,
      String storeName,
      int versionNumber) {
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      ReadWriteStoreRepository repo = resources.getStoreMetadataRepository();
      Store store = repo.getStore(storeName);
      store.updateVersionStatus(versionNumber, VersionStatus.KILLED);
      repo.updateStore(store);
    }
  }
}
