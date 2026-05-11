package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_ROLLED_BACK_VERSION_RETENTION_MS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.controller.HelixVeniceClusterResources;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
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
 * Integration test for {@link com.linkedin.venice.controller.VeniceHelixAdmin#checkRollbackOriginVersionCapacityForNewPush}.
 *
 * <p>After a rollback, v2 becomes ROLLED_BACK (on the parent, once propagated) and v1 becomes
 * current. The test exercises both sides of the rollback-origin retention window:
 * <ul>
 *   <li>A push attempted <em>within</em> the retention window is rejected with the guard's
 *       distinctive message.</li>
 *   <li>A push attempted <em>after</em> the retention window has elapsed is allowed through
 *       and produces a new version.</li>
 * </ul>
 *
 * <p>Config tuning: the min backup version cleanup delay is left at the default (0) so the
 * generic pending-deletion guard does NOT fire first. The rolled-back retention is set to 30
 * minutes — long enough that the within-retention block phase isn't time-sensitive. To exercise
 * the post-retention path, the test rewinds the store's {@code latestVersionPromoteToCurrentTimestamp}
 * directly via the parent controller's repository so the retention check sees an "elapsed"
 * window deterministically, without sleeping or relying on wall-clock timing.
 *
 * <p>This test is in its own class (separate from {@link TestPushBlockedWithinMinCleanupDelay}) so
 * the two guards can be exercised in isolation with their respective configs.
 */
public class TestPushBlockedByRollbackOriginGuard extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT = 240_000; // ms
  private static final long ROLLED_BACK_RETENTION_MS = TimeUnit.MINUTES.toMillis(30);

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
    // Override the default 24h retention so the rollback-origin guard reliably fires during the test.
    // CONTROLLER_BACKUP_VERSION_MIN_CLEANUP_DELAY_MS defaults to 0 in the test harness, which means
    // the other (min-delay) guard does not fire here — ensuring this test exercises the rollback-origin
    // guard and nothing else.
    controllerProps.put(CONTROLLER_ROLLED_BACK_VERSION_RETENTION_MS, ROLLED_BACK_RETENTION_MS);
    return controllerProps;
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testPushBlockedAfterRollbackWithinRolledBackRetention() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("pushBlockedByRollbackOrigin");

    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    try (ControllerClient parentControllerClient = createStoreForJob(
        CLUSTER_NAME,
        "\"string\"",
        NAME_RECORD_V3_SCHEMA.toString(),
        props,
        new UpdateStoreQueryParams())) {
      // Push v1, v2 so the store has two versions to roll back between.
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

      // Rollback: v1 becomes current, v2 becomes ROLLED_BACK (PR #2688).
      ControllerResponse rollbackResponse = parentControllerClient.rollbackToBackupVersion(storeName);
      assertFalse(rollbackResponse.isError(), "rollback failed: " + rollbackResponse.getError());

      // Wait for the parent's view to reflect v2 = ROLLED_BACK so the rollback-origin guard has
      // something to find when the VPJ calls addVersion. Without this wait the parent's in-memory
      // state can lag briefly behind the rollback admin message propagation.
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse resp = parentControllerClient.getStore(storeName);
        assertFalse(resp.isError(), "getStore error: " + resp.getError());
        assertTrue(
            resp.getStore().getVersion(2).isPresent()
                && resp.getStore().getVersion(2).get().getStatus() == VersionStatus.ROLLED_BACK,
            "Expected v2 to be ROLLED_BACK after rollback, got: " + resp.getStore().getVersion(2));
      });

      // Phase 1: within retention — attempt v3 push; the parent-level rollback-origin guard should
      // reject it synchronously so the VPJ throws with the guard's distinctive message.
      try (VenicePushJob job = new VenicePushJob("venice-push-job-v3-blocked-" + storeName, props)) {
        job.run();
        fail("Expected VPJ to fail — rollback-origin guard should block v3 push within retention");
      } catch (Exception e) {
        String message = e.getMessage();
        assertTrue(
            message != null && message.contains("Retry after the rolled-back version is cleaned up"),
            "Expected rollback-origin guard message, got: " + message);
      }

      StoreResponse blockedStoreResponse = parentControllerClient.getStore(storeName);
      assertFalse(blockedStoreResponse.isError());
      assertFalse(
          blockedStoreResponse.getStore().getVersion(3).isPresent(),
          "Version 3 should NOT have been created while v2's retention is active");

      // Phase 2: simulate retention having elapsed by rewinding the store's
      // latestVersionPromoteToCurrentTimestamp far enough into the past that
      // (now > latestPromoteTs + retentionMs) holds
      long rewindTo = System.currentTimeMillis() - ROLLED_BACK_RETENTION_MS - TimeUnit.MINUTES.toMillis(1);
      VeniceParentHelixAdmin parentAdmin = (VeniceParentHelixAdmin) parentController.getVeniceAdmin();
      rewindLatestPromoteTimestamp(
          parentAdmin.getVeniceHelixAdmin().getHelixVeniceClusterResources(CLUSTER_NAME),
          storeName,
          rewindTo);
      for (VeniceControllerWrapper childController: childDatacenters.get(0).getControllers().values()) {
        rewindLatestPromoteTimestamp(
            childController.getVeniceHelixAdmin().getHelixVeniceClusterResources(CLUSTER_NAME),
            storeName,
            rewindTo);
      }
      // Wait for the rewind to be reflected via the parent's getStore API.
      TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
        StoreResponse resp = parentControllerClient.getStore(storeName);
        assertFalse(resp.isError(), "getStore error: " + resp.getError());
        assertTrue(
            resp.getStore().getLatestVersionPromoteToCurrentTimestamp() <= rewindTo,
            "Expected latestPromoteTs to be rewound to " + rewindTo + ", got: "
                + resp.getStore().getLatestVersionPromoteToCurrentTimestamp());
      });

      // Phase 3: post-retention — the same v3 push should now succeed. The guard's early-return
      // path fires (currentTimeMs > retentionExpiresAt) and addVersion proceeds normally.
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 3),
          parentControllerClient,
          60,
          TimeUnit.SECONDS);

      StoreResponse finalStoreResponse = parentControllerClient.getStore(storeName);
      assertFalse(finalStoreResponse.isError());
      assertTrue(
          finalStoreResponse.getStore().getVersion(3).isPresent(),
          "Version 3 should have been created after retention expired");

      // Phase 4: regression for the parent-currentVersion-decrement + filter-tightening fix.
      // v3's promotion in Phase 3 just re-bumped latestVersionPromoteToCurrentTimestamp, re-arming
      // the retention window against the stale ROLLED_BACK v2 (which lingers in parent metadata
      // because parent retains more versions than children). Pre-fix, the rollback-origin filter
      // would match v2 again and block v4. Post-fix, v2.number=2 <= currentVersion=3 ages out.
      StoreResponse preV4 = parentControllerClient.getStore(storeName);
      assertFalse(preV4.isError());
      assertTrue(
          preV4.getStore().getVersion(2).isPresent()
              && preV4.getStore().getVersion(2).get().getStatus() == VersionStatus.ROLLED_BACK,
          "Phase 4 precondition: v2 must still be ROLLED_BACK on parent so the stale-entry "
              + "age-out path is exercised; got: " + preV4.getStore().getVersion(2));
      long retentionExpiresAt = preV4.getStore().getLatestVersionPromoteToCurrentTimestamp() + ROLLED_BACK_RETENTION_MS;
      assertTrue(
          System.currentTimeMillis() < retentionExpiresAt,
          "Phase 4 precondition: retention must be re-armed by v3's promotion (otherwise v4 "
              + "would pass via the early-return path, not the filter age-out). latestPromoteTs="
              + preV4.getStore().getLatestVersionPromoteToCurrentTimestamp() + ", retentionExpiresAt="
              + retentionExpiresAt);

      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 4),
          parentControllerClient,
          60,
          TimeUnit.SECONDS);

      StoreResponse v4StoreResponse = parentControllerClient.getStore(storeName);
      assertFalse(v4StoreResponse.isError());
      assertTrue(
          v4StoreResponse.getStore().getVersion(4).isPresent()
              && v4StoreResponse.getStore().getVersion(4).get().getStatus() == VersionStatus.ONLINE,
          "Version 4 should have been created and ONLINE — stale ROLLED_BACK v2 must not "
              + "re-block subsequent pushes once currentVersion has moved past it");
    }
  }

  private static void rewindLatestPromoteTimestamp(
      HelixVeniceClusterResources resources,
      String storeName,
      long rewindTo) {
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      ReadWriteStoreRepository repo = resources.getStoreMetadataRepository();
      Store store = repo.getStore(storeName);
      store.setLatestVersionPromoteToCurrentTimestamp(rewindTo);
      repo.updateStore(store);
    }
  }
}
