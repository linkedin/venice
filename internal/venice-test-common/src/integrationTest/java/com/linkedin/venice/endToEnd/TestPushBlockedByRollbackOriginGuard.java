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

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
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
import org.testng.annotations.Test;


/**
 * Integration test for {@link com.linkedin.venice.controller.VeniceHelixAdmin#checkRollbackOriginVersionCapacityForNewPush}.
 *
 * <p>After a rollback, v2 becomes ROLLED_BACK (on the parent, once propagated) and v1 becomes
 * current. A new push within the rolled-back retention window should be rejected with a message
 * that tells operators to wait for the rolled-back version to be cleaned up.
 *
 * <p>Config tuning: the min backup version cleanup delay is left at the default (0) so the
 * generic pending-deletion guard does NOT fire first. The rolled-back retention is set to 1 minute
 * so the rollback-origin guard reliably fires within the test lifetime.
 *
 * <p>This test is in its own class (separate from {@link TestPushBlockedWithinMinCleanupDelay}) so
 * the two guards can be exercised in isolation with their respective configs.
 */
public class TestPushBlockedByRollbackOriginGuard extends AbstractMultiRegionTest {
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
    // Override the default 24h retention so the rollback-origin guard reliably fires during the test.
    // CONTROLLER_BACKUP_VERSION_MIN_CLEANUP_DELAY_MS defaults to 0 in the test harness, which means
    // the other (min-delay) guard does not fire here — ensuring this test exercises the rollback-origin
    // guard and nothing else.
    controllerProps.put(CONTROLLER_ROLLED_BACK_VERSION_RETENTION_MS, TimeUnit.MINUTES.toMillis(30));
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

      // Attempt v3 push; the parent-level rollback-origin guard should reject it synchronously
      // so the VPJ throws with the guard's distinctive message.
      try (VenicePushJob job = new VenicePushJob("venice-push-job-v3-" + storeName, props)) {
        job.run();
        fail("Expected VPJ to fail — rollback-origin guard should block v3 push within retention");
      } catch (Exception e) {
        String message = e.getMessage();
        assertTrue(
            message != null && message.contains("Retry after the rolled-back version is cleaned up"),
            "Expected rollback-origin guard message, got: " + message);
      }

      StoreResponse finalStoreResponse = parentControllerClient.getStore(storeName);
      assertFalse(finalStoreResponse.isError());
      assertFalse(
          finalStoreResponse.getStore().getVersion(3).isPresent(),
          "Version 3 should NOT have been created after guard rejection");
    }
  }
}
