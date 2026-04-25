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

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.meta.Version;
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
 * Integration test for {@link com.linkedin.venice.controller.VeniceHelixAdmin#checkBackupVersionCleanupCapacityForNewPush}.
 *
 * <p>After a rollback, v2 becomes ERROR and v1 becomes current. If an operator starts a new push
 * within the min backup version cleanup delay, the controller's capacity guard should reject the
 * push so the VPJ surfaces a clear error instead of hanging.
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
  public void testPushBlockedAfterRollbackWithinMinCleanupDelay() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("pushBlockedAfterRollback");

    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    try (ControllerClient parentControllerClient = createStoreForJob(
        CLUSTER_NAME,
        "\"string\"",
        NAME_RECORD_V3_SCHEMA.toString(),
        props,
        new UpdateStoreQueryParams())) {
      // Push v1 and v2 via VPJ so the store has a real current version to roll back from.
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

      // Rollback: v1 becomes current, v2 becomes ERROR, latestVersionPromoteToCurrentTimestamp resets.
      ControllerResponse rollbackResponse = parentControllerClient.rollbackToBackupVersion(storeName);
      assertFalse(rollbackResponse.isError(), "rollback failed: " + rollbackResponse.getError());

      // Attempt a real VPJ v3 push. The capacity guard should reject it because v2 (ERROR) is
      // pending deletion and latestVersionPromoteToCurrentTimestamp was just reset by the rollback.
      try (VenicePushJob job = new VenicePushJob("venice-push-job-v3-" + storeName, props)) {
        job.run();
        fail("Expected VPJ to fail — capacity guard should block v3 push after rollback within min delay");
      } catch (Exception e) {
        // Verify the failure is specifically from the capacity guard, not some unrelated issue.
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
}
