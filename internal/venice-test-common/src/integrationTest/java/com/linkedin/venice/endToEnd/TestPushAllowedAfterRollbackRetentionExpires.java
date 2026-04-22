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

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
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
 * Companion to {@link TestPushBlockedByRollbackOriginGuard}: verifies that once the rolled-back
 * retention window elapses, a new push is allowed through and becomes the next current version.
 *
 * <p>Retention is tuned to 5 seconds so the test can wait past it within its lifetime.
 */
public class TestPushAllowedAfterRollbackRetentionExpires extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT = 180_000; // ms
  private static final long ROLLED_BACK_RETENTION_MS = TimeUnit.SECONDS.toMillis(5);

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
    controllerProps.put(CONTROLLER_ROLLED_BACK_VERSION_RETENTION_MS, ROLLED_BACK_RETENTION_MS);
    return controllerProps;
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testPushSucceedsAfterRolledBackRetentionExpires() throws IOException, InterruptedException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("pushAllowedAfterRetention");

    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    try (ControllerClient parentControllerClient = createStoreForJob(
        CLUSTER_NAME,
        "\"string\"",
        NAME_RECORD_V3_SCHEMA.toString(),
        props,
        new UpdateStoreQueryParams())) {
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

      // Rollback: v1 becomes current, v2 becomes ROLLED_BACK.
      ControllerResponse rollbackResponse = parentControllerClient.rollbackToBackupVersion(storeName);
      assertFalse(rollbackResponse.isError(), "rollback failed: " + rollbackResponse.getError());

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse resp = parentControllerClient.getStore(storeName);
        assertFalse(resp.isError(), "getStore error: " + resp.getError());
        assertTrue(
            resp.getStore().getVersion(2).isPresent()
                && resp.getStore().getVersion(2).get().getStatus() == VersionStatus.ROLLED_BACK,
            "Expected v2 to be ROLLED_BACK after rollback, got: " + resp.getStore().getVersion(2));
      });

      // Wait past the rolled-back retention so the guard allows the next push through.
      // Buffer added to account for clock skew between latestVersionPromoteToCurrentTimestamp
      // (set at rollback time) and this wait's wall clock.
      Thread.sleep(ROLLED_BACK_RETENTION_MS + TimeUnit.SECONDS.toMillis(2));

      // v3 push should now succeed; the rollback-origin guard must not block it past retention.
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
    }
  }
}
