package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_DELETION_SLEEP_MS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_MIN_CLEANUP_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_REPLICA_REDUCTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_RETENTION_BASED_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.controller.StoreBackupVersionCleanupService;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
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
    controllerProps.put(CONTROLLER_BACKUP_VERSION_DELETION_SLEEP_MS, 10);
    controllerProps.put(CONTROLLER_BACKUP_VERSION_MIN_CLEANUP_DELAY_MS, 10);
    controllerProps.put(CONTROLLER_BACKUP_VERSION_RETENTION_BASED_CLEANUP_ENABLED, "true");
    controllerProps.put(CONTROLLER_BACKUP_VERSION_REPLICA_REDUCTION_ENABLED, "true");
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

  @Test(timeOut = TEST_TIMEOUT)
  public void testRolledBackVersionDeletion() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAME, keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();

      // Push v1 and v2
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

      // Rollback to v1 — v2 should become ROLLED_BACK
      ControllerResponse rollbackResponse = parentControllerClient.rollbackToBackupVersion(storeName);
      Assert.assertFalse(rollbackResponse.isError(), "Rollback failed: " + rollbackResponse.getError());

      // Wait for the child to execute the rollback and mark v2 as ROLLED_BACK
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Store store = veniceHelixAdmin.getStore(CLUSTER_NAME, storeName);
        assertEquals(
            store.getCurrentVersion(),
            1,
            "Current version should be 1 after rollback. " + describeStore(store));
        Version v2 = store.getVersion(2);
        assertNotNull(v2, "Version 2 should still exist immediately after rollback. " + describeStore(store));
        assertEquals(
            v2.getStatus(),
            VersionStatus.ROLLED_BACK,
            "Version 2 should be ROLLED_BACK. " + describeStore(store));
      });

      // Set rolled-back retention to a very short duration so the cleanup service picks it up
      StoreBackupVersionCleanupService.setRolledBackVersionRetentionMs(10);
      try {
        // Wait for the cleanup service to delete v2
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          Store store = veniceHelixAdmin.getStore(CLUSTER_NAME, storeName);
          assertNull(
              store.getVersion(2),
              "Version 2 should be deleted after rolled-back retention. " + describeStore(store));
          assertEquals(store.getCurrentVersion(), 1, "Current version should still be 1. " + describeStore(store));
        });
      } finally {
        StoreBackupVersionCleanupService.setRolledBackVersionRetentionMs(TimeUnit.HOURS.toMillis(24));
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRolledBackRepushVersionDeletion() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAME, keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();

      // Push v1
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          childControllerClient,
          30,
          TimeUnit.SECONDS);

      // Prevent the cleanup service from deleting v1 (the repush source) before we rollback
      StoreBackupVersionCleanupService.setWaitTimeDeleteRepushSourceVersion(TimeUnit.HOURS.toMillis(1));

      // Repush to v2 (source=v1)
      props.put(SOURCE_KAFKA, "true");
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          childControllerClient,
          30,
          TimeUnit.SECONDS);

      // Verify v2 is current and is a repush
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Store store = veniceHelixAdmin.getStore(CLUSTER_NAME, storeName);
        assertEquals(store.getCurrentVersion(), 2, "Current version should be 2. " + describeStore(store));
        Version v2 = store.getVersion(2);
        assertNotNull(v2, "Version 2 should exist. " + describeStore(store));
        assertEquals(v2.getRepushSourceVersion(), 1, "Version 2 should be repushed from v1");
      });
      try {
        // Rollback to v1 — v2 (the repush) should become ROLLED_BACK
        ControllerResponse rollbackResponse = parentControllerClient.rollbackToBackupVersion(storeName);
        Assert.assertFalse(rollbackResponse.isError(), "Rollback failed: " + rollbackResponse.getError());

        // Wait for the child to execute the rollback
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          Store store = veniceHelixAdmin.getStore(CLUSTER_NAME, storeName);
          assertEquals(
              store.getCurrentVersion(),
              1,
              "Current version should be 1 after rollback. " + describeStore(store));
          Version v2 = store.getVersion(2);
          assertNotNull(v2, "Version 2 should still exist after rollback. " + describeStore(store));
          assertEquals(
              v2.getStatus(),
              VersionStatus.ROLLED_BACK,
              "Version 2 (repush) should be ROLLED_BACK. " + describeStore(store));
        });

        // Set rolled-back retention to a very short duration so the cleanup service picks it up
        StoreBackupVersionCleanupService.setRolledBackVersionRetentionMs(10);
        // Wait for the cleanup service to delete v2
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          Store store = veniceHelixAdmin.getStore(CLUSTER_NAME, storeName);
          assertNull(
              store.getVersion(2),
              "Rolled-back repush version 2 should be deleted after retention. " + describeStore(store));
          assertEquals(store.getCurrentVersion(), 1, "Current version should still be 1. " + describeStore(store));
        });
      } finally {
        StoreBackupVersionCleanupService.setRolledBackVersionRetentionMs(TimeUnit.HOURS.toMillis(24));
        StoreBackupVersionCleanupService.setWaitTimeDeleteRepushSourceVersion(10);
      }
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
