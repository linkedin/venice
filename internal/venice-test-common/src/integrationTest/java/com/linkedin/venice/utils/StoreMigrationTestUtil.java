package com.linkedin.venice.utils;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;


/**
 * Utility class for store migration tests.
 */
public class StoreMigrationTestUtil {
  private static final boolean[] ABORT_MIGRATION_PROMPTS_OVERRIDE = { false, true, true };

  public static void startMigration(
      String controllerUrl,
      String storeName,
      String srcClusterName,
      String destClusterName) throws Exception {
    String[] startMigrationArgs = { "--migrate-store", "--url", controllerUrl, "--store", storeName, "--cluster-src",
        srcClusterName, "--cluster-dest", destClusterName };
    AdminTool.main(startMigrationArgs);
  }

  public static void checkMigrationStatus(
      String controllerUrl,
      String storeName,
      String srcClusterName,
      String destClusterName,
      AdminTool.PrintFunction printFunction) throws Exception {
    String[] checkMigrationStatusArgs = { "--migration-status", "--url", controllerUrl, "--store", storeName,
        "--cluster-src", srcClusterName, "--cluster-dest", destClusterName };
    AdminTool.checkMigrationStatus(AdminTool.getCommandLine(checkMigrationStatusArgs), printFunction);
  }

  public static void completeMigration(
      String controllerUrl,
      String storeName,
      String srcClusterName,
      String destClusterName,
      String fabric) {
    String[] completeMigration0 = { "--complete-migration", "--url", controllerUrl, "--store", storeName,
        "--cluster-src", srcClusterName, "--cluster-dest", destClusterName, "--fabric", fabric };

    try (ControllerClient destParentControllerClient = new ControllerClient(destClusterName, controllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        AdminTool.main(completeMigration0);
        // Store discovery should point to the new cluster after completing migration
        ControllerResponse discoveryResponse = destParentControllerClient.discoverCluster(storeName);
        Assert.assertEquals(discoveryResponse.getCluster(), destClusterName);
      });
    }
  }

  public static void endMigration(
      String parentControllerUrl,
      String childControllerUrl,
      String storeName,
      String srcClusterName,
      String destClusterName) throws Exception {
    String[] endMigration = { "--end-migration", "--url", parentControllerUrl, "--store", storeName, "--cluster-src",
        srcClusterName, "--cluster-dest", destClusterName };
    AdminTool.main(endMigration);

    try (ControllerClient srcControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
        ControllerClient destControllerClient = new ControllerClient(destClusterName, parentControllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        // Store should be deleted in source cluster. Store in destination cluster should not be migrating.
        StoreResponse storeResponse = srcControllerClient.getStore(storeName);
        Assert.assertNull(storeResponse.getStore());

        storeResponse = destControllerClient.getStore(storeName);
        Assert.assertNotNull(storeResponse.getStore());
        Assert.assertFalse(storeResponse.getStore().isMigrating());
        Assert.assertFalse(storeResponse.getStore().isMigrationDuplicateStore());
      });
    }
    if (childControllerUrl == null) {
      return;
    }

    // Perform the same check on child controller too
    try (ControllerClient srcControllerClient = new ControllerClient(srcClusterName, childControllerUrl);
        ControllerClient destControllerClient = new ControllerClient(destClusterName, childControllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        // Store should be deleted in source cluster. Store in destination cluster should not be migrating.
        StoreResponse storeResponse = srcControllerClient.getStore(storeName);
        Assert.assertNull(storeResponse.getStore());

        storeResponse = destControllerClient.getStore(storeName);
        Assert.assertNotNull(storeResponse.getStore());
        Assert.assertFalse(storeResponse.getStore().isMigrating());
        Assert.assertFalse(storeResponse.getStore().isMigrationDuplicateStore());
      });
    }
  }

  public static void abortMigration(
      String controllerUrl,
      String storeName,
      boolean force,
      String srcClusterName,
      String destClusterName) {
    AdminTool.abortMigration(
        controllerUrl,
        storeName,
        srcClusterName,
        destClusterName,
        force,
        ABORT_MIGRATION_PROMPTS_OVERRIDE);
  }

  public static void checkStatusAfterAbortMigration(
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient,
      String storeName,
      String srcClusterName) {
    // Migration flag should be false
    // Store should be deleted in dest cluster
    // Cluster discovery should point to src cluster
    StoreResponse storeResponse = srcControllerClient.getStore(storeName);
    Assert.assertNotNull(storeResponse.getStore());
    Assert.assertFalse(storeResponse.getStore().isMigrating());
    storeResponse = destControllerClient.getStore(storeName);
    Assert.assertNull(storeResponse.getStore());
    ControllerResponse discoveryResponse = destControllerClient.discoverCluster(storeName);
    Assert.assertEquals(discoveryResponse.getCluster(), srcClusterName);
  }
}
