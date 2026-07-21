package com.linkedin.venice.controller;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import org.testng.annotations.Test;


public class StoreMigrationHelperTest {
  private static final String SRC_CLUSTER = "src-cluster";
  private static final String DEST_CLUSTER = "dest-cluster";
  private static final String STORE_NAME = "test-store";

  @Test
  public void testAllowsMigrationBetweenNonEncryptionClusters() {
    StoreMigrationHelper.validateEncryptionClusterMigration(false, false, SRC_CLUSTER, DEST_CLUSTER, STORE_NAME);
  }

  @Test
  public void testBlocksMigrationFromEncryptionCluster() {
    assertEncryptionClusterMigrationBlocked(true, false);
  }

  @Test
  public void testBlocksMigrationToEncryptionCluster() {
    assertEncryptionClusterMigrationBlocked(false, true);
  }

  @Test
  public void testBlocksMigrationBetweenEncryptionClusters() {
    assertEncryptionClusterMigrationBlocked(true, true);
  }

  private void assertEncryptionClusterMigrationBlocked(boolean srcEncryptionCluster, boolean destEncryptionCluster) {
    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> StoreMigrationHelper.validateEncryptionClusterMigration(
            srcEncryptionCluster,
            destEncryptionCluster,
            SRC_CLUSTER,
            DEST_CLUSTER,
            STORE_NAME));
    assertTrue(exception.getMessage().contains("migration from or to an encryption cluster is not allowed"));
  }
}
