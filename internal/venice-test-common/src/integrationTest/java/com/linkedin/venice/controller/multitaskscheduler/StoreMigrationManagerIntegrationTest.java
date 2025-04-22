package com.linkedin.venice.controller.multitaskscheduler;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.utils.Time;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreMigrationManagerIntegrationTest {
  private StoreMigrationManager storeMigrationManager;
  private static final long TIMEOUT = 10 * Time.MS_PER_SECOND;

  @BeforeMethod
  public void setUp() {
    storeMigrationManager = StoreMigrationManager.createStoreMigrationManager(2, 3);
  }

  @AfterMethod
  public void tearDown() {
    storeMigrationManager.shutdown();
  }

  @Test(timeOut = TIMEOUT)
  public void testScheduleMigrationWithRealExecutor() throws InterruptedException {
    String storeName = "TestStore";
    String sourceCluster = "SourceCluster";
    String destinationCluster = "DestinationCluster";
    int initialStep = 0;

    // Schedule the migration
    storeMigrationManager.scheduleMigration(storeName, sourceCluster, destinationCluster, initialStep, 1);
    MigrationRecord migrationRecord = storeMigrationManager.getMigrationRecord(storeName);
    migrationRecord.setStoreMigrationStartTime(Instant.now().minus(25, ChronoUnit.HOURS));
    // Allow time for the scheduled tasks to execute until getIsAborted is set to true
    Awaitility.await().atMost(3, TimeUnit.SECONDS).until(migrationRecord::getIsAborted);
    // Verify that the migration record has been removed, indicating task is aborted
    assertEquals(migrationRecord.getCurrentStep(), 2);
    assertNull(
        storeMigrationManager.getMigrationRecord(storeName),
        "Migration record should be cleaned up from migrationRecords after abort migration.");

    // Schedule the migration start from step 3
    int stepUpdateClusterDiscovery = 3;
    ScheduledFuture<?> future2 = storeMigrationManager
        .scheduleMigration(storeName, sourceCluster, destinationCluster, stepUpdateClusterDiscovery);
    MigrationRecord recordFromUpdateClusterDiscovery = storeMigrationManager.getMigrationRecord(storeName);
    try {
      future2.get();
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException executionException) {
      // Handle the exception
      System.err.println("Error occurred while waiting for the future: " + executionException.getCause());
    }
    // Allow time for the sequentially scheduled tasks to execute
    Awaitility.await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
      assertEquals(
          recordFromUpdateClusterDiscovery.getCurrentStep(),
          6,
          "Migration record should be updated to 6 as marked as completed.");
    });
    // Verify that the migration record is not aborted
    assertFalse(recordFromUpdateClusterDiscovery.getIsAborted());
    assertNull(
        storeMigrationManager.getMigrationRecord(storeName),
        "Migration record should be cleaned up from migrationRecords after execution.");
  }
}
