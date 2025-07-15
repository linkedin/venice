package com.linkedin.venice.controller.multitaskscheduler;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.controller.multitaskscheduler.MigrationRecord.Step;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreMigrationManagerTest {
  @Mock
  private ScheduledExecutorService mockExecutorService;

  private StoreMigrationManager storeMigrationManager;

  private static final int THREAD_POOL_SIZE = 5;
  private static final int MAX_RETRY_ATTEMPTS = 3;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    storeMigrationManager = new StoreMigrationManager(THREAD_POOL_SIZE, MAX_RETRY_ATTEMPTS, 1, emptyList()) {
      @Override
      protected ScheduledExecutorService createExecutorService(int threadPoolSize) {
        return mockExecutorService;
      }
    };
  }

  @Test
  public void testGetMigrationRecord() {
    String storeName = "testStore";
    String sourceCluster = "sourceCluster";
    String destinationCluster = "destinationCluster";
    int currStep = 0;
    storeMigrationManager.scheduleMigration(storeName, sourceCluster, destinationCluster, currStep);
    MigrationRecord retrievedRecord = storeMigrationManager.getMigrationRecord(storeName);
    assertEquals(
        retrievedRecord.getStoreName(),
        storeName,
        "Migration record should be present, and store name should match");
    assertEquals(retrievedRecord.getSourceCluster(), sourceCluster);
    assertEquals(retrievedRecord.getDestinationCluster(), destinationCluster);
    assertEquals(retrievedRecord.getCurrentStep(), currStep);
    assertEquals(retrievedRecord.getCurrentStepEnum(), Step.fromStepNumber(currStep));
  }

  @Test
  public void testCleanupMigrationRecord() {
    String storeName = "TestStore";
    storeMigrationManager.scheduleMigration(storeName, "Cluster1", "Cluster2", 0);
    storeMigrationManager.cleanupMigrationRecord(storeName);

    MigrationRecord result = storeMigrationManager.getMigrationRecord(storeName);
    assertNull(result);
  }

  @Test
  public void testScheduleMigration() {
    String storeName = "TestStore";
    String sourceCluster = "Cluster1";
    String destinationCluster = "Cluster2";
    int currentStep = 0;

    storeMigrationManager.scheduleMigration(storeName, sourceCluster, destinationCluster, currentStep);

    verify(mockExecutorService).schedule(any(Runnable.class), eq(0L), eq(TimeUnit.SECONDS));
  }

  @Test
  public void testScheduleNextStep() {
    MigrationRecord record = new MigrationRecord.Builder("TestStore", "Cluster1", "Cluster2").currentStep(1).build();
    int delayInSeconds = 10;

    storeMigrationManager.scheduleNextStep(record, delayInSeconds);

    verify(mockExecutorService).schedule(any(Runnable.class), eq((long) delayInSeconds), eq(TimeUnit.SECONDS));
  }

  @Test
  public void testGetMaxRetryAttempts() {
    assertEquals(storeMigrationManager.getMaxRetryAttempts(), MAX_RETRY_ATTEMPTS, "Max retry attempts should match");
  }

  @Test
  public void testShutdown() throws InterruptedException {
    doReturn(true).when(mockExecutorService).awaitTermination(anyLong(), any(TimeUnit.class));

    storeMigrationManager.shutdown();

    verify(mockExecutorService).shutdown();
    verify(mockExecutorService, never()).shutdownNow();
  }

  @Test
  public void testShutdownForceful() throws InterruptedException {
    doReturn(false).when(mockExecutorService).awaitTermination(anyLong(), any(TimeUnit.class));

    storeMigrationManager.shutdown();

    verify(mockExecutorService).shutdown();
    verify(mockExecutorService).shutdownNow();
  }

}
