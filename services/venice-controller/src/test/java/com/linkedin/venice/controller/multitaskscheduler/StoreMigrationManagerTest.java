package com.linkedin.venice.controller.multitaskscheduler;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.controller.multitaskscheduler.MigrationRecord.Step;
import com.linkedin.venice.controllerapi.ControllerClient;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreMigrationManagerTest {
  @Mock
  private ScheduledExecutorService mockExecutorService;
  @Mock
  private ControllerClient srcControllerClient;
  @Mock
  private ControllerClient destControllerClient;

  private StoreMigrationManager storeMigrationManager;

  private static final int THREAD_POOL_SIZE = 5;
  private static final int MAX_RETRY_ATTEMPTS = 3;
  private static final int DEFAULT_DELAY_IN_SECONDS = 1;
  private static final String SOURCE_CLUSTER = "sourceCluster";
  private static final String DESTINATION_CLUSTER = "destinationCluster";

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

    verify(mockExecutorService)
        .schedule(any(StoreMigrationTask.class), eq((long) DEFAULT_DELAY_IN_SECONDS), eq(TimeUnit.SECONDS));
  }

  /** Positive path – pauseAfter is larger than the current step. */
  @Test
  public void testPauseMigrationAfterSuccess() {
    String storeName = "TestPauseStore";
    storeMigrationManager.scheduleMigration(storeName, SOURCE_CLUSTER, DESTINATION_CLUSTER, 0);

    storeMigrationManager.pauseMigrationAfter(storeName, 1);

    MigrationRecord rec = storeMigrationManager.getMigrationRecord(storeName);
    // the record must now reflect the requested pauseAfter step
    assertNotNull(rec);
    assertEquals(rec.getPauseAfter().getStepNumber(), 1);
    assertFalse(rec.isPaused(), "Migration should not be paused yet, as current step is less than pauseAfter step");
  }

  /** Guard-rail: trying to pause AFTER a step that is smaller than the current step must throw an IllegalArgumentException. */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPauseMigrationAfterCurrentStepGreaterThanPauseAfter() {
    String storeName = "TestBadPauseStore";
    // start the migration already at step 2
    storeMigrationManager.scheduleMigration(storeName, "src", "dest", 2);

    // pause after step 1 -> should fail because 2 > 1
    storeMigrationManager.pauseMigrationAfter(storeName, 1);
  }

  /** Guard-rail: calling pauseMigrationAfter for a store that does not exist must throw an IllegalArgumentException. */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPauseMigrationAfterNoRecord() {
    storeMigrationManager.pauseMigrationAfter("StoreDoNotExist", 0);
  }

  /** Happy-path resume: a paused migration should schedule the next step. */
  @Test
  public void testResumeMigrationSuccess() {
    String storeName = "TestResumeStore";
    storeMigrationManager.scheduleMigration(storeName, SOURCE_CLUSTER, DESTINATION_CLUSTER, 0);

    // Pause after step 0 -> record becomes "paused" (currentStep == pauseAfter)
    storeMigrationManager.pauseMigrationAfter(storeName, 0);
    MigrationRecord rec = storeMigrationManager.getMigrationRecord(storeName);
    assertEquals(rec.getPauseAfter().getStepNumber(), 0);

    // The migration record should be paused now
    rec.setPaused(true);
    // Add Mock StoreMigration task for the store since it is paused
    storeMigrationManager.migrationTasks.put(storeName, mock(StoreMigrationTask.class));

    // Clear the interaction that was produced by the original scheduleMigration
    reset(mockExecutorService);
    // Resume the migration at the next step (step 1)
    storeMigrationManager.resumeMigration(storeName, 1);

    // A brand-new scheduling must have happened
    verify(mockExecutorService)
        .schedule(any(StoreMigrationTask.class), eq((long) DEFAULT_DELAY_IN_SECONDS), eq(TimeUnit.SECONDS));
    assertEquals(rec.getCurrentStep(), 1);
  }

  /** Guard-rail: resuming when the migration is NOT paused must throw. */
  @Test(expectedExceptions = IllegalStateException.class)
  public void testResumeMigrationWhenNotPaused() {
    String storeName = "NotPausedStore";
    storeMigrationManager.scheduleMigration(storeName, SOURCE_CLUSTER, DESTINATION_CLUSTER, 0);

    // The migration was never paused – this must fail
    storeMigrationManager.resumeMigration(storeName, 1);
  }

  /** Guard-rail: resuming when the migration record doesn't exist, must throw an IllegalArgumentException */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testResumeMigrationNoRecord() {
    storeMigrationManager.resumeMigration("unknownStore");
  }

  @Test
  public void testScheduleNextStep() {
    MigrationRecord record = new MigrationRecord.Builder("TestStore", "Cluster1", "Cluster2").currentStep(1).build();
    int delayInSeconds = 10;

    storeMigrationManager.scheduleNextStep(
        record,
        delayInSeconds,
        srcControllerClient,
        destControllerClient,
        Collections.emptyMap(),
        Collections.emptyMap());

    verify(mockExecutorService)
        .schedule(any(StoreMigrationTask.class), eq((long) delayInSeconds), eq(TimeUnit.SECONDS));
    verifyNoMoreInteractions(mockExecutorService);
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
