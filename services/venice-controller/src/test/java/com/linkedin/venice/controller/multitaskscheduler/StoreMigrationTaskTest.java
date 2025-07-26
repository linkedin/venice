package com.linkedin.venice.controller.multitaskscheduler;

import static com.linkedin.venice.controller.multitaskscheduler.MigrationRecord.Step;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.controllerapi.ControllerClient;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreMigrationTaskTest {
  @Mock
  private MigrationRecord mockRecord;
  @Mock
  private StoreMigrationManager mockManager;
  @Mock
  private ControllerClient mocksrcControllerClient;
  @Mock
  ControllerClient mockDestControllerClient;

  private StoreMigrationTask task;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    task = new StoreMigrationTask(
        mockRecord,
        mockManager,
        mocksrcControllerClient,
        mockDestControllerClient,
        new HashMap<>(),
        new HashMap<>(),
        Arrays.asList("fabric1", "fabric2"));
  }

  @Test
  public void testRun_CheckDiskSpace() {
    when(mockRecord.getCurrentStep()).thenReturn(0);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.CHECK_DISK_SPACE);

    task.run();

    verify(mockRecord).setCurrentStep(Step.PRE_CHECK_AND_SUBMIT_MIGRATION_REQUEST);
    verify(mockRecord).resetAttempts();
    verify(mockManager).scheduleNextStep(same(task), eq(0));
  }

  @Test
  public void testRun_PreCheckAndSubmitMigrationRequest() {
    when(mockRecord.getCurrentStep()).thenReturn(1);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.PRE_CHECK_AND_SUBMIT_MIGRATION_REQUEST);

    task.run();

    verify(mockRecord).setCurrentStep(Step.VERIFY_MIGRATION_STATUS);
    verify(mockRecord).resetAttempts();
    verify(mockManager).scheduleNextStep(same(task), eq(0));
  }

  @Test
  public void testRun_VerifyMigrationStatus_Timeout() {
    when(mockRecord.getCurrentStep()).thenReturn(2);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.VERIFY_MIGRATION_STATUS);

    when(mockRecord.getStoreMigrationStartTime()).thenReturn(Instant.now().minus(Duration.ofDays(1)));
    when(mockRecord.getStoreName()).thenReturn("testStore");
    when(mockRecord.getAbortOnFailure()).thenReturn(true);

    // Simulate a status not verified and timeout occurred.
    task.setTimeout(0); // Set timeout to zero to force a timeout during the test
    task.run();

    verify(mockManager).cleanupMigrationRecord(mockRecord.getStoreName());
    verify(mockRecord).setIsAborted(true);
  }

  @Test
  public void testRun_VerifyMigrationStatus_NoTimeout() {
    when(mockRecord.getCurrentStep()).thenReturn(2);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.VERIFY_MIGRATION_STATUS);
    when(mockRecord.getStoreMigrationStartTime()).thenReturn(Instant.now());
    when(mockRecord.getStoreName()).thenReturn("testStore");

    task.run();

    verify(mockManager, never()).cleanupMigrationRecord(mockRecord.getStoreName());
    verify(mockManager).scheduleNextStep(same(task), eq(60));
  }

  @Test
  public void testRun_UpdateClusterDiscovery() {
    when(mockRecord.getCurrentStep()).thenReturn(3);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.UPDATE_CLUSTER_DISCOVERY);

    task.run();

    verify(mockRecord).setCurrentStep(Step.VERIFY_READ_REDIRECTION);
    verify(mockRecord).resetAttempts();
    verify(mockManager).scheduleNextStep(same(task), eq(0));
  }

  @Test
  public void testRun_VerifyReadRedirection() {
    when(mockRecord.getCurrentStep()).thenReturn(4);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.VERIFY_READ_REDIRECTION);

    task.run();

    verify(mockRecord).setCurrentStep(Step.END_MIGRATION);
    verify(mockRecord).resetAttempts();
    verify(mockManager).scheduleNextStep(same(task), eq(0));
  }

  @Test
  public void testRun_EndMigration() {
    when(mockRecord.getCurrentStep()).thenReturn(5);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.END_MIGRATION);
    when(mockRecord.getStoreName()).thenReturn("testStore");

    task.run();

    verify(mockRecord).setCurrentStep(Step.MIGRATION_SUCCEED);
    verify(mockManager).cleanupMigrationRecord(mockRecord.getStoreName());
  }

  @Test
  public void testRun_InvalidMigrationRecordStep() {
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.MIGRATION_SUCCEED); // Invalid step
    when(mockRecord.getAttempts()).thenReturn(0);
    when(mockManager.getMaxRetryAttempts()).thenReturn(3);

    task.run();

    verify(mockManager).cleanupMigrationRecord(mockRecord.getStoreName());
  }

  @Test
  public void testRun_ExceptionHandling_Retry() {
    when(mockRecord.getCurrentStep()).thenThrow(new RuntimeException("Test exception"));
    when(mockRecord.getAttempts()).thenReturn(0);
    when(mockManager.getMaxRetryAttempts()).thenReturn(3);

    task.run();

    verify(mockRecord).incrementAttempts();
    verify(mockManager).scheduleNextStep(any(StoreMigrationTask.class), eq(60));
  }

  @Test
  public void testRun_ExceptionHandling_Abort() {
    when(mockRecord.getCurrentStepEnum()).thenThrow(new RuntimeException("Test exception"));
    when(mockRecord.getAttempts()).thenReturn(3);
    when(mockRecord.getStoreName()).thenReturn("testStore");
    when(mockManager.getMaxRetryAttempts()).thenReturn(3);

    task.run();
    verify(mockRecord, never()).setIsAborted(true);
    verify(mockManager).cleanupMigrationRecord(mockRecord.getStoreName());

    when(mockRecord.getAbortOnFailure()).thenReturn(true);
    task.run();
    verify(mockRecord).setIsAborted(true);
  }

}
