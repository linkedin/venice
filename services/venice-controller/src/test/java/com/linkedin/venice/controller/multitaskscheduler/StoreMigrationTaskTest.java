package com.linkedin.venice.controller.multitaskscheduler;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreMigrationTaskTest {
  private MigrationRecord mockRecord;
  private StoreMigrationManager mockManager;
  private StoreMigrationTask task;

  @BeforeMethod
  public void setUp() {
    mockRecord = mock(MigrationRecord.class);
    mockManager = mock(StoreMigrationManager.class);
    task = new StoreMigrationTask(mockRecord, mockManager);
  }

  @Test
  public void testRun_CheckDiskSpace() {
    when(mockRecord.getCurrentStep()).thenReturn(0);

    task.run();

    verify(mockRecord).setCurrentStep(1);
    verify(mockManager).scheduleNextStep(same(task), eq(0));
  }

  @Test
  public void testRun_PreCheckAndSubmitMigrationRequest() {
    when(mockRecord.getCurrentStep()).thenReturn(1);

    task.run();

    verify(mockRecord).setCurrentStep(2);
    verify(mockManager).scheduleNextStep(same(task), eq(0));
  }

  @Test
  public void testRun_VerifyMigrationStatus_Timeout() {
    when(mockRecord.getCurrentStep()).thenReturn(2);
    when(mockRecord.getStoreMigrationStartTime()).thenReturn(Instant.now().minus(Duration.ofDays(1)));
    when(mockRecord.getStoreName()).thenReturn("testStore");

    // Simulate a status not verified and timeout occurred.
    task.setTimeout(0); // Set timeout to zero to force a timeout during the test
    task.run();

    verify(mockManager).cleanupMigrationRecord(mockRecord.getStoreName());
    verify(mockRecord).setIsAborted(true);
  }

  @Test
  public void testRun_VerifyMigrationStatus_NoTimeout() {
    when(mockRecord.getCurrentStep()).thenReturn(2);
    when(mockRecord.getStoreMigrationStartTime()).thenReturn(Instant.now());
    when(mockRecord.getStoreName()).thenReturn("testStore");

    task.run();

    verify(mockManager, never()).cleanupMigrationRecord(mockRecord.getStoreName());
  }

  @Test
  public void testRun_UpdateClusterDiscovery() {
    when(mockRecord.getCurrentStep()).thenReturn(3);

    task.run();

    verify(mockRecord).setCurrentStep(4);
    verify(mockManager).scheduleNextStep(same(task), eq(0));
  }

  @Test
  public void testRun_VerifyReadRedirection() {
    when(mockRecord.getCurrentStep()).thenReturn(4);

    task.run();

    verify(mockRecord).setCurrentStep(5);
    verify(mockManager).scheduleNextStep(same(task), eq(0));
  }

  @Test
  public void testRun_EndMigration() {
    when(mockRecord.getCurrentStep()).thenReturn(5);
    when(mockRecord.getStoreName()).thenReturn("testStore");

    task.run();

    verify(mockRecord).setCurrentStep(6);
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
    when(mockRecord.getCurrentStep()).thenThrow(new RuntimeException("Test exception"));
    when(mockRecord.getAttempts()).thenReturn(3);
    when(mockManager.getMaxRetryAttempts()).thenReturn(3);

    task.run();

    verify(mockRecord).setIsAborted(true);
    verify(mockManager).cleanupMigrationRecord(mockRecord.getStoreName());
  }

}
