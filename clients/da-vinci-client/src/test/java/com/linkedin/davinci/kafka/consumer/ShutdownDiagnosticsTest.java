package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IngestionShutdownDiagnostics;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.mockito.InOrder;
import org.testng.annotations.Test;


public class ShutdownDiagnosticsTest {
  @Test
  public void testInterruptedCheckpointStillCountsAsFinishedAttempt() throws InterruptedException {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    VeniceServerConfig config = mock(VeniceServerConfig.class);
    when(task.getServerConfig()).thenReturn(config);
    when(config.isServerIngestionCheckpointDuringGracefulShutdownEnabled()).thenReturn(true);
    doThrow(new InterruptedException("test drain interruption")).when(task)
        .waitForAllMessageToBeProcessedFromTopicPartition(any(), any());
    doCallRealMethod().when(task).executeShutdownRunnable(any(), anyList(), any());
    task.shutdownDiagnostics = new IngestionShutdownDiagnostics();
    task.shutdownDiagnostics.setPartitions(1);

    expectThrows(
        VeniceException.class,
        () -> task.executeShutdownRunnable(mock(PartitionConsumptionState.class), new ArrayList<>(), null));

    assertTrue(task.shutdownDiagnostics.snapshot().contains("checkpointsFinished=1, checkpointsRemaining=0"));
  }

  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testOnlyEnabledServerCloseCreatesTimingState(boolean enabled, boolean daVinci) {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    VeniceServerConfig config = mock(VeniceServerConfig.class);
    when(task.getServerConfig()).thenReturn(config);
    when(config.isShutdownDiagnosticsEnabled()).thenReturn(enabled);
    when(task.isDaVinciClient()).thenReturn(daVinci);
    doCallRealMethod().when(task).closeForServerShutdown();

    // Routine closes never initialize diagnostics, regardless of the config.
    task.close();
    assertNull(task.shutdownDiagnostics);
    task.closeForServerShutdown();
    if (enabled && !daVinci) {
      assertNotNull(task.shutdownDiagnostics);
      assertTrue(task.shutdownDiagnostics.snapshot().startsWith("phase=CLOSE_REQUEST_TO_LOOP_EXIT"));
    } else {
      assertNull(task.shutdownDiagnostics);
    }
    verify(task, times(2)).close();
  }

  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testSnapshotRunsBeforeForcedInterruptEvenOnFailure(boolean interrupted, boolean snapshotFails)
      throws InterruptedException {
    ExecutorService executor = mock(ExecutorService.class);
    Runnable snapshot = mock(Runnable.class);
    if (interrupted) {
      when(executor.awaitTermination(60, TimeUnit.SECONDS)).thenThrow(new InterruptedException("test"));
    } else {
      when(executor.awaitTermination(60, TimeUnit.SECONDS)).thenReturn(false);
      if (!snapshotFails) {
        when(executor.awaitTermination(30, TimeUnit.SECONDS)).thenReturn(true);
      }
    }
    if (snapshotFails) {
      doThrow(new IllegalStateException("test snapshot failure")).when(snapshot).run();
    }
    try {
      if (snapshotFails) {
        expectThrows(
            IllegalStateException.class,
            () -> KafkaStoreIngestionService.shutdownExecutorService(executor, "test", false, snapshot));
      } else {
        KafkaStoreIngestionService.shutdownExecutorService(executor, "test", false, snapshot);
      }
      InOrder order = inOrder(executor, snapshot);
      order.verify(executor).shutdown();
      order.verify(executor).awaitTermination(60, TimeUnit.SECONDS);
      order.verify(snapshot).run();
      order.verify(executor).shutdownNow();
      assertEquals(Thread.currentThread().isInterrupted(), interrupted);
      if (!interrupted && !snapshotFails) {
        order.verify(executor).awaitTermination(30, TimeUnit.SECONDS);
      }
    } finally {
      Thread.interrupted();
    }
  }
}
