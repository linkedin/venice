package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubTopicImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.InMemoryLogAppender;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ShutdownCheckpointDiagnosticsTest {
  static class Logs implements AutoCloseable {
    final InMemoryLogAppender appender = new InMemoryLogAppender.Builder().build();
    final Logger logger;

    Logs(Class<?> type) {
      logger = (Logger) LogManager.getLogger(type);
      appender.start();
      logger.addAppender(appender);
    }

    List<String> diagnostics() {
      return appender.getLogs()
          .stream()
          .filter(line -> line.startsWith("Shutdown checkpoint") || line.startsWith("Blob bootstrap original metadata"))
          .collect(java.util.stream.Collectors.toList());
    }

    @Override
    public void close() {
      logger.removeAppender(appender);
      appender.stop();
    }
  }

  private static Map<Integer, PartitionConsumptionState> inventory(int count) {
    Map<Integer, PartitionConsumptionState> inventory = new LinkedHashMap<>();
    for (int i = 0; i < count; i++) {
      // Deliberately non-numeric visitation order.
      int id = 10000 - i;
      PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
      when(pcs.getPartition()).thenReturn(id);
      when(pcs.getReplicaTopicPartition())
          .thenReturn(new PubSubTopicPartitionImpl(new PubSubTopicImpl("diagnostics_v1"), id));
      inventory.put(id, pcs);
    }
    return inventory;
  }

  private static class Fixture {
    final StoreIngestionTask task = mock(StoreIngestionTask.class);
    final VeniceServerConfig config = mock(VeniceServerConfig.class);
    final StoreBufferService buffer = mock(StoreBufferService.class);
    final Map<Integer, PartitionConsumptionState> inventory;

    Fixture(boolean enabled, int partitions) throws Exception {
      inventory = inventory(partitions);
      when(task.getServerConfig()).thenReturn(config);
      when(config.isIngestionCheckpointDiagnosticsEnabled()).thenReturn(enabled);
      when(config.isServerIngestionCheckpointDuringGracefulShutdownEnabled()).thenReturn(true);
      when(config.getShutdownSyncOffsetTimeoutMs()).thenReturn(100L);
      when(config.getParallelShutdownThreadPoolSize()).thenReturn(1);
      when(config.getShutdownPartitionStateTimeoutMs()).thenReturn(5000L);
      when(task.getPartitionConsumptionStateMap()).thenReturn(inventory);
      when(task.getStoreBufferService()).thenReturn(buffer);
      when(buffer.execSyncOffsetCommandAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
      when(task.measureHybridHeartbeatTimestamp(any(), anyBoolean())).thenReturn(123L);
      doCallRealMethod().when(task).shutdownPartitionConsumptionStates();
      doCallRealMethod().when(task).executeShutdownRunnable(any(), anyList(), any());
    }
  }

  @DataProvider
  public Object[][] modes() {
    return new Object[][] { { false, false }, { false, true }, { true, false }, { true, true } };
  }

  @Test(dataProvider = "modes")
  public void testNormalShutdownOnlyEmitsOneSummaryWhenEnabled(boolean enabled, boolean parallel) throws Exception {
    Fixture fixture = new Fixture(enabled, 3);
    when(fixture.config.isParallelResourceShutdownEnabled()).thenReturn(parallel);
    try (Logs logs = new Logs(StoreIngestionTask.class)) {
      fixture.task.shutdownPartitionConsumptionStates();
      List<String> diagnostics = logs.diagnostics();
      if (enabled) {
        assertEquals(diagnostics.size(), 1);
        assertTrue(diagnostics.get(0).contains("total=3, commandCompleted=3, skipped=0, unfinished=0"));
      } else {
        assertTrue(diagnostics.isEmpty());
        verify(fixture.task, never()).measureHybridHeartbeatTimestamp(any(), anyBoolean());
        fixture.inventory.values().forEach(pcs -> verify(pcs, never()).getPartition());
      }
    }
  }

  @Test
  public void testSequentialInterruptedWaitIncludesUnvisitedPartitions() throws Exception {
    Fixture fixture = new Fixture(true, 3);
    CompletableFuture<Void> interrupted = new CompletableFuture<Void>() {
      @Override
      public Void get(long timeout, TimeUnit unit) throws InterruptedException {
        throw new InterruptedException("test interruption");
      }
    };
    when(fixture.buffer.execSyncOffsetCommandAsync(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null), interrupted);
    boolean wasInterrupted = Thread.currentThread().isInterrupted();
    try (Logs logs = new Logs(StoreIngestionTask.class)) {
      assertThrows(VeniceException.class, () -> fixture.task.shutdownPartitionConsumptionStates());
      assertEquals(Thread.currentThread().isInterrupted(), wasInterrupted);
      assertTrue(interrupted.isCancelled(), "Existing wait cancellation must be preserved");
      List<String> diagnostics = logs.diagnostics();
      assertEquals(diagnostics.size(), 2);
      assertTrue(diagnostics.get(0).contains("commandCompleted=1, skipped=0, unfinished=2"));
      assertTrue(diagnostics.get(1).contains("partition=9999,stage=COMPLETION_UNKNOWN"));
      assertTrue(diagnostics.get(1).contains("partition=9998,stage=NOT_STARTED"));
      assertTrue(diagnostics.get(1).contains("observedPersistedHeartbeat/checkpoint=unavailable"));
      assertFalse(diagnostics.get(1).contains("partition=10000,"));
    }
  }

  @Test
  public void testCheckpointDisabledIsSkippedNotCompleted() throws Exception {
    Fixture fixture = new Fixture(true, 2);
    when(fixture.config.isServerIngestionCheckpointDuringGracefulShutdownEnabled()).thenReturn(false);
    try (Logs logs = new Logs(StoreIngestionTask.class)) {
      fixture.task.shutdownPartitionConsumptionStates();
      assertEquals(logs.diagnostics().size(), 1);
      assertTrue(logs.diagnostics().get(0).contains("commandCompleted=0, skipped=2, unfinished=0"));
      verify(fixture.buffer, never()).execSyncOffsetCommandAsync(any(), any());
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testExceptionalAndTimedOutCommandsAreNotCompleted(boolean parallel) throws Exception {
    Fixture fixture = new Fixture(true, 2);
    when(fixture.config.isParallelResourceShutdownEnabled()).thenReturn(parallel);
    CompletableFuture<Void> failed = new CompletableFuture<>();
    failed.completeExceptionally(new IllegalStateException("test failure"));
    CompletableFuture<Void> pending = new CompletableFuture<>();
    when(fixture.buffer.execSyncOffsetCommandAsync(any(), any())).thenReturn(failed, pending);
    when(fixture.config.getShutdownSyncOffsetTimeoutMs()).thenReturn(0L);
    try (Logs logs = new Logs(StoreIngestionTask.class)) {
      fixture.task.shutdownPartitionConsumptionStates();
      assertTrue(logs.diagnostics().get(0).contains("commandCompleted=0, skipped=0, unfinished=2"));
      assertTrue(pending.isCancelled());
    }
  }

  @Test
  public void testGlobalRtDivCommandIsObservedWithoutAddingDrainerCommands() throws Exception {
    Fixture fixture = new Fixture(true, 2);
    when(fixture.task.isGlobalRtDivEnabled()).thenReturn(true);
    when(fixture.task.flushGlobalRtDivCheckpoint(any())).thenReturn(CompletableFuture.completedFuture(null));
    try (Logs logs = new Logs(StoreIngestionTask.class)) {
      fixture.task.shutdownPartitionConsumptionStates();
      assertEquals(logs.diagnostics().size(), 1);
      assertTrue(logs.diagnostics().get(0).contains("commandCompleted=2, skipped=0, unfinished=0"));
      verify(fixture.buffer, never()).execSyncOffsetCommandAsync(any(), any());
    }
  }

  @Test
  public void testParallelOuterTimeoutReportsInFlightCommand() throws Exception {
    Fixture fixture = new Fixture(true, 1);
    when(fixture.config.isParallelResourceShutdownEnabled()).thenReturn(true);
    when(fixture.config.getShutdownPartitionStateTimeoutMs()).thenReturn(0L);
    CountDownLatch waiting = new CountDownLatch(1);
    CompletableFuture<Void> pending = new CompletableFuture<Void>() {
      @Override
      public Void get(long timeout, TimeUnit unit) throws InterruptedException {
        waiting.countDown();
        new CountDownLatch(1).await();
        return null;
      }
    };
    when(fixture.buffer.execSyncOffsetCommandAsync(any(), any())).thenReturn(pending);
    doAnswer(invocation -> {
      invocation.callRealMethod();
      assertTrue(waiting.await(5, TimeUnit.SECONDS));
      return null;
    }).when(fixture.task).executeShutdownRunnable(any(), anyList(), any());
    try (Logs logs = new Logs(StoreIngestionTask.class)) {
      fixture.task.shutdownPartitionConsumptionStates();
      assertTrue(logs.diagnostics().get(0).contains("commandCompleted=0, skipped=0, unfinished=1"));
      assertTrue(logs.diagnostics().get(1).contains("partition=10000,stage="));
    }
  }

  @Test
  public void testLatePersistenceDoesNotRelabelCancelledWaitAsFailureOrCompletion() {
    Map<Integer, PartitionConsumptionState> inventory = inventory(1);
    ShutdownCheckpointDiagnostics diagnostics = new ShutdownCheckpointDiagnostics(inventory);
    ShutdownCheckpointDiagnostics.Partition partition = diagnostics.partition(10000);
    CompletableFuture<Void> command = new CompletableFuture<>();
    partition.observe(command);
    command.cancel(true);
    List<ShutdownCheckpointDiagnostics.Observation> before = diagnostics.snapshot(inventory, pcs -> 30);
    diagnostics.persisted(10000, 20, 25);
    List<ShutdownCheckpointDiagnostics.Observation> after = diagnostics.snapshot(inventory, pcs -> 40);
    assertEquals(before.get(0).persisted, "unavailable");
    assertEquals(after.get(0).persisted, "20/25");
    assertEquals(after.get(0).stage, ShutdownCheckpointDiagnostics.Stage.COMPLETION_UNKNOWN);
    assertEquals(after.get(0).liveHeartbeat.longValue(), 40L);
  }

  @Test
  public void testFailedHeartbeatMeasurementStillReportsAllPartitions() {
    Map<Integer, PartitionConsumptionState> inventory = inventory(2);
    ShutdownCheckpointDiagnostics diagnostics = new ShutdownCheckpointDiagnostics(inventory);
    List<ShutdownCheckpointDiagnostics.Observation> observations = diagnostics.snapshot(inventory, pcs -> {
      throw new IllegalStateException("measurement unavailable");
    });
    try (Logs logs = new Logs(ShutdownCheckpointDiagnostics.class)) {
      diagnostics.log(logs.logger, "test_v1", observations);
      assertTrue(logs.diagnostics().get(0).contains("unfinished=2"));
      assertTrue(logs.diagnostics().get(1).contains("liveHeartbeat=unavailable"));
    }
  }

  @Test
  public void testBatchesIncludeEveryUnfinishedPartition() {
    Map<Integer, PartitionConsumptionState> inventory = inventory(53);
    ShutdownCheckpointDiagnostics diagnostics = new ShutdownCheckpointDiagnostics(inventory);
    try (Logs logs = new Logs(ShutdownCheckpointDiagnostics.class)) {
      diagnostics.log(logs.logger, "test_v1", diagnostics.snapshot(inventory, pcs -> Long.MAX_VALUE));
      List<String> output = logs.diagnostics();
      assertEquals(output.size(), 4);
      assertTrue(output.get(0).contains("unfinished=53"));
      for (Integer partition: inventory.keySet()) {
        assertEquals(output.stream().filter(line -> line.contains("partition=" + partition + ",")).count(), 1L);
      }
      for (int i = 1; i < output.size(); i++) {
        assertTrue(output.get(i).contains("batch=" + i + ", batches=3"));
        assertTrue(output.get(i).length() < 5000, "Fixed-size numeric entries bound each batch");
      }
    }
  }
}
