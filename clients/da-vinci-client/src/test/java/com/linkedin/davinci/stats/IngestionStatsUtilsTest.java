package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.StatsErrorCode.INACTIVE_STORE_INGESTION_TASK;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.kafka.consumer.StorageUtilizationManager;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import java.util.function.Predicate;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link IngestionStatsUtils}.
 */
public class IngestionStatsUtilsTest {
  @Test
  public void testHasActiveIngestionTask() {
    // Null task
    assertFalse(IngestionStatsUtils.hasActiveIngestionTask(null));

    // Task not running
    StoreIngestionTask notRunningTask = mock(StoreIngestionTask.class);
    when(notRunningTask.isRunning()).thenReturn(false);
    assertFalse(IngestionStatsUtils.hasActiveIngestionTask(notRunningTask));

    // Task running
    StoreIngestionTask runningTask = mock(StoreIngestionTask.class);
    when(runningTask.isRunning()).thenReturn(true);
    assertTrue(IngestionStatsUtils.hasActiveIngestionTask(runningTask));
  }

  @Test
  public void testGetIngestionTaskErroredGaugeWithNullTask() {
    assertEquals(IngestionStatsUtils.getIngestionTaskErroredGauge(null), 0);
  }

  @Test
  public void testGetIngestionTaskErroredGaugeWithInactiveTask() {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    when(task.isRunning()).thenReturn(false);
    assertEquals(IngestionStatsUtils.getIngestionTaskErroredGauge(task), 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetIngestionTaskErroredGaugeNoPartitionsCompleted() {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    when(task.isRunning()).thenReturn(true);
    when(task.getFailedIngestionPartitionCount()).thenReturn(5);
    when(task.hasAnyPartitionConsumptionState(any(Predicate.class))).thenReturn(false);

    // Should return 0 if no partitions completed (even if there are failed partitions)
    assertEquals(IngestionStatsUtils.getIngestionTaskErroredGauge(task), 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetIngestionTaskErroredGaugeWithCompletedPartitions() {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    when(task.isRunning()).thenReturn(true);
    when(task.getFailedIngestionPartitionCount()).thenReturn(3);
    when(task.hasAnyPartitionConsumptionState(any(Predicate.class))).thenReturn(true);

    // Should return failed count when any partition has completed
    assertEquals(IngestionStatsUtils.getIngestionTaskErroredGauge(task), 3);
  }

  @Test
  public void testGetWriteComputeErrorCodeWithNullTask() {
    assertEquals(IngestionStatsUtils.getWriteComputeErrorCode(null), INACTIVE_STORE_INGESTION_TASK.code);
  }

  @Test
  public void testGetWriteComputeErrorCodeWithInactiveTask() {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    when(task.isRunning()).thenReturn(false);
    assertEquals(IngestionStatsUtils.getWriteComputeErrorCode(task), INACTIVE_STORE_INGESTION_TASK.code);
  }

  @Test
  public void testGetWriteComputeErrorCodeWithActiveTask() {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    when(task.isRunning()).thenReturn(true);
    when(task.getWriteComputeErrorCode()).thenReturn(42);
    assertEquals(IngestionStatsUtils.getWriteComputeErrorCode(task), 42);
  }

  @Test
  public void testGetStorageQuotaUsedWithNullTask() {
    assertEquals(IngestionStatsUtils.getStorageQuotaUsed(null), 0.0);
  }

  @Test
  public void testGetStorageQuotaUsedWithInactiveTask() {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    when(task.isRunning()).thenReturn(false);
    assertEquals(IngestionStatsUtils.getStorageQuotaUsed(task), 0.0);
  }

  @Test
  public void testGetStorageQuotaUsedWithNullStorageManager() {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    when(task.isRunning()).thenReturn(true);
    when(task.getStorageUtilizationManager()).thenReturn(null);
    assertEquals(IngestionStatsUtils.getStorageQuotaUsed(task), 0.0);
  }

  @Test
  public void testGetStorageQuotaUsedWithActiveTask() {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    StorageUtilizationManager storageManager = mock(StorageUtilizationManager.class);

    when(task.isRunning()).thenReturn(true);
    when(task.getStorageUtilizationManager()).thenReturn(storageManager);
    when(storageManager.getDiskQuotaUsage()).thenReturn(0.75);

    assertEquals(IngestionStatsUtils.getStorageQuotaUsed(task), 0.75);
  }
}
