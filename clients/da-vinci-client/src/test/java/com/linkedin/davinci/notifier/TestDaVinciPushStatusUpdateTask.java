package com.linkedin.davinci.notifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.testng.annotations.Test;


public class TestDaVinciPushStatusUpdateTask {
  @Test
  public void testUpdateTask() {
    String storeName = Utils.getUniqueString("test-store");
    int versionNumber = 1;
    Version version = mock(Version.class);
    doReturn(storeName).when(version).getStoreName();
    doReturn(versionNumber).when(version).getNumber();
    PushStatusStoreWriter pushStatusStoreWriter = mock(PushStatusStoreWriter.class);
    // Always return true for this supplier since there is no VersionBackend
    Supplier<Boolean> areAllPartitionFuturesCompletedSuccessfully = () -> true;
    DaVinciPushStatusUpdateTask task = new DaVinciPushStatusUpdateTask(
        version,
        100,
        pushStatusStoreWriter,
        areAllPartitionFuturesCompletedSuccessfully);
    task.updatePartitionStatus(1, ExecutionStatus.COMPLETED, Optional.empty());
    task.updatePartitionStatus(2, ExecutionStatus.COMPLETED, Optional.empty());
    task.updatePartitionStatus(3, ExecutionStatus.COMPLETED, Optional.empty());
    // Verify that the status is consistent across all partitions
    assertTrue(task.areAllPartitionsOnSameTerminalStatus(ExecutionStatus.COMPLETED, Optional.empty()));

    // Set partition 4 to ERROR
    task.updatePartitionStatus(4, ExecutionStatus.ERROR, Optional.empty());
    assertFalse(task.areAllPartitionsOnSameTerminalStatus(ExecutionStatus.COMPLETED, Optional.empty()));
    assertTrue(task.isAnyPartitionOnErrorStatus(Optional.empty()));

    // Set the status of partition 4 back to STARTED
    task.updatePartitionStatus(4, ExecutionStatus.STARTED, Optional.empty());
    assertFalse(task.areAllPartitionsOnSameTerminalStatus(ExecutionStatus.COMPLETED, Optional.empty()));

    // Start the task
    task.start();
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(pushStatusStoreWriter, times(1)).writeVersionLevelPushStatus(
          eq(storeName),
          eq(versionNumber),
          eq(ExecutionStatus.STARTED),
          any(),
          Optional.empty());
      // However, COMPLETED status should never be sent
      verify(pushStatusStoreWriter, never()).writeVersionLevelPushStatus(
          eq(storeName),
          eq(versionNumber),
          eq(ExecutionStatus.COMPLETED),
          any(),
          Optional.empty());
    });
    // Update the push status of partition 4 to COMPLETED too
    task.updatePartitionStatus(4, ExecutionStatus.COMPLETED, Optional.empty());
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(pushStatusStoreWriter, times(1)).writeVersionLevelPushStatus(
          eq(storeName),
          eq(versionNumber),
          eq(ExecutionStatus.COMPLETED),
          any(),
          Optional.empty());
    });
    task.shutdown();
  }
}
