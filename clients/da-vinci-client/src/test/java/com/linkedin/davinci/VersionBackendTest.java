package com.linkedin.davinci;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VersionBackendTest {
  @Test
  public void testMaybeReportIncrementalPushStatus() {
    VersionBackend versionBackend = mock(VersionBackend.class);
    Map<Integer, List<String>> partitionToPendingReportIncrementalPushList = new VeniceConcurrentHashMap<>();
    Map<Integer, Boolean> partitionToBatchReportEOIPEnabled = new VeniceConcurrentHashMap<>();
    doReturn(partitionToBatchReportEOIPEnabled).when(versionBackend).getPartitionToBatchReportEOIPEnabled();
    doReturn(partitionToPendingReportIncrementalPushList).when(versionBackend)
        .getPartitionToPendingReportIncrementalPushList();
    doCallRealMethod().when(versionBackend).maybeReportIncrementalPushStatus(anyInt(), anyString(), any(), any());
    Consumer<String> mockConsumer = mock(Consumer.class);

    versionBackend
        .maybeReportIncrementalPushStatus(0, "a", ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED, mockConsumer);
    verify(mockConsumer, times(1)).accept("a");
    versionBackend
        .maybeReportIncrementalPushStatus(0, "a", ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED, mockConsumer);
    verify(mockConsumer, times(2)).accept("a");

    partitionToBatchReportEOIPEnabled.put(0, true);
    versionBackend
        .maybeReportIncrementalPushStatus(0, "a", ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED, mockConsumer);
    verify(mockConsumer, times(2)).accept("a");
    versionBackend
        .maybeReportIncrementalPushStatus(0, "a", ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED, mockConsumer);
    verify(mockConsumer, times(2)).accept("a");
    Assert.assertTrue(partitionToPendingReportIncrementalPushList.containsKey(0));
    Assert.assertEquals(partitionToPendingReportIncrementalPushList.get(0).size(), 1);
  }

  @Test
  public void testMaybeReportBatchEOIPStatus() {
    VersionBackend versionBackend = mock(VersionBackend.class);
    List<String> incPushList = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      incPushList.add("inc_" + i);
    }
    Map<Integer, List<String>> partitionToPendingReportIncrementalPushList = Collections.singletonMap(0, incPushList);
    Map<Integer, Boolean> partitionToBatchReportEOIPEnabled = new VeniceConcurrentHashMap<>();
    partitionToBatchReportEOIPEnabled.put(0, true);
    doReturn(partitionToBatchReportEOIPEnabled).when(versionBackend).getPartitionToBatchReportEOIPEnabled();
    doReturn(partitionToPendingReportIncrementalPushList).when(versionBackend)
        .getPartitionToPendingReportIncrementalPushList();
    doCallRealMethod().when(versionBackend).maybeReportBatchEOIPStatus(anyInt(), any());
    Consumer<String> mockConsumer = mock(Consumer.class);
    versionBackend.maybeReportBatchEOIPStatus(0, mockConsumer);
    verify(mockConsumer, times(50)).accept(anyString());
    verify(mockConsumer, times(0)).accept("inc_0");
    verify(mockConsumer, times(0)).accept("inc_49");
    verify(mockConsumer, times(1)).accept("inc_50");
    verify(mockConsumer, times(1)).accept("inc_99");
    Assert.assertFalse(partitionToBatchReportEOIPEnabled.get(0));
  }
}
