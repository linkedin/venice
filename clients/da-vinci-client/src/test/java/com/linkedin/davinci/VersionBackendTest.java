package com.linkedin.davinci;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
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
    List<String> pendingReportVersionList = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      pendingReportVersionList.add("version_" + i);
    }
    partitionToPendingReportIncrementalPushList.put(0, pendingReportVersionList);

    Map<Integer, Boolean> partitionToBatchReportEOIPEnabled = new VeniceConcurrentHashMap<>();
    doReturn(partitionToBatchReportEOIPEnabled).when(versionBackend).getPartitionToBatchReportEOIPEnabled();
    doReturn(partitionToPendingReportIncrementalPushList).when(versionBackend)
        .getPartitionToPendingReportIncrementalPushList();
    Version version = new VersionImpl("test_store", 1, "dummy");
    doReturn(version).when(versionBackend).getVersion();
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
    Assert.assertEquals(partitionToPendingReportIncrementalPushList.get(0).size(), 50);
    Assert.assertEquals(partitionToPendingReportIncrementalPushList.get(0).get(0), "version_0");
    Assert.assertEquals(partitionToPendingReportIncrementalPushList.get(0).get(49), "version_49");

    versionBackend
        .maybeReportIncrementalPushStatus(0, "a", ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED, mockConsumer);
    verify(mockConsumer, times(2)).accept("a");
    Assert.assertTrue(partitionToPendingReportIncrementalPushList.containsKey(0));
    Assert.assertEquals(partitionToPendingReportIncrementalPushList.get(0).size(), 50);
    Assert.assertEquals(partitionToPendingReportIncrementalPushList.get(0).get(0), "version_1");
    Assert.assertEquals(partitionToPendingReportIncrementalPushList.get(0).get(49), "a");
  }

  @Test
  public void testMaybeReportBatchEOIPStatus() {
    VersionBackend versionBackend = mock(VersionBackend.class);
    List<String> incPushList = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      incPushList.add("inc_" + i);
    }
    Map<Integer, List<String>> partitionToPendingReportIncrementalPushList = Collections.singletonMap(0, incPushList);
    Map<Integer, Boolean> partitionToBatchReportEOIPEnabled = new VeniceConcurrentHashMap<>();
    partitionToBatchReportEOIPEnabled.put(0, true);
    doReturn(partitionToBatchReportEOIPEnabled).when(versionBackend).getPartitionToBatchReportEOIPEnabled();
    doReturn(partitionToPendingReportIncrementalPushList).when(versionBackend)
        .getPartitionToPendingReportIncrementalPushList();
    doCallRealMethod().when(versionBackend).maybeReportBatchEOIPStatus(anyInt(), any());
    Version version = new VersionImpl("test_store", 1, "dummy");
    doReturn(version).when(versionBackend).getVersion();
    Consumer<String> mockConsumer = mock(Consumer.class);
    versionBackend.maybeReportBatchEOIPStatus(0, mockConsumer);
    verify(mockConsumer, times(50)).accept(anyString());
    verify(mockConsumer, times(1)).accept("inc_0");
    verify(mockConsumer, times(1)).accept("inc_49");
    Assert.assertFalse(partitionToBatchReportEOIPEnabled.get(0));
  }

  @Test
  public void testSendOutHeartBeat() {
    String storeName = "test_store";
    DaVinciBackend backend = mock(DaVinciBackend.class);
    doReturn(true).when(backend).hasCurrentVersionBootstrapping();
    PushStatusStoreWriter mockWriter = mock(PushStatusStoreWriter.class);
    doReturn(mockWriter).when(backend).getPushStatusStoreWriter();

    Version currentVersion = mock(Version.class);
    doReturn(storeName).when(currentVersion).getStoreName();
    doReturn(1).when(currentVersion).getNumber();
    Version futureVersion = mock(Version.class);
    doReturn(storeName).when(futureVersion).getStoreName();
    doReturn(2).when(futureVersion).getNumber();

    VersionBackend.sendOutHeartbeat(backend, currentVersion);
    VersionBackend.sendOutHeartbeat(backend, futureVersion);

    verify(mockWriter, times(2)).writeHeartbeatForBootstrappingInstance(storeName);
    verify(mockWriter, never()).writeHeartbeat(storeName);

    doReturn(false).when(backend).hasCurrentVersionBootstrapping();
    VersionBackend.sendOutHeartbeat(backend, currentVersion);
    VersionBackend.sendOutHeartbeat(backend, futureVersion);

    verify(mockWriter, times(2)).writeHeartbeat(storeName);
  }
}
