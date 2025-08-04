package com.linkedin.davinci.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatLagMonitorAction;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreVersionInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.metrics.MetricsRepository;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceLeaderFollowerStateModelTest extends
    AbstractVenicePartitionStateModelTest<LeaderFollowerPartitionStateModel, LeaderFollowerIngestionProgressNotifier> {
  private HeartbeatMonitoringService spyHeartbeatMonitoringService;

  @Override
  protected LeaderFollowerPartitionStateModel getParticipantStateModel() {
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(new HashSet<>()).when(serverConfig).getRegionNames();
    doReturn("local").when(serverConfig).getRegionName();
    doReturn(Duration.ofSeconds(5)).when(serverConfig).getServerMaxWaitForVersionInfo();
    HeartbeatMonitoringService heartbeatMonitoringService =
        new HeartbeatMonitoringService(new MetricsRepository(), mockReadOnlyStoreRepository, serverConfig, null);
    spyHeartbeatMonitoringService = spy(heartbeatMonitoringService);
    return new LeaderFollowerPartitionStateModel(
        mockIngestionBackend,
        mockStoreConfig,
        testPartition,
        mockNotifier,
        mockReadOnlyStoreRepository,
        CompletableFuture.completedFuture(mockPushStatusAccessor),
        null,
        mockParticipantStateTransitionStats,
        spyHeartbeatMonitoringService);
  }

  @Override
  protected LeaderFollowerIngestionProgressNotifier getNotifier() throws InterruptedException {
    LeaderFollowerIngestionProgressNotifier mockNotifier = mock(LeaderFollowerIngestionProgressNotifier.class);
    doAnswer(invocation -> {
      Thread.sleep(3000);
      return null;
    }).when(mockNotifier).waitConsumptionCompleted(anyString(), anyInt(), anyInt(), any());
    return mockNotifier;
  }

  @Test
  public void testOnBecomeFollowerFromOffline() throws Exception {
    // if the resource is not the current serving version, latch is not placed.
    // Note that the mockStore has version 1 for the resource
    when(mockStore.getCurrentVersion()).thenReturn(2);
    testStateModel.onBecomeStandbyFromOffline(mockMessage, mockContext);
    verify(mockNotifier, never()).startConsumption(mockMessage.getResourceName(), testPartition);
    verify(mockNotifier, never()).waitConsumptionCompleted(
        mockMessage.getResourceName(),
        testPartition,
        Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS,
        mockStoreIngestionService);

    when(mockSystemStore.getCurrentVersion()).thenReturn(2);
    testStateModel.onBecomeStandbyFromOffline(mockSystemStoreMessage, mockContext);
    verify(mockNotifier, never()).startConsumption(mockSystemStoreMessage.getResourceName(), testPartition);
    verify(mockNotifier, never()).waitConsumptionCompleted(
        mockSystemStoreMessage.getResourceName(),
        testPartition,
        Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS,
        mockStoreIngestionService);

    // When serving current version system store, it should have latch in place.
    when(mockSystemStore.getCurrentVersion()).thenReturn(1);
    testStateModel.onBecomeStandbyFromOffline(mockSystemStoreMessage, mockContext);
    verify(mockNotifier).startConsumption(mockSystemStoreMessage.getResourceName(), testPartition);
    verify(mockNotifier).waitConsumptionCompleted(
        mockSystemStoreMessage.getResourceName(),
        testPartition,
        Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS,
        mockStoreIngestionService);

    when(mockStore.getCurrentVersion()).thenReturn(1);
    testStateModel.onBecomeStandbyFromOffline(mockMessage, mockContext);
    verify(mockNotifier).startConsumption(mockMessage.getResourceName(), testPartition);
    verify(mockNotifier).waitConsumptionCompleted(
        mockMessage.getResourceName(),
        testPartition,
        Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS,
        mockStoreIngestionService);

    // When its future version that is completed, it should have a latch in place.
    when(mockStore.getCurrentVersion()).thenReturn(0);
    when(mockStore.getVersionStatus(1)).thenReturn(VersionStatus.PUSHED);
    Version mockVersion = mock(Version.class);
    when(mockStore.getVersion(1)).thenReturn(mockVersion);
    when(mockVersion.getStatus()).thenReturn(VersionStatus.PUSHED);
    testStateModel.onBecomeStandbyFromOffline(mockMessage, mockContext);
    verify(mockNotifier, times(2)).startConsumption(mockMessage.getResourceName(), testPartition);
    verify(mockNotifier, times(2)).waitConsumptionCompleted(
        mockMessage.getResourceName(),
        testPartition,
        Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS,
        mockStoreIngestionService);

    // When its future version that is not completed, it should not have a latch in place
    when(mockStore.getVersionStatus(1)).thenReturn(VersionStatus.STARTED);
    when(mockVersion.getStatus()).thenReturn(VersionStatus.STARTED);
    testStateModel.onBecomeStandbyFromOffline(mockMessage, mockContext);
    verify(mockNotifier, times(2)).startConsumption(mockMessage.getResourceName(), testPartition);
    verify(mockNotifier, times(2)).waitConsumptionCompleted(
        mockMessage.getResourceName(),
        testPartition,
        Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS,
        mockStoreIngestionService);

  }

  @Test
  public void testGracefulDropForCurrentVersionResource() {
    // if the resource is not the current serving version, state transition thread will not be blocked.
    when(mockStore.getCurrentVersion()).thenReturn(2);
    testStateModel.onBecomeDroppedFromOffline(mockMessage, mockContext);
    verify(mockParticipantStateTransitionStats, never()).incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    verify(mockParticipantStateTransitionStats, never()).decrementThreadBlockedOnOfflineToDroppedTransitionCount();

    // if the resource is the current serving version, state transition thread will be blocked.
    when(mockStore.getCurrentVersion()).thenReturn(1);
    testStateModel.onBecomeDroppedFromOffline(mockMessage, mockContext);
    verify(mockParticipantStateTransitionStats, times(1)).incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    verify(mockParticipantStateTransitionStats, times(1)).decrementThreadBlockedOnOfflineToDroppedTransitionCount();
  }

  @Test
  public void testRemoveCVStateWhenBecomeOfflineFromStandby() {
    Version version = new VersionImpl("mockStore.getName()", 2, "");
    when(mockStore.getVersion(Mockito.anyInt())).thenReturn(version);
    when(mockStore.getCurrentVersion()).thenReturn(2);
    when(mockIngestionBackend.stopConsumption(any(VeniceStoreVersionConfig.class), eq(testPartition)))
        .thenReturn(CompletableFuture.completedFuture(null));

    testStateModel.onBecomeOfflineFromStandby(mockMessage, mockContext);

    verify(mockIngestionBackend).stopConsumption(any(VeniceStoreVersionConfig.class), eq(testPartition));
    verify(mockPushStatusAccessor).deleteReplicaStatus(resourceName, testPartition);
  }

  @Test
  public void testWhenBecomeOfflineFromStandbyWithVersionDeletion() {
    when(mockStore.getVersion(1)).thenReturn(null);
    when(mockStore.getCurrentVersion()).thenReturn(2);
    when(mockIngestionBackend.stopConsumption(any(VeniceStoreVersionConfig.class), eq(testPartition)))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(mockReadOnlyStoreRepository.waitVersion(eq(storeName), eq(version), any(), anyLong()))
        .thenReturn(new StoreVersionInfo(mockStore, null));
    testStateModel.onBecomeOfflineFromStandby(mockMessage, mockContext);
    verify(spyHeartbeatMonitoringService).removeLagMonitor(any(), eq(testPartition));
  }

  @Test
  public void testTransitionToDropIsBlockedOnDropPartitionAction() {
    // if the resource is not the current serving version, state transition thread will not be blocked. However, we
    // still block it once to wait for drop partition action if it's done asynchronously via SIT
    CompletableFuture mockDropPartitionFuture = mock(CompletableFuture.class);
    when(mockDropPartitionFuture.isDone()).thenReturn(false);
    when(mockIngestionBackend.dropStoragePartitionGracefully(any(), anyInt(), anyInt()))
        .thenReturn(mockDropPartitionFuture);
    when(mockStore.getCurrentVersion()).thenReturn(2);
    testStateModel.onBecomeDroppedFromOffline(mockMessage, mockContext);
    verify(mockParticipantStateTransitionStats, times(1)).incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    verify(mockParticipantStateTransitionStats, times(1)).decrementThreadBlockedOnOfflineToDroppedTransitionCount();

    reset(mockParticipantStateTransitionStats);
    // if the resource is the current serving version, state transition thread will be blocked. And it will be blocked
    // again for drop partition action if it's done asynchronously via SIT
    when(mockStore.getCurrentVersion()).thenReturn(1);
    testStateModel.onBecomeDroppedFromOffline(mockMessage, mockContext);
    verify(mockParticipantStateTransitionStats, times(2)).incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    verify(mockParticipantStateTransitionStats, times(2)).decrementThreadBlockedOnOfflineToDroppedTransitionCount();
  }

  @Test
  public void testSetHeartbeatMonitoringWhen() {
    when(mockStore.getCurrentVersion()).thenReturn(1);
    when(mockReadOnlyStoreRepository.waitVersion(eq(storeName), eq(version), any(), anyLong()))
        .thenReturn(new StoreVersionInfo(mockStore, null));
    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
      testStateModel.onBecomeStandbyFromOffline(mockMessage, mockContext);
    });
    try {
      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
        // Make sure lag monitor happens before wait latch action completes.
        verify(spyHeartbeatMonitoringService)
            .updateLagMonitor(any(), eq(testPartition), eq(HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR));
        Assert.assertFalse(future.isDone());
      });
    } finally {
      future.complete(null);
    }
  }
}
