package com.linkedin.davinci.helix;

import static com.linkedin.venice.helix.HelixState.DROPPED_STATE;
import static com.linkedin.venice.helix.HelixState.LEADER_STATE;
import static com.linkedin.venice.helix.HelixState.OFFLINE_STATE;
import static com.linkedin.venice.helix.HelixState.STANDBY_STATE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.DefaultIngestionBackend;
import com.linkedin.davinci.ingestion.IngestionBackend;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.stats.ParticipantStateTransitionStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatLagMonitorAction;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.davinci.store.rocksdb.RocksDBStorageEngineFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class LeaderFollowerPartitionStateModelTest {
  private IngestionBackend ingestionBackend;
  private KafkaStoreIngestionService storeIngestionService;
  private VeniceStoreVersionConfig storeAndServerConfigs;
  private LeaderFollowerIngestionProgressNotifier notifier;
  private ReadOnlyStoreRepository metadataRepo;
  private CompletableFuture<HelixPartitionStatusAccessor> partitionPushStatusAccessorFuture;
  private ParticipantStateTransitionStats stateTransitionStats;
  private HeartbeatMonitoringService heartbeatMonitoringService;
  private LeaderFollowerPartitionStateModel leaderFollowerPartitionStateModel;
  private static final String storeName = "store_85c9234588_1cce12d5";
  private static final int storeVersion = 3;
  private static final int partition = 0;
  private static final String resourceName = storeName + "_v" + storeVersion;

  @BeforeMethod
  public void setUp() {
    ingestionBackend = mock(IngestionBackend.class);
    storeIngestionService = mock(KafkaStoreIngestionService.class);
    doReturn(storeIngestionService).when(ingestionBackend).getStoreIngestionService();
    doReturn(CompletableFuture.completedFuture(null)).when(ingestionBackend)
        .stopConsumption(any(), anyInt(), anyString());
    doReturn(CompletableFuture.completedFuture(null)).when(ingestionBackend)
        .dropStoragePartitionGracefully(any(), anyInt(), anyInt(), anyString());
    doReturn(CompletableFuture.completedFuture(null)).when(storeIngestionService).stopConsumption(any(), anyInt());

    storeAndServerConfigs = mock(VeniceStoreVersionConfig.class);
    when(storeAndServerConfigs.getStoreVersionMetadataWaitDuringStateTransitionTimeMs()).thenReturn(5000L);
    when(storeAndServerConfigs.getStopConsumptionTimeoutInSeconds()).thenReturn(17);
    notifier = mock(LeaderFollowerIngestionProgressNotifier.class);
    metadataRepo = mock(ReadOnlyStoreRepository.class);
    // Default store mock so waitForVersionToBeAvailable() finds the version immediately
    Store defaultStore = mock(Store.class);
    when(defaultStore.getVersion(storeVersion)).thenReturn(mock(Version.class));
    doReturn(defaultStore).when(metadataRepo).getStoreOrThrow(storeName);
    partitionPushStatusAccessorFuture = CompletableFuture.completedFuture(mock(HelixPartitionStatusAccessor.class));
    stateTransitionStats = mock(ParticipantStateTransitionStats.class);
    heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    leaderFollowerPartitionStateModel = buildModel(notifier);
  }

  private LeaderFollowerPartitionStateModel buildModel(LeaderFollowerIngestionProgressNotifier notifier) {
    return new LeaderFollowerPartitionStateModel(
        ingestionBackend,
        storeAndServerConfigs,
        partition,
        notifier,
        metadataRepo,
        partitionPushStatusAccessorFuture,
        "instanceName",
        stateTransitionStats,
        heartbeatMonitoringService,
        resourceName);
  }

  /**
   * Stubs {@code metadataRepo} so the version under test resolves to {@link VersionStatus#STARTED}, satisfying
   * the per-poll status re-check inside {@code waitUntilFutureVersionLagAcceptable}.
   */
  private void stubVersionStarted() {
    Store store = mock(Store.class);
    Version mockVersion = mock(Version.class);
    when(mockVersion.getStatus()).thenReturn(VersionStatus.STARTED);
    when(store.getVersion(storeVersion)).thenReturn(mockVersion);
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());
  }

  @Test
  public void testUpdateLagMonitor() {
    Message message = mock(Message.class);
    NotificationContext context = mock(NotificationContext.class);
    when(message.getResourceName()).thenReturn(resourceName);
    Store store = mock(Store.class);
    when(store.getVersionStatus(anyInt())).thenReturn(VersionStatus.STARTED);
    Version mockVersion = mock(Version.class);
    when(mockVersion.getStatus()).thenReturn(VersionStatus.STARTED);
    when(store.getVersion(storeVersion)).thenReturn(mockVersion);
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());

    LeaderFollowerPartitionStateModel leaderFollowerPartitionStateModelSpy = spy(leaderFollowerPartitionStateModel);

    // STANDBY->LEADER
    leaderFollowerPartitionStateModelSpy.onBecomeLeaderFromStandby(message, context);
    verify(heartbeatMonitoringService, never()).updateLagMonitor(
        eq(resourceName),
        eq(partition),
        eq(HeartbeatLagMonitorAction.SET_LEADER_MONITOR),
        anyString());

    // LEADER->STANDBY
    leaderFollowerPartitionStateModelSpy.onBecomeStandbyFromLeader(message, context);
    verify(heartbeatMonitoringService, never()).updateLagMonitor(
        eq(resourceName),
        eq(partition),
        eq(HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR),
        anyString());

    // OFFLINE->STANDBY
    leaderFollowerPartitionStateModelSpy.onBecomeStandbyFromOffline(message, context);
    verify(heartbeatMonitoringService, times(1)).updateLagMonitor(
        eq(resourceName),
        eq(partition),
        eq(HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR),
        anyString());

    // STANDBY->OFFLINE
    leaderFollowerPartitionStateModelSpy.onBecomeOfflineFromStandby(message, context);
    verify(heartbeatMonitoringService)
        .updateLagMonitor(eq(resourceName), eq(partition), eq(HeartbeatLagMonitorAction.REMOVE_MONITOR), anyString());
  }

  @Test
  public void testStateModelStats() {
    Message message = mock(Message.class);
    NotificationContext context = mock(NotificationContext.class);
    when(message.getResourceName()).thenReturn(resourceName);
    Store store = mock(Store.class);
    when(store.getVersionStatus(anyInt())).thenReturn(VersionStatus.STARTED);
    Version mockVersion = mock(Version.class);
    when(mockVersion.getStatus()).thenReturn(VersionStatus.STARTED);
    when(store.getVersion(storeVersion)).thenReturn(mockVersion);
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());

    LeaderFollowerPartitionStateModel leaderFollowerPartitionStateModelSpy = spy(leaderFollowerPartitionStateModel);

    // OFFLINE->STANDBY
    doReturn(OFFLINE_STATE).when(message).getFromState();
    doReturn(STANDBY_STATE).when(message).getToState();
    leaderFollowerPartitionStateModelSpy.onBecomeStandbyFromOffline(message, context);
    verify(stateTransitionStats).trackStateTransitionStarted(OFFLINE_STATE, STANDBY_STATE);
    verify(stateTransitionStats).trackStateTransitionCompleted(OFFLINE_STATE, STANDBY_STATE);

    // STANDBY->LEADER
    doReturn(STANDBY_STATE).when(message).getFromState();
    doReturn(LEADER_STATE).when(message).getToState();
    leaderFollowerPartitionStateModelSpy.onBecomeLeaderFromStandby(message, context);
    verify(stateTransitionStats).trackStateTransitionStarted(STANDBY_STATE, LEADER_STATE);
    verify(stateTransitionStats).trackStateTransitionCompleted(STANDBY_STATE, LEADER_STATE);

    // LEADER->STANDBY
    doReturn(LEADER_STATE).when(message).getFromState();
    doReturn(STANDBY_STATE).when(message).getToState();
    leaderFollowerPartitionStateModelSpy.onBecomeStandbyFromLeader(message, context);
    verify(stateTransitionStats).trackStateTransitionStarted(LEADER_STATE, STANDBY_STATE);
    verify(stateTransitionStats).trackStateTransitionCompleted(LEADER_STATE, STANDBY_STATE);

    // STANDBY->OFFLINE
    doReturn(STANDBY_STATE).when(message).getFromState();
    doReturn(OFFLINE_STATE).when(message).getToState();
    leaderFollowerPartitionStateModelSpy.onBecomeOfflineFromStandby(message, context);
    verify(stateTransitionStats).trackStateTransitionStarted(STANDBY_STATE, OFFLINE_STATE);
    verify(stateTransitionStats).trackStateTransitionCompleted(STANDBY_STATE, OFFLINE_STATE);

    // OFFLINE -> DROPPED
    doReturn(OFFLINE_STATE).when(message).getFromState();
    doReturn(DROPPED_STATE).when(message).getToState();
    leaderFollowerPartitionStateModelSpy.onBecomeDroppedFromOffline(message, context);
    verify(stateTransitionStats).trackStateTransitionStarted(OFFLINE_STATE, DROPPED_STATE);
    verify(stateTransitionStats).trackStateTransitionCompleted(OFFLINE_STATE, DROPPED_STATE);
  }

  /**
   * When the transition handler throws, {@code executeStateTransition} must call
   * {@link ParticipantStateTransitionStats#trackStateTransitionFailed} (not
   * {@link ParticipantStateTransitionStats#trackStateTransitionCompleted}) so the in-progress
   * counter does not leak +1, and the exception must still propagate.
   */
  @Test
  public void testStateTransitionFailureRecordedOnHandlerException() {
    Message message = mock(Message.class);
    NotificationContext context = mock(NotificationContext.class);
    when(message.getResourceName()).thenReturn(resourceName);
    doReturn(STANDBY_STATE).when(message).getFromState();
    doReturn(LEADER_STATE).when(message).getToState();

    // The STANDBY->LEADER handler invokes promoteToLeader; force it to throw.
    doThrow(new RuntimeException("simulated handler failure")).when(storeIngestionService)
        .promoteToLeader(any(), anyInt(), any());

    try {
      leaderFollowerPartitionStateModel.onBecomeLeaderFromStandby(message, context);
      fail("Expected handler exception to propagate");
    } catch (RuntimeException expected) {
      assertEquals(expected.getMessage(), "simulated handler failure");
    }

    verify(stateTransitionStats).trackStateTransitionStarted(STANDBY_STATE, LEADER_STATE);
    verify(stateTransitionStats).trackStateTransitionFailed(STANDBY_STATE, LEADER_STATE);
    verify(stateTransitionStats, never()).trackStateTransitionCompleted(STANDBY_STATE, LEADER_STATE);
  }

  private Message prepareGracefulDrop() {
    when(metadataRepo.getStoreOrThrow(storeName).getCurrentVersion()).thenReturn(storeVersion);
    when(storeAndServerConfigs.getStoreVersionName()).thenReturn(resourceName);
    when(storeAndServerConfigs.getPartitionGracefulDropDelaySeconds()).thenReturn(30);
    Message message = mock(Message.class);
    when(message.getResourceName()).thenReturn(resourceName);
    when(message.getFromState()).thenReturn(OFFLINE_STATE);
    when(message.getToState()).thenReturn(DROPPED_STATE);
    return message;
  }

  @DataProvider
  public Object[][] gracefulDropOrdering() {
    return new Object[][] { { true, 30 }, { false, 30 }, { true, 0 }, { false, 0 } };
  }

  @Test(dataProvider = "gracefulDropOrdering")
  public void testGracefulDropWithdrawsBeforeFullDrain(boolean offlineObserved, int delaySeconds) throws Exception {
    Message message = prepareGracefulDrop();
    when(storeAndServerConfigs.getPartitionGracefulDropDelaySeconds()).thenReturn(delaySeconds);
    doReturn(new CompletableFuture<>()).when(ingestionBackend).stopConsumption(any(), anyInt(), anyString());
    if (offlineObserved) {
      leaderFollowerPartitionStateModel.onBecomeOfflineFromStandby(message, null);
    }
    CompletableFuture<Void> unsubscribe = mock(CompletableFuture.class);
    doReturn(unsubscribe).when(storeIngestionService).stopConsumption(storeAndServerConfigs, partition);
    HelixPartitionStatusAccessor accessor = partitionPushStatusAccessorFuture.join();
    doAnswer(invocation -> {
      verify(accessor).deleteReplicaStatus(resourceName, partition);
      doThrow(new VeniceException("Synthetic post-drop withdrawal failure")).when(accessor)
          .deleteReplicaStatus(anyString(), anyInt());
      return CompletableFuture.completedFuture(null);
    }).when(ingestionBackend).dropStoragePartitionGracefully(any(), anyInt(), anyInt(), anyString());
    try (MockedStatic<Utils> utils = mockStatic(Utils.class, CALLS_REAL_METHODS)) {
      utils.when(() -> Utils.sleep(anyLong())).thenReturn(true);
      when(unsubscribe.get(17, TimeUnit.SECONDS)).thenAnswer(invocation -> {
        verify(accessor, never()).deleteReplicaStatus(anyString(), anyInt());
        utils.verify(() -> Utils.sleep(anyLong()), never());
        verify(ingestionBackend, never()).dropStoragePartitionGracefully(any(), anyInt(), anyInt(), anyString());
        return null;
      });
      utils.when(() -> Utils.sleep(TimeUnit.SECONDS.toMillis(delaySeconds))).thenAnswer(invocation -> {
        verify(unsubscribe).get(17, TimeUnit.SECONDS);
        verify(accessor).deleteReplicaStatus(resourceName, partition);
        verify(ingestionBackend, never()).dropStoragePartitionGracefully(any(), anyInt(), anyInt(), anyString());
        return true;
      });
      leaderFollowerPartitionStateModel.onBecomeDroppedFromOffline(message, null);
      utils.verify(() -> Utils.sleep(anyLong()), times(delaySeconds > 0 ? 1 : 0));
    }
    InOrder order = inOrder(unsubscribe, accessor, ingestionBackend);
    order.verify(unsubscribe).get(17, TimeUnit.SECONDS);
    order.verify(accessor).deleteReplicaStatus(resourceName, partition);
    order.verify(ingestionBackend).dropStoragePartitionGracefully(any(), anyInt(), anyInt(), anyString());
    verify(accessor).deleteReplicaStatus(resourceName, partition);
    verify(stateTransitionStats).incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    verify(stateTransitionStats).decrementThreadBlockedOnOfflineToDroppedTransitionCount();
  }

  @Test
  public void testGracefulDropRecoversFromFailedResetStop() throws Exception {
    Message message = prepareGracefulDrop();
    CompletableFuture<Void> resetStop = new CompletableFuture<>();
    resetStop.completeExceptionally(new VeniceException("Synthetic reset unsubscribe failure"));
    doReturn(resetStop).when(ingestionBackend).stopConsumption(any(), anyInt(), anyString());
    leaderFollowerPartitionStateModel.reset();

    CompletableFuture<Void> unsubscribe = mock(CompletableFuture.class);
    doReturn(unsubscribe).when(storeIngestionService).stopConsumption(storeAndServerConfigs, partition);
    when(unsubscribe.get(17, TimeUnit.SECONDS)).thenReturn(null);
    HelixPartitionStatusAccessor accessor = partitionPushStatusAccessorFuture.join();
    try (MockedStatic<Utils> utils = mockStatic(Utils.class, CALLS_REAL_METHODS)) {
      utils.when(() -> Utils.sleep(anyLong())).thenReturn(true);
      utils.when(() -> Utils.sleep(TimeUnit.SECONDS.toMillis(30))).thenAnswer(invocation -> {
        verify(unsubscribe).get(17, TimeUnit.SECONDS);
        verify(accessor).deleteReplicaStatus(resourceName, partition);
        verify(ingestionBackend, never()).dropStoragePartitionGracefully(any(), anyInt(), anyInt(), anyString());
        return true;
      });
      leaderFollowerPartitionStateModel.onBecomeDroppedFromOffline(message, null);
      utils.verify(() -> Utils.sleep(TimeUnit.SECONDS.toMillis(30)));
    }
    InOrder order = inOrder(ingestionBackend, storeIngestionService, unsubscribe, accessor);
    order.verify(ingestionBackend).stopConsumption(eq(storeAndServerConfigs), eq(partition), anyString());
    order.verify(storeIngestionService).stopConsumption(storeAndServerConfigs, partition);
    order.verify(unsubscribe).get(17, TimeUnit.SECONDS);
    order.verify(accessor).deleteReplicaStatus(resourceName, partition);
    order.verify(ingestionBackend).dropStoragePartitionGracefully(any(), anyInt(), anyInt(), anyString());
    assertTrue(resetStop.isCompletedExceptionally());
  }

  @DataProvider
  public Object[][] gracefulDropFailures() {
    return new Object[][] { { "unsubscribe" }, { "timeout" }, { "withdrawal" }, { "interrupt-unsubscribe" },
        { "interrupt-drain" }, { "interrupt-accessor" }, { "interrupt-retry" }, { "interrupt-withdrawal" } };
  }

  @Test(dataProvider = "gracefulDropFailures")
  public void testGracefulDropFailureRetainsStorage(String failureStep) throws Exception {
    Message message = prepareGracefulDrop();
    HelixPartitionStatusAccessor accessor = partitionPushStatusAccessorFuture.join();
    if ("interrupt-accessor".equals(failureStep)) {
      partitionPushStatusAccessorFuture = new CompletableFuture<>();
      leaderFollowerPartitionStateModel = buildModel(notifier);
    }
    if ("interrupt-accessor".equals(failureStep) || "interrupt-retry".equals(failureStep)
        || "interrupt-withdrawal".equals(failureStep)) {
      when(storeAndServerConfigs.getPartitionGracefulDropDelaySeconds()).thenReturn(0);
    }
    CompletableFuture<Void> unsubscribe = mock(CompletableFuture.class);
    doReturn(unsubscribe).when(storeIngestionService).stopConsumption(any(), anyInt());
    when(unsubscribe.get(17, TimeUnit.SECONDS)).thenAnswer(invocation -> {
      switch (failureStep) {
        case "unsubscribe":
          throw new ExecutionException(new VeniceException("Synthetic unsubscribe failure"));
        case "timeout":
          throw new TimeoutException("Synthetic unsubscribe timeout");
        case "interrupt-unsubscribe":
          throw new InterruptedException("Synthetic unsubscribe interruption");
        case "interrupt-accessor":
          Thread.currentThread().interrupt();
          return null;
        default:
          return null;
      }
    });
    if ("withdrawal".equals(failureStep)) {
      doThrow(new VeniceException("Synthetic withdrawal failure")).when(accessor)
          .deleteReplicaStatus(anyString(), anyInt());
    }
    if ("interrupt-retry".equals(failureStep)) {
      doThrow(new VeniceException("Synthetic withdrawal failure")).doNothing()
          .when(accessor)
          .deleteReplicaStatus(anyString(), anyInt());
    } else if ("interrupt-withdrawal".equals(failureStep)) {
      doAnswer(invocation -> {
        Thread.currentThread().interrupt();
        return null;
      }).when(accessor).deleteReplicaStatus(anyString(), anyInt());
    }
    try (MockedStatic<Utils> utils = mockStatic(Utils.class, CALLS_REAL_METHODS)) {
      utils.when(() -> Utils.sleep(anyLong())).thenReturn(true);
      if ("interrupt-drain".equals(failureStep)) {
        utils.when(() -> Utils.sleep(TimeUnit.SECONDS.toMillis(30))).thenAnswer(invocation -> {
          Thread.currentThread().interrupt();
          return false;
        });
      } else if ("interrupt-retry".equals(failureStep)) {
        utils.when(() -> Utils.sleep(1000)).thenAnswer(invocation -> {
          Thread.currentThread().interrupt();
          return false;
        });
      }
      VeniceException exception = expectThrows(
          VeniceException.class,
          () -> leaderFollowerPartitionStateModel.onBecomeDroppedFromOffline(message, null));
      assertEquals(Thread.currentThread().isInterrupted(), failureStep.startsWith("interrupt"));
      if ("interrupt-accessor".equals(failureStep)) {
        assertTrue(exception.getCause() instanceof InterruptedException);
        assertFalse(partitionPushStatusAccessorFuture.isDone());
        verify(accessor, never()).deleteReplicaStatus(anyString(), anyInt());
      } else if ("interrupt-retry".equals(failureStep)) {
        verify(accessor).deleteReplicaStatus(resourceName, partition);
      }
    } finally {
      Thread.interrupted();
    }
    verify(unsubscribe).get(17, TimeUnit.SECONDS);
    verify(ingestionBackend, never()).dropStoragePartitionGracefully(any(), anyInt(), anyInt(), anyString());
    verify(stateTransitionStats).trackStateTransitionFailed(OFFLINE_STATE, DROPPED_STATE);
    verify(stateTransitionStats).incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    verify(stateTransitionStats).decrementThreadBlockedOnOfflineToDroppedTransitionCount();
  }

  @DataProvider
  public Object[][] gracefulDropVersionRoles() {
    return new Object[][] { { storeVersion, false, false, true, true }, { storeVersion + 2, false, false, true, true },
        { storeVersion + 3, false, false, false, true }, { storeVersion - 2, false, false, false, true },
        { Store.NON_EXISTING_VERSION, false, false, false, true }, { storeVersion + 2, true, false, true, true },
        { storeVersion + 2, false, true, false, true }, { storeVersion, false, false, true, false },
        { storeVersion + 2, false, false, false, false }, { storeVersion + 2, true, false, false, false } };
  }

  @Test(dataProvider = "gracefulDropVersionRoles")
  public void testGracefulDropForCurrentAndBackupVersions(
      int currentVersion,
      boolean demoteBeforeDrop,
      boolean versionRemovedFromMetadata,
      boolean expectDelay,
      boolean backupGracefulDropEnabled) {
    Message message = mock(Message.class);
    NotificationContext context = mock(NotificationContext.class);
    when(message.getResourceName()).thenReturn(resourceName);
    when(storeAndServerConfigs.getPartitionGracefulDropDelaySeconds()).thenReturn(30);
    when(storeAndServerConfigs.isBackupVersionGracefulDropEnabled()).thenReturn(backupGracefulDropEnabled);

    Store store = mock(Store.class);
    when(store.getCurrentVersion()).thenReturn(demoteBeforeDrop ? storeVersion : currentVersion);
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());
    Version failedVersion = new VersionImpl(storeName, storeVersion + 1, "test-push");
    List<Version> versions = new ArrayList<>(
        Arrays.asList(
            new VersionImpl(storeName, storeVersion + 3, "test-push"),
            new VersionImpl(storeName, storeVersion - 2, "test-push"),
            new VersionImpl(storeName, storeVersion + 2, "test-push"),
            failedVersion));
    if (!versionRemovedFromMetadata) {
      versions.add(new VersionImpl(storeName, storeVersion, "test-push"));
    }
    versions.forEach(version -> version.setStatus(VersionStatus.ONLINE));
    failedVersion.setStatus(VersionStatus.ERROR);
    when(store.getVersions()).thenReturn(versions);

    if (demoteBeforeDrop) {
      leaderFollowerPartitionStateModel.onBecomeOfflineFromStandby(message, context);
      when(store.getCurrentVersion()).thenReturn(currentVersion);
    }

    int expectedWaitCount = expectDelay ? 1 : 0;
    long gracefulDropDelayMs = TimeUnit.SECONDS.toMillis(30);
    try (MockedStatic<Utils> utils = mockStatic(Utils.class, CALLS_REAL_METHODS)) {
      utils.when(() -> Utils.sleep(anyLong())).thenAnswer(invocation -> {
        long delayMs = invocation.getArgument(0);
        assertEquals(delayMs, gracefulDropDelayMs);
        verify(ingestionBackend, never()).dropStoragePartitionGracefully(any(), anyInt(), anyInt(), anyString());
        return true;
      });

      leaderFollowerPartitionStateModel.onBecomeDroppedFromOffline(message, context);

      utils.verify(() -> Utils.sleep(anyLong()), times(expectedWaitCount));
    }

    verify(stateTransitionStats, times(expectedWaitCount)).incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    verify(stateTransitionStats, times(expectedWaitCount)).decrementThreadBlockedOnOfflineToDroppedTransitionCount();
    verify(ingestionBackend).dropStoragePartitionGracefully(any(), anyInt(), anyInt(), anyString());
  }

  @Test
  public void testDeletedStoreSkipsGracefulDropDelay() {
    Message message = mock(Message.class);
    when(message.getResourceName()).thenReturn(resourceName);
    when(storeAndServerConfigs.getPartitionGracefulDropDelaySeconds()).thenReturn(30);
    doThrow(new VeniceNoStoreException(storeName)).when(metadataRepo).getStoreOrThrow(storeName);

    try (MockedStatic<Utils> utils = mockStatic(Utils.class, CALLS_REAL_METHODS)) {
      utils.when(() -> Utils.sleep(anyLong())).thenReturn(true);

      leaderFollowerPartitionStateModel.onBecomeDroppedFromOffline(message, mock(NotificationContext.class));

      utils.verify(() -> Utils.sleep(anyLong()), never());
    }

    verify(ingestionBackend).dropStoragePartitionGracefully(any(), anyInt(), anyInt(), anyString());
    verify(stateTransitionStats, never()).incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    verify(stateTransitionStats, never()).decrementThreadBlockedOnOfflineToDroppedTransitionCount();
  }

  /**
   * OFFLINE->STANDBY needs to complete when the current version is demoted to a backup version.
   */
  @Test
  public void testOnStandbyCompletionUponDemotionToBackupVersion() throws Exception {
    Store store = mock(Store.class);
    when(store.getCurrentVersion()).thenReturn(storeVersion); // Version starts as current so the latch is created
    when(store.getBootstrapToOnlineTimeoutInHours()).thenReturn(24); // large timeout for latch await
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());
    when(store.getVersion(storeVersion)).thenReturn(mock(Version.class));
    LeaderFollowerIngestionProgressNotifier notifier = new LeaderFollowerIngestionProgressNotifier();
    LeaderFollowerPartitionStateModel model = buildModel(notifier);

    // Start the OFFLINE -> STANDBY transition in a background thread which should be blocked in
    // waitConsumptionCompleted, because this is the current version,
    Message message = mock(Message.class);
    when(message.getResourceName()).thenReturn(resourceName);
    NotificationContext context = mock(NotificationContext.class);
    CompletableFuture<Void> transitionFuture =
        CompletableFuture.runAsync(() -> model.onBecomeStandbyFromOffline(message, context));

    // Wait until the latch is created, confirming the transition is blocking.
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> assertNotNull(
            notifier.getIngestionCompleteFlag(resourceName, partition),
            "Latch must be created for a current-version OFFLINE->STANDBY transition"));
    assertFalse(transitionFuture.isDone(), "Transition must be blocked waiting for the latch");

    // Simulate CURRENT -> BACKUP version role flip: a newer version has been promoted.
    when(store.getCurrentVersion()).thenReturn(storeVersion + 1);

    // Simulate StoreIngestionTask.stopTrackingCurrentVersionIngestion() being called
    notifier.stopped(resourceName, partition, null);
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, transitionFuture::isDone);
    // Ensure the transition completed successfully (will throw if it completed exceptionally)
    transitionFuture.join();
    assertNull(
        notifier.getIngestionCompleteFlag(resourceName, partition),
        "Latch should be released/removed after current version is demoted");
  }

  /**
   * Integration test that verifies RocksDB deletion rate limiting is honored during
   * OFFLINE->DROPPED state transition (backup version deletion).
   * This test creates a real storage engine with data, then triggers the OFFLINE->DROPPED
   * state transition and verifies that the deletion is throttled according to the configured rate.
   *
   * This test also creates a snapshot (simulating blob transfer scenario) to verify that
   * rate limiting works correctly even when hard links exist.
   */
  @Test(groups = { "integration" })
  public void testOfflineToDroppedTransitionHonorsRateLimiting() throws Exception {
    // Setup: Create a real storage engine with rate limiting
    String testDataPath = Utils.getUniqueTempPath();
    long deletionRateBytesPerSec = 20L * 1024 * 1024; // 20 MB/s

    VeniceProperties veniceServerProperties = new PropertyBuilder()
        .put(AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB).toProperties())
        .put("data.base.path", testDataPath)
        .put(
            RocksDBServerConfig.ROCKSDB_SST_FILE_MANAGER_DELETE_RATE_BYTES_PER_SECOND,
            String.valueOf(deletionRateBytesPerSec))
        .build();

    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);

    try {
      // Create a storage engine with ~200 MB of data
      final String testStoreName = "test_store_rate_limit";
      final int testVersion = 1;
      final String testTopic = Version.composeKafkaTopic(testStoreName, testVersion);
      final int testPartition = 0;

      VeniceStoreVersionConfig testStoreConfig =
          new VeniceStoreVersionConfig(testTopic, veniceServerProperties, PersistenceType.ROCKS_DB);
      StorageEngine storeEngine = factory.getStorageEngine(testStoreConfig);
      storeEngine.addStoragePartitionIfAbsent(testPartition);

      // Write ~200 MB of data to generate SST files
      long targetDataSizeBytes = 200L * 1024 * 1024; // 200 MB
      long valueSizeBytes = 50 * 1024; // 50 KB per value
      long numRecords = targetDataSizeBytes / valueSizeBytes;

      Random random = new Random(42);
      byte[] valueBytes = new byte[(int) valueSizeBytes];

      for (int i = 0; i < numRecords; i++) {
        String keyString = "key_" + i;
        byte[] keyBytes = keyString.getBytes();
        random.nextBytes(valueBytes);
        storeEngine.put(testPartition, keyBytes, ByteBuffer.wrap(valueBytes));
      }

      // Verify data was written
      File storeDir = new File(factory.getRocksDBPath(testTopic, testPartition)).getParentFile();
      assertTrue(storeDir.exists(), "Store directory should exist before deletion");

      // Sync/flush data to disk before creating snapshot
      AbstractStoragePartition partition = storeEngine.getPartitionOrThrow(testPartition);
      partition.sync();

      // Create a snapshot to simulate blob transfer scenario
      // This creates hard links to the SST files, helping test the case where
      // snapshot exists during backup version deletion
      partition.createSnapshot();

      // Verify snapshot was created
      // Note: serverConfig.getRocksDBPath() returns dataBasePath + "/rocksdb"
      String rocksDBBasePath = serverConfig.getRocksDBPath();
      String snapshotPath = RocksDBUtils.composeSnapshotDir(rocksDBBasePath, testTopic, testPartition);
      File partitionSnapshotDir = new File(snapshotPath);
      assertTrue(partitionSnapshotDir.exists(), "Snapshot directory should exist before deletion");

      // Setup: Create a mock ingestion backend that uses the real storage service
      KafkaStoreIngestionService mockIngestionService = mock(KafkaStoreIngestionService.class);
      DefaultIngestionBackend realIngestionBackend = mock(DefaultIngestionBackend.class);

      // When dropStoragePartitionGracefully is called, actually delete the partition
      when(realIngestionBackend.dropStoragePartitionGracefully(any(), eq(testPartition), anyInt(), anyString()))
          .thenAnswer(invocation -> {
            // Simulate the actual deletion by calling factory.removeStorageEngine
            factory.removeStorageEngine(storeEngine);
            return CompletableFuture.completedFuture(null);
          });

      when(realIngestionBackend.getStoreIngestionService()).thenReturn(mockIngestionService);
      when(realIngestionBackend.stopConsumption(any(), anyInt(), anyString()))
          .thenReturn(CompletableFuture.completedFuture(null));

      // Setup: Create state model with real backend
      VeniceStoreVersionConfig stateModelConfig = mock(VeniceStoreVersionConfig.class);
      when(stateModelConfig.getStopConsumptionTimeoutInSeconds()).thenReturn(60);
      when(stateModelConfig.getPartitionGracefulDropDelaySeconds()).thenReturn(0); // No graceful drop delay

      ReadOnlyStoreRepository mockMetadataRepo = mock(ReadOnlyStoreRepository.class);
      Store mockStore = mock(Store.class);
      when(mockStore.getCurrentVersion()).thenReturn(testVersion + 1); // Not current version
      doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(anyString());

      ParticipantStateTransitionStats mockStats = mock(ParticipantStateTransitionStats.class);
      HeartbeatMonitoringService mockHeartbeatService = mock(HeartbeatMonitoringService.class);
      CompletableFuture<HelixPartitionStatusAccessor> mockAccessorFuture =
          CompletableFuture.completedFuture(mock(HelixPartitionStatusAccessor.class));

      LeaderFollowerPartitionStateModel stateModel = new LeaderFollowerPartitionStateModel(
          realIngestionBackend,
          stateModelConfig,
          testPartition,
          mock(LeaderFollowerIngestionProgressNotifier.class),
          mockMetadataRepo,
          mockAccessorFuture,
          "testInstance",
          mockStats,
          mockHeartbeatService,
          testTopic);

      // Execute: Trigger OFFLINE->DROPPED state transition
      Message message = mock(Message.class);
      when(message.getResourceName()).thenReturn(testTopic);
      NotificationContext context = mock(NotificationContext.class);

      long startTime = System.currentTimeMillis();
      stateModel.onBecomeDroppedFromOffline(message, context);
      long elapsedTime = System.currentTimeMillis() - startTime;

      // Verify: Deletion should be throttled (should take ~10 seconds for 200 MB at 20 MB/s)
      // This validates that rate limiting works even when a snapshot exists (hard links scenario)
      double elapsedTimeSec = elapsedTime / 1000.0;
      assertTrue(
          elapsedTimeSec >= 8.0,
          String.format(
              "Deletion during OFFLINE->DROPPED should be throttled (>= 8 seconds), but took %.2f seconds. "
                  + "This test includes snapshot creation to verify rate limiting works with hard links.",
              elapsedTimeSec));

      // Verify: Store directory should be deleted
      assertFalse(storeDir.exists(), "Store directory should be deleted after OFFLINE->DROPPED transition");

      // Verify: Snapshot directory should also be deleted
      assertFalse(
          partitionSnapshotDir.exists(),
          "Snapshot directory should be deleted after OFFLINE->DROPPED transition");

    } finally {
      factory.close();
      try {
        FileUtils.deleteDirectory(new File(testDataPath));
      } catch (Exception e) {
        System.err.println("Failed to cleanup test directory: " + testDataPath + ", error: " + e.getMessage());
      }
    }
  }

  /**
   * When {@link com.linkedin.venice.ConfigKeys#SERVER_FUTURE_VERSION_STANDBY_LAG_CHECK_ENABLED} is not enabled
   * (the default), {@code onBecomeStandbyFromOffline} must not invoke {@code waitUntilFutureVersionLagAcceptable}
   * at all for an in-progress future-version push, preserving the pre-existing (no-wait) behavior.
   */
  @Test
  public void testWaitUntilFutureVersionLagAcceptableDisabledByDefault() throws InterruptedException {
    Message message = mock(Message.class);
    NotificationContext context = mock(NotificationContext.class);
    when(message.getResourceName()).thenReturn(resourceName);

    Store store = mock(Store.class);
    Version mockVersion = mock(Version.class);
    when(mockVersion.getStatus()).thenReturn(VersionStatus.STARTED); // push still in progress, not ready
    when(store.getVersion(storeVersion)).thenReturn(mockVersion);
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());
    doReturn(store).when(metadataRepo).getStore(anyString());

    LeaderFollowerPartitionStateModel spyModel = spy(leaderFollowerPartitionStateModel);
    spyModel.onBecomeStandbyFromOffline(message, context);

    verify(spyModel, never()).waitUntilFutureVersionLagAcceptable(anyString());
  }

  /**
   * When lag is already within the configured threshold, the wait must return immediately after a single
   * measurement, without sleeping/polling further.
   */
  @Test
  public void testWaitUntilFutureVersionLagAcceptableProceedsWhenLagWithinThreshold() {
    when(storeAndServerConfigs.isFutureVersionStandbyLagCheckEnabled()).thenReturn(true);
    when(storeAndServerConfigs.getFutureVersionStandbyLagThreshold()).thenReturn(100L);
    when(storeAndServerConfigs.getFutureVersionStandbyLagCheckTimeoutMinutes()).thenReturn(5);
    when(storeAndServerConfigs.getFutureVersionStandbyLagCheckPollIntervalMinutes()).thenReturn(1);
    stubVersionStarted();

    StoreIngestionTask ingestionTask = mock(StoreIngestionTask.class);
    doReturn(ingestionTask).when(storeIngestionService).getStoreIngestionTask(resourceName);
    doReturn(50L).when(ingestionTask).getLocalVersionTopicLag(partition);

    leaderFollowerPartitionStateModel.waitUntilFutureVersionLagAcceptable(resourceName);

    verify(ingestionTask, times(1)).getLocalVersionTopicLag(partition);
  }

  /**
   * When lag starts above the threshold but catches up within the timeout window, the wait must keep polling
   * (re-measuring lag) until it becomes acceptable, then return.
   */
  @Test
  public void testWaitUntilFutureVersionLagAcceptablePollsUntilLagCatchesUp() {
    when(storeAndServerConfigs.isFutureVersionStandbyLagCheckEnabled()).thenReturn(true);
    when(storeAndServerConfigs.getFutureVersionStandbyLagThreshold()).thenReturn(100L);
    when(storeAndServerConfigs.getFutureVersionStandbyLagCheckTimeoutMinutes()).thenReturn(5);
    when(storeAndServerConfigs.getFutureVersionStandbyLagCheckPollIntervalMinutes()).thenReturn(0);
    stubVersionStarted();

    StoreIngestionTask ingestionTask = mock(StoreIngestionTask.class);
    doReturn(ingestionTask).when(storeIngestionService).getStoreIngestionTask(resourceName);
    doReturn(200L, 200L, 50L).when(ingestionTask).getLocalVersionTopicLag(partition);

    leaderFollowerPartitionStateModel.waitUntilFutureVersionLagAcceptable(resourceName);

    verify(ingestionTask, times(3)).getLocalVersionTopicLag(partition);
  }

  /**
   * If the version transitions away from {@link VersionStatus#STARTED} (e.g. to {@link VersionStatus#KILLED})
   * while polling, the wait must stop re-checking lag and return immediately on the next poll, rather than
   * continuing to occupy the state-transition worker thread until the timeout elapses.
   */
  @Test
  public void testWaitUntilFutureVersionLagAcceptableStopsWhenVersionNoLongerStarted() {
    when(storeAndServerConfigs.isFutureVersionStandbyLagCheckEnabled()).thenReturn(true);
    when(storeAndServerConfigs.getFutureVersionStandbyLagThreshold()).thenReturn(100L);
    when(storeAndServerConfigs.getFutureVersionStandbyLagCheckTimeoutMinutes()).thenReturn(5);
    when(storeAndServerConfigs.getFutureVersionStandbyLagCheckPollIntervalMinutes()).thenReturn(0);

    Store store = mock(Store.class);
    Version mockVersion = mock(Version.class);
    // First poll: still STARTED (so lag is measured once); subsequent polls: version has been killed.
    when(mockVersion.getStatus()).thenReturn(VersionStatus.STARTED, VersionStatus.KILLED);
    when(store.getVersion(storeVersion)).thenReturn(mockVersion);
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());

    StoreIngestionTask ingestionTask = mock(StoreIngestionTask.class);
    doReturn(ingestionTask).when(storeIngestionService).getStoreIngestionTask(resourceName);
    // Lag never catches up, so the only way the loop can exit before the timeout is via the status re-check.
    doReturn(200L).when(ingestionTask).getLocalVersionTopicLag(partition);

    leaderFollowerPartitionStateModel.waitUntilFutureVersionLagAcceptable(resourceName);

    // Lag should only have been measured once, before the version's status flipped away from STARTED.
    verify(ingestionTask, times(1)).getLocalVersionTopicLag(partition);
  }

  /**
   * When lag never catches up, the wait must give up after the configured timeout and proceed anyway
   * (best-effort semantics), rather than blocking indefinitely.
   */
  @Test
  public void testWaitUntilFutureVersionLagAcceptableTimesOutAndProceeds() {
    when(storeAndServerConfigs.isFutureVersionStandbyLagCheckEnabled()).thenReturn(true);
    when(storeAndServerConfigs.getFutureVersionStandbyLagThreshold()).thenReturn(100L);
    when(storeAndServerConfigs.getFutureVersionStandbyLagCheckTimeoutMinutes()).thenReturn(0);
    when(storeAndServerConfigs.getFutureVersionStandbyLagCheckPollIntervalMinutes()).thenReturn(0);
    stubVersionStarted();

    StoreIngestionTask ingestionTask = mock(StoreIngestionTask.class);
    doReturn(ingestionTask).when(storeIngestionService).getStoreIngestionTask(resourceName);
    doReturn(200L).when(ingestionTask).getLocalVersionTopicLag(partition);

    // Must return promptly (best-effort timeout) instead of looping forever.
    leaderFollowerPartitionStateModel.waitUntilFutureVersionLagAcceptable(resourceName);

    verify(ingestionTask, atLeastOnce()).getLocalVersionTopicLag(partition);
  }

  /**
   * When lag cannot be measured (e.g. PubSub error surfaced as {@code Long.MAX_VALUE}), the wait must fail open
   * and proceed immediately, matching the pre-existing (no-wait) behavior.
   */
  @Test
  public void testWaitUntilFutureVersionLagAcceptableFailsOpenWhenLagCannotBeMeasured() {
    when(storeAndServerConfigs.isFutureVersionStandbyLagCheckEnabled()).thenReturn(true);
    when(storeAndServerConfigs.getFutureVersionStandbyLagThreshold()).thenReturn(0L);
    when(storeAndServerConfigs.getFutureVersionStandbyLagCheckTimeoutMinutes()).thenReturn(5);
    when(storeAndServerConfigs.getFutureVersionStandbyLagCheckPollIntervalMinutes()).thenReturn(1);
    stubVersionStarted();

    StoreIngestionTask ingestionTask = mock(StoreIngestionTask.class);
    doReturn(ingestionTask).when(storeIngestionService).getStoreIngestionTask(resourceName);
    doReturn(Long.MAX_VALUE).when(ingestionTask).getLocalVersionTopicLag(partition);

    leaderFollowerPartitionStateModel.waitUntilFutureVersionLagAcceptable(resourceName);

    verify(ingestionTask, times(1)).getLocalVersionTopicLag(partition);
  }

  /**
   * When no ingestion task is found for the resource (e.g. torn down concurrently), the wait must fail open and
   * return without throwing.
   */
  @Test
  public void testWaitUntilFutureVersionLagAcceptableFailsOpenWhenNoIngestionTask() {
    when(storeAndServerConfigs.isFutureVersionStandbyLagCheckEnabled()).thenReturn(true);
    doReturn(null).when(storeIngestionService).getStoreIngestionTask(resourceName);

    // Should not throw NPE and should return immediately.
    leaderFollowerPartitionStateModel.waitUntilFutureVersionLagAcceptable(resourceName);
  }

  /**
   * OFFLINE-&gt;STANDBY for a future version whose push is still in progress (neither current version nor an
   * already-ready future version) must route through {@link LeaderFollowerPartitionStateModel#waitUntilFutureVersionLagAcceptable}
   * instead of the full-completion {@code waitConsumptionCompleted} path.
   */
  @Test
  public void testOnBecomeStandbyFromOfflineUsesLagWaitForInProgressFutureVersion() throws InterruptedException {
    Message message = mock(Message.class);
    NotificationContext context = mock(NotificationContext.class);
    when(message.getResourceName()).thenReturn(resourceName);

    Store store = mock(Store.class);
    // getCurrentVersion() defaults to 0 (Mockito primitive default), which is not equal to storeVersion (3),
    // so this replica is not the current version.
    Version mockVersion = mock(Version.class);
    when(mockVersion.getStatus()).thenReturn(VersionStatus.STARTED); // push still in progress, not ready
    when(store.getVersion(storeVersion)).thenReturn(mockVersion);
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());
    // Utils.isFutureVersion() uses getStore() (not getStoreOrThrow()) to resolve the store.
    doReturn(store).when(metadataRepo).getStore(anyString());
    when(storeAndServerConfigs.isFutureVersionStandbyLagCheckEnabled()).thenReturn(true);

    LeaderFollowerPartitionStateModel spyModel = spy(leaderFollowerPartitionStateModel);
    spyModel.onBecomeStandbyFromOffline(message, context);

    verify(spyModel, times(1)).waitUntilFutureVersionLagAcceptable(resourceName);
    verify(notifier, never()).waitConsumptionCompleted(anyString(), anyInt(), anyInt(), any());
  }

  /**
   * OFFLINE-&gt;STANDBY for the current version must continue to use the full-completion
   * {@code waitConsumptionCompleted} path, and must not invoke the new lag-based wait.
   */
  @Test
  public void testOnBecomeStandbyFromOfflineUsesFullWaitForCurrentVersion() throws InterruptedException {
    Message message = mock(Message.class);
    NotificationContext context = mock(NotificationContext.class);
    when(message.getResourceName()).thenReturn(resourceName);

    Store store = mock(Store.class);
    when(store.getCurrentVersion()).thenReturn(storeVersion);
    when(store.getVersion(storeVersion)).thenReturn(mock(Version.class));
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());

    LeaderFollowerPartitionStateModel spyModel = spy(leaderFollowerPartitionStateModel);
    spyModel.onBecomeStandbyFromOffline(message, context);

    verify(spyModel, never()).waitUntilFutureVersionLagAcceptable(anyString());
    verify(notifier, times(1)).waitConsumptionCompleted(eq(resourceName), eq(partition), anyInt(), any());
  }

  /**
   * OFFLINE-&gt;STANDBY for a backup version (older than the current serving version) must not use either wait
   * path: it's neither the current version nor a future version, so it should proceed immediately.
   */
  @Test
  public void testOnBecomeStandbyFromOfflineSkipsBothWaitsForBackupVersion() throws InterruptedException {
    Message message = mock(Message.class);
    NotificationContext context = mock(NotificationContext.class);
    when(message.getResourceName()).thenReturn(resourceName);

    Store store = mock(Store.class);
    // Current version is newer than this replica's version, so this replica is a backup version.
    when(store.getCurrentVersion()).thenReturn(storeVersion + 1);
    when(store.getVersion(storeVersion)).thenReturn(mock(Version.class));
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());
    doReturn(store).when(metadataRepo).getStore(anyString());

    LeaderFollowerPartitionStateModel spyModel = spy(leaderFollowerPartitionStateModel);
    spyModel.onBecomeStandbyFromOffline(message, context);

    verify(spyModel, never()).waitUntilFutureVersionLagAcceptable(anyString());
    verify(notifier, never()).waitConsumptionCompleted(anyString(), anyInt(), anyInt(), any());
  }

}
