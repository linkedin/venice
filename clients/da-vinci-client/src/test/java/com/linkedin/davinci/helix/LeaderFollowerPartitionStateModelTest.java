package com.linkedin.davinci.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.IngestionBackend;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.stats.ParticipantStateTransitionStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatLagMonitorAction;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VersionStatus;
import java.util.concurrent.CompletableFuture;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LeaderFollowerPartitionStateModelTest {
  private IngestionBackend ingestionBackend;
  private KafkaStoreIngestionService storeIngestionService;
  private VeniceStoreVersionConfig storeAndServerConfigs;
  private LeaderFollowerIngestionProgressNotifier notifier;
  private ReadOnlyStoreRepository metadataRepo;
  private CompletableFuture<HelixPartitionStatusAccessor> partitionPushStatusAccessorFuture;
  private ParticipantStateTransitionStats threadPoolStats;
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
    doReturn(CompletableFuture.completedFuture(null)).when(ingestionBackend).stopConsumption(any(), anyInt());

    storeAndServerConfigs = mock(VeniceStoreVersionConfig.class);
    notifier = mock(LeaderFollowerIngestionProgressNotifier.class);
    metadataRepo = mock(ReadOnlyStoreRepository.class);
    partitionPushStatusAccessorFuture = CompletableFuture.completedFuture(mock(HelixPartitionStatusAccessor.class));
    threadPoolStats = mock(ParticipantStateTransitionStats.class);
    heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    leaderFollowerPartitionStateModel = new LeaderFollowerPartitionStateModel(
        ingestionBackend,
        storeAndServerConfigs,
        partition,
        notifier,
        metadataRepo,
        partitionPushStatusAccessorFuture,
        "instanceName",
        threadPoolStats,
        heartbeatMonitoringService);
  }

  @Test
  public void testUpdateLagMonitor() {
    Message message = mock(Message.class);
    NotificationContext context = mock(NotificationContext.class);
    when(message.getResourceName()).thenReturn(resourceName);
    Store store = mock(Store.class);
    when(store.getVersionStatus(anyInt())).thenReturn(VersionStatus.STARTED);
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());

    LeaderFollowerPartitionStateModel leaderFollowerPartitionStateModelSpy = spy(leaderFollowerPartitionStateModel);

    // STANDBY->LEADER
    leaderFollowerPartitionStateModelSpy.onBecomeLeaderFromStandby(message, context);
    verify(heartbeatMonitoringService, never())
        .updateLagMonitor(eq(resourceName), eq(partition), eq(HeartbeatLagMonitorAction.SET_LEADER_MONITOR));

    // LEADER->STANDBY
    leaderFollowerPartitionStateModelSpy.onBecomeStandbyFromLeader(message, context);
    verify(heartbeatMonitoringService, never())
        .updateLagMonitor(eq(resourceName), eq(partition), eq(HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR));

    // OFFLINE->STANDBY
    leaderFollowerPartitionStateModelSpy.onBecomeStandbyFromOffline(message, context);
    verify(heartbeatMonitoringService, times(1))
        .updateLagMonitor(eq(resourceName), eq(partition), eq(HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR));

    // STANDBY->OFFLINE
    leaderFollowerPartitionStateModelSpy.onBecomeOfflineFromStandby(message, context);
    verify(heartbeatMonitoringService)
        .updateLagMonitor(eq(resourceName), eq(partition), eq(HeartbeatLagMonitorAction.REMOVE_MONITOR));
  }
}
