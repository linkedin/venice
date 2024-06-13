package com.linkedin.davinci.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
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
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Pair;
import java.time.Duration;
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
    storeAndServerConfigs = mock(VeniceStoreVersionConfig.class);
    notifier = mock(LeaderFollowerIngestionProgressNotifier.class);
    metadataRepo = mock(ReadOnlyStoreRepository.class);
    partitionPushStatusAccessorFuture = mock(CompletableFuture.class);
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
    Store store = mock(Store.class);
    Version version = mock(Version.class);
    when(message.getResourceName()).thenReturn(resourceName);

    LeaderFollowerPartitionStateModel leaderFollowerPartitionStateModelSpy = spy(leaderFollowerPartitionStateModel);

    // test when both store and version are null
    when(metadataRepo.waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong()))
        .thenReturn(Pair.create(null, null));
    leaderFollowerPartitionStateModelSpy.onBecomeLeaderFromStandby(message, context);
    verify(metadataRepo).waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong());
    verify(heartbeatMonitoringService, never()).addLeaderLagMonitor(any(Version.class), anyInt());

    // test when store is not null and version is null
    when(metadataRepo.waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong()))
        .thenReturn(Pair.create(store, null));
    leaderFollowerPartitionStateModelSpy.onBecomeLeaderFromStandby(message, context);
    verify(metadataRepo, times(2)).waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong());
    verify(heartbeatMonitoringService, never()).addLeaderLagMonitor(any(Version.class), anyInt());

    // test both store and version are not null
    when(metadataRepo.waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong()))
        .thenReturn(Pair.create(store, version));
    doNothing().when(leaderFollowerPartitionStateModelSpy).executeStateTransition(any(), any(), any());
    leaderFollowerPartitionStateModelSpy.onBecomeLeaderFromStandby(message, context);
    verify(metadataRepo, times(3)).waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong());
    verify(heartbeatMonitoringService).addLeaderLagMonitor(version, partition);
  }
}
