package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.IngestionBackend;
import com.linkedin.davinci.stats.ParticipantStateTransitionStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;


/**
 * State Transition Handler factory for handling Leader/Follower resources in the storage node.
 */

public class LeaderFollowerPartitionStateModelFactory extends AbstractStateModelFactory {
  private final LeaderFollowerIngestionProgressNotifier leaderFollowerStateModelNotifier =
      new LeaderFollowerIngestionProgressNotifier();
  private final HeartbeatMonitoringService heartbeatMonitoringService;

  public LeaderFollowerPartitionStateModelFactory(
      IngestionBackend ingestionBackend,
      VeniceConfigLoader configService,
      ExecutorService executorService,
      ParticipantStateTransitionStats stateTransitionStats,
      ReadOnlyStoreRepository metadataRepo,
      CompletableFuture<HelixPartitionStatusAccessor> partitionPushStatusAccessorFuture,
      String instanceName,
      HeartbeatMonitoringService heartbeatMonitoringService) {
    super(
        ingestionBackend,
        configService,
        executorService,
        stateTransitionStats,
        metadataRepo,
        partitionPushStatusAccessorFuture,
        instanceName);
    this.heartbeatMonitoringService = heartbeatMonitoringService;

    // Add a new notifier to let state model knows ingestion has caught up the lag so that it can complete the offline
    // to standby state transition.
    ingestionBackend.addIngestionNotifier(leaderFollowerStateModelNotifier);
    logger.info("LeaderFollowerParticipantModelFactory Created");
  }

  @Override
  public LeaderFollowerPartitionStateModel createNewStateModel(String resourceName, String partitionName) {
    // TODO: we should cache config object and reuse it for all the partitions of the same store.
    VeniceStoreVersionConfig storeVersionConfig =
        getConfigService().getStoreConfig(HelixUtils.getResourceName(partitionName));
    int partitionId = HelixUtils.getPartitionId(partitionName);
    LogContext.setLogContext(storeVersionConfig.getLogContext());
    logger.info(
        "Creating LeaderFollowerParticipantModel handler for partition: {}",
        Utils.getReplicaId(resourceName, partitionId));
    return new LeaderFollowerPartitionStateModel(
        getIngestionBackend(),
        storeVersionConfig,
        partitionId,
        leaderFollowerStateModelNotifier,
        getStoreMetadataRepo(),
        partitionPushStatusAccessorFuture,
        instanceName,
        getStateTransitionStats(resourceName),
        heartbeatMonitoringService);
  }

  /**
   * The leader follower state thread pool strategy specifies how thread pools are allocated for Helix state transition.
   * Today, two strategies are supported:
   * 1. single pool strategy - a single thread pool for all store version Helix state transition.
   * 2. dual pool strategy - system has two thread pools, one for current and backup versions and a second one for
   *    future versions.
   */
  public enum LeaderFollowerThreadPoolStrategy {
    SINGLE_POOL_STRATEGY, DUAL_POOL_STRATEGY
  }
}
