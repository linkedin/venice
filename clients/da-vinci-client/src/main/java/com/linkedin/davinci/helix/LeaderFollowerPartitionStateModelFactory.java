package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.VeniceIngestionBackend;
import com.linkedin.davinci.stats.ParticipantStateTransitionStats;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.utils.HelixUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;


/**
 * State Transition Handler factory for handling Leader/Follower resources in the storage node.
 */

public class LeaderFollowerPartitionStateModelFactory extends AbstractStateModelFactory {
  private final LeaderFollowerIngestionProgressNotifier leaderFollowerStateModelNotifier =
      new LeaderFollowerIngestionProgressNotifier();

  public LeaderFollowerPartitionStateModelFactory(
      VeniceIngestionBackend ingestionBackend,
      VeniceConfigLoader configService,
      ExecutorService executorService,
      ParticipantStateTransitionStats stateTransitionStats,
      ReadOnlyStoreRepository metadataRepo,
      CompletableFuture<HelixPartitionStatusAccessor> partitionPushStatusAccessorFuture,
      String instanceName) {
    super(
        ingestionBackend,
        configService,
        executorService,
        stateTransitionStats,
        metadataRepo,
        partitionPushStatusAccessorFuture,
        instanceName);

    // Add a new notifier to let state model knows ingestion has caught up the lag so that it can complete the offline
    // to standby state transition.
    ingestionBackend.addIngestionNotifier(leaderFollowerStateModelNotifier);
    logger.info("LeaderFollowerParticipantModelFactory Created");
  }

  @Override
  public LeaderFollowerPartitionStateModel createNewStateModel(String resourceName, String partitionName) {
    logger.info("Creating LeaderFollowerParticipantModel handler for partition: {}", partitionName);
    return new LeaderFollowerPartitionStateModel(
        getIngestionBackend(),
        getConfigService().getStoreConfig(HelixUtils.getResourceName(partitionName)),
        HelixUtils.getPartitionId(partitionName),
        leaderFollowerStateModelNotifier,
        getStoreMetadataRepo(),
        partitionPushStatusAccessorFuture,
        instanceName,
        getStateTransitionStats(resourceName));
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
