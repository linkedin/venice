package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.VeniceIngestionBackend;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.utils.HelixUtils;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;


/**
 * State Transition Handler factory for handling Leader/Follower resources in the storage node.
 * For Online/Offline resources, please refer to {@link OnlineOfflinePartitionStateModelFactory}
 */

public class LeaderFollowerPartitionStateModelFactory extends AbstractStateModelFactory {
  private LeaderFollowerIngestionProgressNotifier leaderFollowerStateModelNotifier = new LeaderFollowerIngestionProgressNotifier();

  public LeaderFollowerPartitionStateModelFactory(VeniceIngestionBackend ingestionBackend, VeniceConfigLoader configService,
                                                  ExecutorService executorService, ReadOnlyStoreRepository metadataRepo,
                                                  Optional<CompletableFuture<HelixPartitionStatusAccessor>> partitionPushStatusAccessorFuture,
                                                  String instanceName
  ) {
    super(ingestionBackend, configService, executorService, metadataRepo,
        partitionPushStatusAccessorFuture, instanceName);

    // Add a new notifier to let state model knows ingestion has caught up the lag so that it can complete the offline to
    // standby state transition.
    ingestionBackend.addLeaderFollowerIngestionNotifier(leaderFollowerStateModelNotifier);
    logger.info("LeaderFollowerParticipantModelFactory Created");
  }

  @Override
  public LeaderFollowerPartitionStateModel createNewStateModel(String resourceName, String partitionName) {
    logger.info("Creating LeaderFollowerParticipantModel handler for partition: " + partitionName);
    return new LeaderFollowerPartitionStateModel(getIngestionBackend(),
        getConfigService().getStoreConfig(HelixUtils.getResourceName(partitionName)),
        HelixUtils.getPartitionId(partitionName), leaderFollowerStateModelNotifier, getStoreMetadataRepo(),
        partitionPushStatusAccessorFuture, instanceName);
  }
}
