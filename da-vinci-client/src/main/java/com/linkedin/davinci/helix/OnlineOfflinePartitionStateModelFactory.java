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
 * State Transition Handler factory for handling Online/Offline resources in the storage node.
 * For leader/Follower resources, please refer to {@link LeaderFollowerPartitionStateModelFactory}
 */
public class OnlineOfflinePartitionStateModelFactory extends AbstractStateModelFactory {
  private final OnlineOfflineIngestionProgressNotifier onlineOfflineStateModelNotifier = new OnlineOfflineIngestionProgressNotifier();

  public OnlineOfflinePartitionStateModelFactory(VeniceIngestionBackend ingestionBackend,
                                                 VeniceConfigLoader configService, ExecutorService executorService,
                                                 ReadOnlyStoreRepository readOnlyStoreRepository,
                                                 Optional<CompletableFuture<HelixPartitionStatusAccessor>> partitionPushStatusAccessorFuture,
                                                 String instanceName
  ) {
    super(ingestionBackend, configService, executorService, readOnlyStoreRepository,
        partitionPushStatusAccessorFuture, instanceName);

    // Add a new notifier to let state model knows the end of consumption so that it can complete the bootstrap to
    // online state transition.
    ingestionBackend.addOnlineOfflineIngestionNotifier(onlineOfflineStateModelNotifier);
    logger.info(getClass() + " created");
  }

  @Override
  public OnlineOfflinePartitionStateModel createNewStateModel(String resourceName, String partitionName) {
    logger.info("Creating VenicePartitionStateTransitionHandler for partition: " + partitionName);
    return new OnlineOfflinePartitionStateModel(getIngestionBackend(),
        getConfigService().getStoreConfig(HelixUtils.getResourceName(partitionName)),
        HelixUtils.getPartitionId(partitionName), onlineOfflineStateModelNotifier, getStoreMetadataRepo(),
        partitionPushStatusAccessorFuture, instanceName);
  }

  OnlineOfflineIngestionProgressNotifier getNotifier() {
    return this.onlineOfflineStateModelNotifier;
  }
}
