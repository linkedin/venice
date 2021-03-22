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
 * For leader/Follower resources, please refer to {@link LeaderFollowerParticipantModelFactory}
 */
public class VeniceStateModelFactory extends AbstractParticipantModelFactory {
  private final OnlineOfflineStateModelNotifier onlineOfflineStateModelNotifier = new OnlineOfflineStateModelNotifier();

  public VeniceStateModelFactory(VeniceIngestionBackend ingestionBackend,
      VeniceConfigLoader configService, ExecutorService executorService,
      ReadOnlyStoreRepository readOnlyStoreRepository,
      Optional<CompletableFuture<HelixPartitionStatusAccessor>> partitionPushStatusAccessorFuture, String instanceName) {
    super(ingestionBackend, configService, executorService, readOnlyStoreRepository,
        partitionPushStatusAccessorFuture, instanceName);

    // Add a new notifier to let state model knows the end of consumption so that it can complete the bootstrap to
    // online state transition.
    ingestionBackend.addOnlineOfflineIngestionNotifier(onlineOfflineStateModelNotifier);
    logger.info("VenicePartitionStateTransitionHandlerFactory created");
  }

  @Override
  public VenicePartitionStateModel createNewStateModel(String resourceName, String partitionName) {
    logger.info("Creating VenicePartitionStateTransitionHandler for partition: " + partitionName);
    return new VenicePartitionStateModel(getIngestionBackend(),
        getConfigService().getStoreConfig(HelixUtils.getResourceName(partitionName)),
        HelixUtils.getPartitionId(partitionName), onlineOfflineStateModelNotifier, getMetadataRepo(),
        partitionPushStatusAccessorFuture, instanceName);
  }

  OnlineOfflineStateModelNotifier getNotifier() {
    return this.onlineOfflineStateModelNotifier;
  }
}
