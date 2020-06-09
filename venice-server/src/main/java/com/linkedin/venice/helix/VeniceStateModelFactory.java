package com.linkedin.venice.helix;

import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.HelixUtils;
import java.util.concurrent.ExecutorService;


/**
 * State Transition Handler factory for handling Online/Offline resources in the storage node.
 * For leader/Follower resources, please refer to {@link LeaderFollowerParticipantModelFactory}
 */
public class VeniceStateModelFactory extends AbstractParticipantModelFactory {
  private final StateModelNotifier stateModelNotifier = new StateModelNotifier();

  public VeniceStateModelFactory(StoreIngestionService storeIngestionService,
          StorageService storageService,
          VeniceConfigLoader configService,
          ExecutorService executorService,
          ReadOnlyStoreRepository readOnlyStoreRepository) {
    super(storeIngestionService, storageService, configService, executorService, readOnlyStoreRepository);

    // Add a new notifier to let state model knows the end of consumption so that it can complete the bootstrap to
    // online state transition.
    storeIngestionService.addNotifier(stateModelNotifier);
    logger.info("VenicePartitionStateTransitionHandlerFactory created");
  }

  @Override
  public VenicePartitionStateModel createNewStateModel(String resourceName, String partitionName) {
    logger.info("Creating VenicePartitionStateTransitionHandler for partition: " + partitionName);
    return new VenicePartitionStateModel(getStoreIngestionService(), getStorageService(),
        getConfigService().getStoreConfig(HelixUtils.getResourceName(partitionName)),
        HelixUtils.getPartitionId(partitionName), stateModelNotifier, getMetadataRepo());
  }

  StateModelNotifier getNotifier() {
    return this.stateModelNotifier;
  }
}
