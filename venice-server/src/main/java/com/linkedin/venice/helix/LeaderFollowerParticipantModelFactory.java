package com.linkedin.venice.helix;

import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.HelixUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.helix.participant.statemachine.StateModel;


/**
 * State Transition Handler factory for handling Leader/Follower resources in the storage node.
 * For Online/Offline resources, please refer to {@link VeniceStateModelFactory}
 */

public class LeaderFollowerParticipantModelFactory extends AbstractParticipantModelFactory {
  private LeaderFollowerStateModelNotifier notifier = new LeaderFollowerStateModelNotifier();

  public LeaderFollowerParticipantModelFactory(StoreIngestionService storeIngestionService,
      StorageService storageService, VeniceConfigLoader configService, ExecutorService executorService,
      ReadOnlyStoreRepository metadataRepo,
      CompletableFuture<HelixPartitionPushStatusAccessor> partitionPushStatusAccessorFuture,
      String instanceName) {
    super(storeIngestionService, storageService, configService, executorService, metadataRepo,
        partitionPushStatusAccessorFuture, instanceName);
    logger.info("LeaderFollowerParticipantModelFactory Created");
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    logger.info("Creating LeaderFollowerParticipantModel handler for partition: " + partitionName);
    return new LeaderFollowerParticipantModel(getStoreIngestionService(), getStorageService(),
        getConfigService().getStoreConfig(HelixUtils.getResourceName(partitionName)),
        HelixUtils.getPartitionId(partitionName), notifier, getMetadataRepo(), partitionPushStatusAccessorFuture,
        instanceName);
  }
}