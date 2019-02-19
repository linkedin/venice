package com.linkedin.venice.helix;

import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.HelixUtils;
import java.util.concurrent.ExecutorService;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.log4j.Logger;


public class LeaderFollowerParticipantModelFactory extends AbstractParticipantModelFactory {
  public LeaderFollowerParticipantModelFactory(StoreIngestionService storeIngestionService,
      StorageService storageService,
      VeniceConfigLoader configService,
      ExecutorService executorService ) {
    super(storeIngestionService, storageService, configService, executorService);
    logger.info("LeaderFollowerParticipantModelFactory Created");
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    logger.info("Creating LeaderFollowerParticipantModel handler for partition: " + partitionName);
    return new LeaderFollowerParticipantModel(getStoreIngestionService(), getStorageService()
        , getConfigService().getStoreConfig(HelixUtils.getResourceName(partitionName))
        , HelixUtils.getPartitionId(partitionName));
  }
}