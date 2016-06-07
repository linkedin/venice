package com.linkedin.venice.helix;

import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.HelixUtils;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.log4j.Logger;


/**
 * State Transition Handler factory to create transition handler for all stores on the current node.
 */
public class VeniceStateModelFactory extends StateModelFactory<StateModel> {

  private static final Logger logger = Logger.getLogger(VeniceStateModelFactory.class.getName());

  private final KafkaConsumerService kafkaConsumerService;
  private final StorageService storageService;
  private final VeniceConfigLoader configService;

  public VeniceStateModelFactory(KafkaConsumerService kafkaConsumerService,
          StorageService storageService,
          VeniceConfigLoader configService) {
    logger.info("Creating VenicePartitionStateTransitionHandlerFactory for Node: "
        + configService.getVeniceServerConfig().getNodeId());
    this.kafkaConsumerService = kafkaConsumerService;
    this.storageService = storageService;
    this.configService = configService;
  }

  /**
   * This method will be invoked only once per partition per session
   * @param  resourceName cluster where state transition is happening
   * @param partitionName for which the State Transition Handler is required.
   * @return VenicePartitionStateModel for the partition.
   */
  @Override
  public VenicePartitionStateModel createNewStateModel(String resourceName, String partitionName) {
    logger.info("Creating VenicePartitionStateTransitionHandler for partition: " + partitionName + " for Store " + resourceName);
    return new VenicePartitionStateModel(kafkaConsumerService, storageService
        , configService.getStoreConfig(HelixUtils.getStoreName(partitionName))
        , HelixUtils.getPartitionId(partitionName));
  }
}
