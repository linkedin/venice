package com.linkedin.venice.helix;

import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.utils.HelixUtils;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.id.PartitionId;
import org.apache.log4j.Logger;


/**
 * State Transition Handler factory to create transition handler for all stores on the current node.
 */
public class VenicePartitionStateTransitionHandlerFactory extends StateTransitionHandlerFactory<VenicePartitionStateModel> {

  private static final Logger logger = Logger.getLogger(VenicePartitionStateTransitionHandlerFactory.class.getName());

  private final KafkaConsumerService kafkaConsumerService;
  private final StoreRepository storeRepository;
  private final VeniceConfigService configService;

  public VenicePartitionStateTransitionHandlerFactory (KafkaConsumerService kafkaConsumerService,
      StoreRepository storeRepository, VeniceConfigService configService) {
    logger.info("Creating VenicePartitionStateTransitionHandlerFactory for Node: "
        + configService.getVeniceServerConfig().getNodeId());
    this.kafkaConsumerService = kafkaConsumerService;
    this.storeRepository = storeRepository;
    this.configService = configService;
  }

  /**
   * This method will be invoked only once per partition per session
   * @param partitionId for which the State Transition Handler is required.
   * @return VenicePartitionStateModel for the partition.
   */
  @Override
  public VenicePartitionStateModel createStateTransitionHandler(PartitionId partitionId) {
    logger.info("Creating VenicePartitionStateTransitionHandler for partition: " + partitionId.stringify());
    return new VenicePartitionStateModel(kafkaConsumerService, storeRepository
        , configService.getStoreConfig(HelixUtils.getVeniceStoreNameFromHelixPartitionId(partitionId))
        , HelixUtils.getVenicePartitionIdFromHelixPartitionId(partitionId));
  }
}
