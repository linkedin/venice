package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.id.PartitionId;


public class VenicePartitionStateTransitionHandlerFactory extends StateTransitionHandlerFactory<VenicePartitionStateModel> {

  private final KafkaConsumerService kafkaConsumerService;
  private final StoreRepository storeRepository;
  private final VeniceStoreConfig storeConfig;

  public VenicePartitionStateTransitionHandlerFactory (KafkaConsumerService kafkaConsumerService,
      StoreRepository storeRepository, VeniceStoreConfig storeConfig) {
    this.kafkaConsumerService = kafkaConsumerService;
    this.storeRepository = storeRepository;
    this.storeConfig = storeConfig;
  }

  /**
   * This method will be invoked only once per partition per session
   * @param partitionId for which the State Transition Handler is required.
   * @return VenicePartitionStateModel for the partition.
   */
  @Override
  public VenicePartitionStateModel createStateTransitionHandler(PartitionId partitionId) {
    return new VenicePartitionStateModel(kafkaConsumerService, storeRepository, storeConfig,
        Integer.valueOf(partitionId.stringify()));
  }
}
