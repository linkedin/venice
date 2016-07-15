package com.linkedin.venice.helix;

import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.HelixUtils;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.log4j.Logger;


/**
 * State Transition Handler factory to create transition handler for all stores on the current node.
 */
public class VeniceStateModelFactory extends StateModelFactory<StateModel> {

  private static final Logger logger = Logger.getLogger(VeniceStateModelFactory.class);

  private final KafkaConsumerService kafkaConsumerService;
  private final StorageService storageService;
  private final VeniceConfigLoader configService;
  private final StateModelNotifier stateModelNotifer = new StateModelNotifier();

  public VeniceStateModelFactory(KafkaConsumerService kafkaConsumerService,
          StorageService storageService,
          VeniceConfigLoader configService) {
    logger.info("Creating VenicePartitionStateTransitionHandlerFactory for Node: "
        + configService.getVeniceServerConfig().getNodeId());
    this.kafkaConsumerService = kafkaConsumerService;
    this.storageService = storageService;
    this.configService = configService;
    // Add a new notifier to let state model knows the end of consumption so that it can complete the bootstrap to
    // online state transition.
    kafkaConsumerService.addNotifier(stateModelNotifer);
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
        , HelixUtils.getPartitionId(partitionName), stateModelNotifer);
  }

  /**
   * Notifier used to get completed notification from consumer service and let state model knows the end of consumption
   */
  public static class StateModelNotifier implements VeniceNotifier {
    private ConcurrentMap<String, CountDownLatch> stateModelToLatchMap = new ConcurrentHashMap<>();

    /**
     * Create a latch to wait on.
     * @param resourceName
     * @param partitionId
     */
    void startConsumption(String resourceName, int partitionId) {
      CountDownLatch latch = new CountDownLatch(1);
      stateModelToLatchMap.put(resourceName + "_" + partitionId, latch);
    }

    /**
     * Wait on the existing latch until consumption is completed.
     * @param resourceName
     * @param partitionId
     * @throws InterruptedException
     */
    void waitConsumptionCompleted(String resourceName, int partitionId)
        throws InterruptedException {
      CountDownLatch latch = stateModelToLatchMap.get(resourceName + "_" + partitionId);
      if (latch == null) {
        logger.error("No latch is found for resource:" + resourceName + " partition:" + partitionId);
      } else {
        // TODO timeout
        latch.await();
        stateModelToLatchMap.remove(resourceName + "_" + partitionId);
      }
    }

    CountDownLatch getLatch(String resourceName, int partitionId) {
      return stateModelToLatchMap.get(resourceName + "_" + partitionId);
    }

    void removeLatch(String resourcename, int partitionId) {
      stateModelToLatchMap.remove(resourcename + "_" + partitionId);
    }

    @Override
    public void started(String resourceName, int partitionId) {
    }

    /**
     * Count down the latch once consumption is completed.
     * @param resourceName
     * @param partitionId partitionId
     * @param offset
     */
    @Override
    public void completed(String resourceName, int partitionId, long offset) {
      CountDownLatch latch = stateModelToLatchMap.get(resourceName + "_" + partitionId);
      if (latch == null) {
        logger.error("No latch is found for resource:" + resourceName + " partition:" + partitionId);
      } else {
        latch.countDown();
      }
    }

    @Override
    public void progress(String resourceName, int partitionId, long offset) {
    }

    @Override
    public void close() {
    }

    @Override
    public void error(String resourceName, int partitionId, String message, Exception ex) {
    }
  }
}
