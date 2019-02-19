package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.HelixUtils;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;


/**
 * State Transition Handler factory to create transition handler for all stores on the current node.
 */
public class VeniceStateModelFactory extends AbstractParticipantModelFactory {
  private final StateModelNotifier stateModelNotifier = new StateModelNotifier();

  private final ReadOnlyStoreRepository readOnlyStoreRepository;
  // TODO We should use the same value as Helix used for state transition timeout.

  public VeniceStateModelFactory(StoreIngestionService storeIngestionService,
          StorageService storageService,
          VeniceConfigLoader configService,
          ExecutorService executorService,
          ReadOnlyStoreRepository readOnlyStoreRepository) {
    super(storeIngestionService, storageService, configService, executorService);
    this.readOnlyStoreRepository = readOnlyStoreRepository;

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
        HelixUtils.getPartitionId(partitionName), stateModelNotifier, readOnlyStoreRepository);
  }

  StateModelNotifier getNotifier() {
    return this.stateModelNotifier;
  }
  /**
   * Notifier used to get completed notification from consumer service and let state model knows the end of consumption
   */
  public static class StateModelNotifier implements VeniceNotifier {
    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());
    private ConcurrentMap<String, CountDownLatch> stateModelToLatchMap = new ConcurrentHashMap<>();

    private ConcurrentMap<String, Boolean> stateModelToSuccessMap = new ConcurrentHashMap<>();

    /**
     * Create a latch to wait on.
     * @param resourceName
     * @param partitionId
     */
    void startConsumption(String resourceName, int partitionId) {
      CountDownLatch latch = new CountDownLatch(1);
      stateModelToLatchMap.put(getStateModelIdentification(resourceName, partitionId), latch);
      stateModelToSuccessMap.put(getStateModelIdentification(resourceName, partitionId), false);
    }

    /**
     * Wait on the existing latch until consumption is completed.
     * @param resourceName
     * @param partitionId
     * @throws InterruptedException
     */
    void waitConsumptionCompleted(String resourceName, int partitionId, int bootstrapToOnlineTimeoutInHours)
        throws InterruptedException {
      String stateModeId = getStateModelIdentification(resourceName , partitionId);
      CountDownLatch latch = stateModelToLatchMap.get(stateModeId);
      if (latch == null) {
        String errorMsg = "No latch is found for resource:" + resourceName + " partition:" + partitionId;
        logger.error(errorMsg);
        throw new VeniceException(errorMsg);
      } else {
        if(!latch.await(bootstrapToOnlineTimeoutInHours, TimeUnit.HOURS)){
          // Timeout
          String errorMsg =
              "After waiting " + bootstrapToOnlineTimeoutInHours + " hours, resource:" + resourceName + " partition:"
                  + partitionId + " still can not become online from bootstrap.";
          logger.error(errorMsg);
          throw new VeniceException(errorMsg);
        }
        stateModelToLatchMap.remove(stateModeId);
        // If consumption is failed, throw an exception here, Helix will put this replcia to ERROR state.
        if (!stateModelToSuccessMap.remove(stateModeId)) {
          throw new VeniceException(
              "Consumption is failed. Thrown an exception to put this replica:" + stateModeId + " to ERROR state.");
        }
      }
    }

    CountDownLatch getLatch(String resourceName, int partitionId) {
      return stateModelToLatchMap.get(getStateModelIdentification(resourceName, partitionId));
    }

    void removeLatch(String resourceName, int partitionId) {
      stateModelToLatchMap.remove(getStateModelIdentification(resourceName, partitionId))  ;
    }

    private void countDownTheLatch(String resourceName, int partitionId){
      CountDownLatch latch = getLatch(resourceName, partitionId);
      if (latch == null) {
        logger.error("No latch is found for resource:" + resourceName + " partition:" + partitionId);
      } else {
        latch.countDown();
      }
    }

    @Override
    public void started(String resourceName, int partitionId, String message) {}

    @Override
    public void restarted(String storeName, int partitionId, long offset, String message) {}

    /**
     * Count down the latch once consumption is completed.
     * @param resourceName
     * @param partitionId partitionId
     * @param offset
     */
    @Override
    public void completed(String resourceName, int partitionId, long offset, String message) {
      stateModelToSuccessMap.put(getStateModelIdentification(resourceName, partitionId), true);
      countDownTheLatch(resourceName, partitionId);
    }

    @Override
    public void progress(String resourceName, int partitionId, long offset, String message) {}

    @Override
    public void endOfPushReceived(String storeName, int partitionId, long offset, String message) {}

    @Override
    public void startOfBufferReplayReceived(String storeName, int partitionId, long offset, String message) {}

    @Override
    public void startOfIncrementalPushReceived(String storeName, int partitionId, long offset, String message) {}

    @Override
    public void endOfIncrementalPushReceived(String storeName, int partitionId, long offset, String message) {}

    @Override
    public void close() {}

    @Override
    public void error(String resourceName, int partitionId, String message, Exception ex) {
      countDownTheLatch(resourceName,partitionId);
    }

    private static String getStateModelIdentification(String resourceName, int partitionId) {
      return resourceName + "_" + partitionId;
    }
  }
}
