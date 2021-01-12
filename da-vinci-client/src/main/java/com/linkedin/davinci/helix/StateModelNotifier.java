package com.linkedin.davinci.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.Version;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

import static com.linkedin.davinci.helix.AbstractParticipantModelFactory.getStateModelIdentification;


/**
 * This class notify the Helix State Models (SM) about corresponding ingestion progress.
 * The class also holds latches that can be used in SM in the cases when state transitions
 * need to coordinate with ingestion progress.
 */
public abstract class StateModelNotifier implements VeniceNotifier {
  private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());
  private Map<String, CountDownLatch> stateModelToLatchMap = new VeniceConcurrentHashMap<>();
  private Map<String, Boolean> stateModelToSuccessMap = new VeniceConcurrentHashMap<>();

  void startConsumption(String resourceName, int partitionId) {
    CountDownLatch latch = new CountDownLatch(1);
    stateModelToLatchMap.put(getStateModelIdentification(resourceName, partitionId), latch);
    stateModelToSuccessMap.put(getStateModelIdentification(resourceName, partitionId), false);
  }

  void waitConsumptionCompleted(String resourceName, int partitionId, int bootstrapToOnlineTimeoutInHours,
      StoreIngestionService storeIngestionService) throws InterruptedException {
    String stateModelId = getStateModelIdentification(resourceName , partitionId);
    CountDownLatch latch = stateModelToLatchMap.get(stateModelId);
    if (latch == null) {
      String errorMsg = "No latch is found for resource:" + resourceName + " partition:" + partitionId;
      logger.error(errorMsg);
      throw new VeniceException(errorMsg);
    } else {
      if (!latch.await(bootstrapToOnlineTimeoutInHours, TimeUnit.HOURS)) {
        // Timeout
        String errorMsg =
            "After waiting " + bootstrapToOnlineTimeoutInHours + " hours, resource:" + resourceName + " partition:"
                + partitionId + " still can not become online from bootstrap.";
        logger.error(errorMsg);
        // Report ingestion_failure and ingestion_task_errored_gauge
        String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
        int versionNumber = Version.parseVersionFromKafkaTopicName(resourceName);
        storeIngestionService.getAggStoreIngestionStats().recordIngestionFailure(storeName);
        storeIngestionService.getAggVersionedStorageIngestionStats().setIngestionTaskErroredGauge(storeName, versionNumber);
        VeniceException veniceException =  new VeniceException(errorMsg);
        storeIngestionService.getStoreIngestionTask(resourceName).reportError(errorMsg, partitionId, veniceException);
      }
      stateModelToLatchMap.remove(stateModelId);
      // If consumption is failed, throw an exception here, Helix will put this replica to ERROR state.
      if (stateModelToSuccessMap.containsKey(stateModelId) && !stateModelToSuccessMap.get(stateModelId)) {
        throw new VeniceException("Consumption is failed. Thrown an exception to put this replica:" +
            stateModelId + " to ERROR state.");
      }
    }
  }

  CountDownLatch getLatch(String resourceName, int partitionId) {
    return stateModelToLatchMap.get(getStateModelIdentification(resourceName, partitionId));
  }

  void removeLatch(String resourceName, int partitionId) {
    stateModelToLatchMap.remove(getStateModelIdentification(resourceName, partitionId))  ;
  }

  @Override
  public void completed(String resourceName, int partitionId, long offset, String message) {
    CountDownLatch latch = getLatch(resourceName, partitionId);
    if (latch != null) {
      stateModelToSuccessMap.put(getStateModelIdentification(resourceName, partitionId), true);
      latch.countDown();
    } else {
      logger.info("No latch is found for resource:" + resourceName + " partition:" + partitionId);
    }
  }

  @Override
  public void error(String resourceName, int partitionId, String message, Exception ex) {
    CountDownLatch latch = getLatch(resourceName, partitionId);
    if (latch != null) {
      latch.countDown();
    } else {
      logger.info("No latch is found for resource:" + resourceName + " partition:" + partitionId);
    }
  }

  @Override
  public void stopped(String resourceName, int partitionId, long offset) {
    /**
     * Must remove the state model from the model-to-success map first before releasing the latch;
     * otherwise, error will happen in {@link #waitConsumptionCompleted} if latch is released but
     * consumption is not completed.
     */
    stateModelToSuccessMap.remove(getStateModelIdentification(resourceName, partitionId));
    CountDownLatch latch = getLatch(resourceName, partitionId);
    if (latch != null) {
      latch.countDown();
    } else {
      logger.info("No latch is found for resource:" + resourceName + " partition:" + partitionId);
    }
  }
}
