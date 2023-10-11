package com.linkedin.davinci.helix;

import static com.linkedin.davinci.helix.AbstractStateModelFactory.getStateModelID;

import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class notifies the Helix State Models (SM) about corresponding ingestion progress.
 * The class also holds latches that can be used in SM in the cases when state transitions
 * need to coordinate with ingestion progress.
 */
public abstract class StateModelIngestionProgressNotifier implements VeniceNotifier {
  private final Logger logger = LogManager.getLogger(this.getClass());
  private final Map<String, CountDownLatch> stateModelToIngestionCompleteFlagMap = new VeniceConcurrentHashMap<>();
  private final Map<String, Boolean> stateModelToSuccessMap = new VeniceConcurrentHashMap<>();

  void startConsumption(String resourceName, int partitionId) {
    final String stateModelID = getStateModelID(resourceName, partitionId);
    stateModelToIngestionCompleteFlagMap.put(stateModelID, new CountDownLatch(1));
    stateModelToSuccessMap.put(stateModelID, false);
  }

  void waitConsumptionCompleted(
      String resourceName,
      int partitionId,
      int bootstrapToOnlineTimeoutInHours,
      StoreIngestionService storeIngestionService) throws InterruptedException {
    String stateModelId = getStateModelID(resourceName, partitionId);
    CountDownLatch ingestionCompleteFlag = stateModelToIngestionCompleteFlagMap.get(stateModelId);
    if (ingestionCompleteFlag == null) {
      String errorMsg =
          "No ingestion complete flag is found for resource:" + resourceName + " partition:" + partitionId;
      logger.error(errorMsg);
      throw new VeniceException(errorMsg);
    } else {
      if (!ingestionCompleteFlag.await(bootstrapToOnlineTimeoutInHours, TimeUnit.HOURS)) {
        // Timeout
        String errorMsg = "After waiting " + bootstrapToOnlineTimeoutInHours + " hours, resource:" + resourceName
            + " partition:" + partitionId + " still can not become online from bootstrap.";
        logger.error(errorMsg);
        // Report ingestion_failure
        String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
        storeIngestionService.recordIngestionFailure(storeName);
        VeniceException veniceException = new VeniceException(errorMsg);
        storeIngestionService.getStoreIngestionTask(resourceName).reportError(errorMsg, partitionId, veniceException);
      }
      stateModelToIngestionCompleteFlagMap.remove(stateModelId);
      // If consumption is failed, throw an exception here, Helix will put this replica to ERROR state.
      if (stateModelToSuccessMap.containsKey(stateModelId) && !stateModelToSuccessMap.get(stateModelId)) {
        throw new VeniceException(
            "Consumption is failed. Thrown an exception to put this replica:" + stateModelId + " to ERROR state.");
      }
    }
  }

  void stopConsumption(String resourceName, int partitionId) {
    final String stateModelID = getStateModelID(resourceName, partitionId);
    stateModelToIngestionCompleteFlagMap.remove(stateModelID);
    stateModelToSuccessMap.remove(stateModelID);
  }

  CountDownLatch getIngestionCompleteFlag(String resourceName, int partitionId) {
    return stateModelToIngestionCompleteFlagMap.get(getStateModelID(resourceName, partitionId));
  }

  void removeIngestionCompleteFlag(String resourceName, int partitionId) {
    stateModelToIngestionCompleteFlagMap.remove(getStateModelID(resourceName, partitionId));
  }

  @Override
  public void completed(String resourceName, int partitionId, long offset, String message) {
    CountDownLatch ingestionCompleteFlag = getIngestionCompleteFlag(resourceName, partitionId);
    if (ingestionCompleteFlag != null) {
      stateModelToSuccessMap.put(getStateModelID(resourceName, partitionId), true);
      ingestionCompleteFlag.countDown();
    } else {
      logger.info("No ingestion complete flag is found for resource: {} partition: {}", resourceName, partitionId);
    }
  }

  @Override
  public void error(String resourceName, int partitionId, String message, Exception ex) {
    CountDownLatch ingestionCompleteFlag = getIngestionCompleteFlag(resourceName, partitionId);
    if (ingestionCompleteFlag != null) {
      ingestionCompleteFlag.countDown();
    } else {
      logger.info("No ingestion complete flag is found for resource: {} partition: {}", resourceName, partitionId);
    }
  }

  @Override
  public void stopped(String resourceName, int partitionId, long offset) {
    /**
     * Must remove the state model from the model-to-success map first before releasing the latch;
     * otherwise, error will happen in {@link #waitConsumptionCompleted} if latch is released but
     * consumption is not completed.
     */
    stateModelToSuccessMap.remove(getStateModelID(resourceName, partitionId));
    CountDownLatch ingestionCompleteFlag = getIngestionCompleteFlag(resourceName, partitionId);
    if (ingestionCompleteFlag != null) {
      ingestionCompleteFlag.countDown();
    } else {
      logger.info("No ingestion complete flag is found for resource: {} partition: {}", resourceName, partitionId);
    }
  }
}
