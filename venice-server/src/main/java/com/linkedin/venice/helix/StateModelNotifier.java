package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.stats.AggStoreIngestionStats;
import com.linkedin.venice.stats.AggVersionedStorageIngestionStats;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

import static com.linkedin.venice.helix.AbstractParticipantModelFactory.getStateModelIdentification;


/**
 * This class notify the Helix State Models (SM) about corresponding ingestion progress.
 * The class also holds latches that can be used in SM in the cases when state transitions
 * need to coordinate with ingestion progress.
 */
public class StateModelNotifier implements VeniceNotifier {
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
   //   AggStoreIngestionStats storeIngestionStats, AggVersionedStorageIngestionStats versionedStorageIngestionStats)
   //   throws InterruptedException {
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
      // If consumption is failed, throw an exception here, Helix will put this replcia to ERROR state.
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
    if (getLatch(resourceName, partitionId) != null) {
      stateModelToSuccessMap.put(getStateModelIdentification(resourceName, partitionId), true);
      getLatch(resourceName, partitionId).countDown();
    } else {
      logger.info("No latch is found for resource:" + resourceName + " partition:" + partitionId);
    }
  }

  @Override
  public void error(String resourceName, int partitionId, String message, Exception ex) {
    if (getLatch(resourceName, partitionId) != null) {
      getLatch(resourceName, partitionId).countDown();
    } else {
      logger.info("No latch is found for resource:" + resourceName + " partition:" + partitionId);
    }
  }
}
