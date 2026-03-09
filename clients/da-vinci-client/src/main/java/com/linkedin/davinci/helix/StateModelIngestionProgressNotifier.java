package com.linkedin.davinci.helix;

import static com.linkedin.davinci.helix.AbstractStateModelFactory.getStateModelID;

import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceTimeoutException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.stats.dimensions.VeniceIngestionFailureReason;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class notifies the Helix State Models (SM) about corresponding ingestion progress.
 * The class also holds latches that can be used in SM in the cases when state transitions
 * need to coordinate with ingestion progress.
 */
public class StateModelIngestionProgressNotifier implements VeniceNotifier {
  private final Logger logger = LogManager.getLogger(this.getClass());
  private static final long WAIT_POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
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
    waitConsumptionCompleted(
        resourceName,
        partitionId,
        bootstrapToOnlineTimeoutInHours,
        storeIngestionService,
        () -> false);
  }

  boolean waitConsumptionCompleted(
      String resourceName,
      int partitionId,
      int bootstrapToOnlineTimeoutInHours,
      StoreIngestionService storeIngestionService,
      BooleanSupplier shouldStopWaiting) throws InterruptedException {
    String stateModelId = getStateModelID(resourceName, partitionId);
    CountDownLatch ingestionCompleteFlag = stateModelToIngestionCompleteFlagMap.get(stateModelId);
    if (ingestionCompleteFlag == null) {
      String errorMsg =
          "No ingestion complete flag is found for resource:" + resourceName + " partition:" + partitionId;
      logger.error(errorMsg);
      throw new VeniceException(errorMsg);
    } else {
      long remainingWaitTimeInMs = TimeUnit.HOURS.toMillis(bootstrapToOnlineTimeoutInHours);
      long deadlineInMs = System.currentTimeMillis() + remainingWaitTimeInMs;
      boolean ingestionCompleted = false;
      while (!ingestionCompleted && remainingWaitTimeInMs > 0) {
        if (shouldStopWaiting.getAsBoolean()) {
          logger.info(
              "Stop waiting for ingestion completion for resource: {} partition: {} because version role changed",
              resourceName,
              partitionId);
          stopConsumption(resourceName, partitionId);
          return false;
        }
        ingestionCompleted =
            ingestionCompleteFlag.await(Math.min(WAIT_POLL_INTERVAL_MS, remainingWaitTimeInMs), TimeUnit.MILLISECONDS);
        remainingWaitTimeInMs = deadlineInMs - System.currentTimeMillis();
      }
      if (!ingestionCompleted) {
        // Timeout
        String errorMsg = "After waiting " + bootstrapToOnlineTimeoutInHours + " hours, resource:" + resourceName
            + " partition:" + partitionId + " still can not become online from bootstrap.";
        logger.error(errorMsg);
        // Report ingestion_failure
        String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
        int version = Version.parseVersionFromKafkaTopicName(resourceName);
        storeIngestionService
            .recordIngestionFailure(storeName, version, VeniceIngestionFailureReason.SERVING_VERSION_BOOTSTRAP_TIMEOUT);
        VeniceTimeoutException veniceException = new VeniceTimeoutException(errorMsg);
        storeIngestionService.getStoreIngestionTask(resourceName).reportError(errorMsg, partitionId, veniceException);
      }
      stateModelToIngestionCompleteFlagMap.remove(stateModelId);
      // If consumption is failed, throw an exception here, Helix will put this replica to ERROR state.
      if (stateModelToSuccessMap.containsKey(stateModelId) && !stateModelToSuccessMap.get(stateModelId)) {
        throw new VeniceException(
            "Consumption is failed. Thrown an exception to put this replica:" + stateModelId + " to ERROR state.");
      }
      return true;
    }
  }

  void stopConsumption(String resourceName, int partitionId) {
    final String stateModelID = getStateModelID(resourceName, partitionId);
    // Remove success first to avoid false ERROR promotion if a waiting transition wakes up while stopping.
    stateModelToSuccessMap.remove(stateModelID);
    CountDownLatch ingestionCompleteFlag = stateModelToIngestionCompleteFlagMap.get(stateModelID);
    if (ingestionCompleteFlag != null) {
      ingestionCompleteFlag.countDown();
    }
    stateModelToIngestionCompleteFlagMap.remove(stateModelID);
  }

  CountDownLatch getIngestionCompleteFlag(String resourceName, int partitionId) {
    return stateModelToIngestionCompleteFlagMap.get(getStateModelID(resourceName, partitionId));
  }

  @Override
  public void completed(String resourceName, int partitionId, PubSubPosition position, String message) {
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
    logger.error("Ingestion failed for replica: {} : {}", Utils.getReplicaId(resourceName, partitionId), message, ex);
    CountDownLatch ingestionCompleteFlag = getIngestionCompleteFlag(resourceName, partitionId);
    if (ingestionCompleteFlag != null) {
      ingestionCompleteFlag.countDown();
    } else {
      logger.info("No ingestion complete flag is found for resource: {} partition: {}", resourceName, partitionId);
    }
  }

  @Override
  public void stopped(String resourceName, int partitionId, PubSubPosition position) {
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
