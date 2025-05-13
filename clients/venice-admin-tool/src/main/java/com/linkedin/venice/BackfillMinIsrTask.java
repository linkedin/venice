package com.linkedin.venice;

import static com.linkedin.venice.ConfigKeys.KAFKA_MIN_IN_SYNC_REPLICAS;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class BackfillMinIsrTask implements Function<String, Boolean> {
  public static final String TASK_NAME = "BackfillMinIsr";
  private static final Logger LOGGER = LogManager.getLogger(BackfillMinIsrTask.class);
  private final Map<String, ControllerClient> childControllerClientMap;
  private final int vtMinISR;
  private final int rtMinISR;

  public BackfillMinIsrTask(Map<String, ControllerClient> controllerClientMap, int vtMinISR, int rtMinISR) {
    this.childControllerClientMap = controllerClientMap;
    this.vtMinISR = vtMinISR;
    this.rtMinISR = rtMinISR;
  }

  private Version getCurrentVersion(ControllerClient controllerClient, String storeName) {
    StoreResponse storeResponse = controllerClient.getStore(storeName);
    if (storeResponse.isError()) {
      LOGGER.warn("Unable to get store: {}", storeName);
      return null;
    }

    StoreInfo store = storeResponse.getStore();

    int currentVersionNum = store.getLargestUsedVersionNumber();
    Optional<Version> currentVersion = store.getVersion(currentVersionNum);

    if (!currentVersion.isPresent()) {
      LOGGER.warn("Unable to find version: {} in store: {}", currentVersionNum, storeName);
      return null;
    }

    return currentVersion.get();
  }

  @Override
  public Boolean apply(String storeName) {
    for (Map.Entry<String, ControllerClient> controllerClientEntry: childControllerClientMap.entrySet()) {
      ControllerClient childControllerClient = controllerClientEntry.getValue();
      Version version = getCurrentVersion(childControllerClient, storeName);
      if (version == null) {
        LOGGER.warn("Unable to find current version for store: {}", storeName);
        return false;
      }

      // Update RT min ISR
      if (version.isHybrid()) {
        String realTimeTopicName = Utils.getRealTimeTopicName(version);
        ControllerResponse rtResponse =
            childControllerClient.updateKafkaTopicMinInSyncReplica(realTimeTopicName, rtMinISR);

        if (rtResponse.isError()) {
          LOGGER.warn(
              "Unable to update the rt: {} min ISR to {} for store: {}",
              realTimeTopicName,
              KAFKA_MIN_IN_SYNC_REPLICAS,
              storeName);
          return false;
        }
      }

      // Update VT min ISR
      ControllerResponse vtResponse =
          childControllerClient.updateKafkaTopicMinInSyncReplica(version.kafkaTopicName(), vtMinISR);
      if (vtResponse.isError()) {
        LOGGER.warn(
            "Unable to update vt: {} min ISR to {} for store: {}",
            version.kafkaTopicName(),
            KAFKA_MIN_IN_SYNC_REPLICAS,
            storeName);
        return false;
      }
    }

    return true;
  }
}
