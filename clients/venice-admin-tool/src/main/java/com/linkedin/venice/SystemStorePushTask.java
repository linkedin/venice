package com.linkedin.venice;

import static com.linkedin.venice.AdminTool.printObject;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class aims to do one time emtpy push to all user system stores of a specific user store.
 * It will aggregate and compute the largest used version from all regions and update store before performing empty push.
 * It will also skip empty push to store which is being migrated and is in the destination cluster.
 */
public class SystemStorePushTask implements Function<String, Boolean> {
  public static final String TASK_NAME = "PushSystemStore";
  private static final Logger LOGGER = LogManager.getLogger(SystemStorePushTask.class);
  private static final int JOB_POLLING_RETRY_COUNT = 200;
  private static final int JOB_POLLING_RETRY_PERIOD_IN_SECONDS = 5;
  private static final String SYSTEM_STORE_PUSH_TASK_LOG_PREFIX = "[**** SYSTEM STORE PUSH ****]";
  private final List<VeniceSystemStoreType> systemStoreTypeList;

  private final ControllerClient parentControllerClient;
  private final String clusterName;
  private final Map<String, ControllerClient> childControllerClientMap;

  public SystemStorePushTask(
      ControllerClient parentControllerClient,
      Map<String, ControllerClient> controllerClientMap,
      String clusterName,
      Optional<String> systemStoreTypeFilter) {
    this.parentControllerClient = parentControllerClient;
    this.childControllerClientMap = controllerClientMap;
    this.clusterName = clusterName;
    if (systemStoreTypeFilter.isPresent()) {
      systemStoreTypeList = Collections.singletonList(VeniceSystemStoreType.valueOf(systemStoreTypeFilter.get()));
    } else {
      systemStoreTypeList =
          Arrays.asList(VeniceSystemStoreType.META_STORE, VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE);
    }
  }

  public Boolean apply(String storeName) {
    StoreResponse storeResponse = parentControllerClient.getStore(storeName);
    if (storeResponse.isError()) {
      LOGGER.error("{} Unable to locate user store: {}", SYSTEM_STORE_PUSH_TASK_LOG_PREFIX, storeName);
      return false;
    }
    if (storeResponse.getStore().isMigrating() && storeResponse.getStore().isMigrationDuplicateStore()) {
      LOGGER.error(
          "{} Unable to empty push to system store of migrating dest cluster store: {} in cluster: {}",
          SYSTEM_STORE_PUSH_TASK_LOG_PREFIX,
          storeName,
          clusterName);
      return false;
    }

    for (VeniceSystemStoreType type: systemStoreTypeList) {
      String systemStoreName = type.getSystemStoreName(storeName);
      /**
       *  In current implementation, a push to system store will flip the flag to true, which can introduce unexpected
       *  behavior to the store. Here, we skip the system store push if it is turned off.
       */
      boolean isSystemStoreEnabled = VeniceSystemStoreType.META_STORE.equals(type)
          ? storeResponse.getStore().isStoreMetaSystemStoreEnabled()
          : storeResponse.getStore().isDaVinciPushStatusStoreEnabled();
      if (!isSystemStoreEnabled) {
        LOGGER.warn(
            "{} System store: {} is disabled. Will skip the push.",
            SYSTEM_STORE_PUSH_TASK_LOG_PREFIX,
            systemStoreName);
      }
      VersionResponse response = parentControllerClient.getStoreLargestUsedVersion(clusterName, systemStoreName);
      if (response.isError()) {
        LOGGER.error(
            "{} Unable to locate largest used store version for: {}",
            SYSTEM_STORE_PUSH_TASK_LOG_PREFIX,
            systemStoreName);
        return false;
      }
      int largestUsedVersion = response.getVersion();

      int version = getStoreLargestUsedVersionNumber(parentControllerClient, systemStoreName);
      if (version == -1) {
        return false;
      }
      largestUsedVersion = Math.max(largestUsedVersion, version);
      for (Map.Entry<String, ControllerClient> controllerClientEntry: childControllerClientMap.entrySet()) {
        int result = getStoreLargestUsedVersionNumber(controllerClientEntry.getValue(), systemStoreName);
        if (result == -1) {
          LOGGER.error(
              "{} Unable to locate store for: {} in region: {}",
              SYSTEM_STORE_PUSH_TASK_LOG_PREFIX,
              systemStoreName,
              controllerClientEntry.getKey());
          return false;
        }
        largestUsedVersion = Math.max(largestUsedVersion, result);
      }

      LOGGER.info("Aggregate largest version: {} for store: {}", largestUsedVersion, systemStoreName);
      ControllerResponse controllerResponse = parentControllerClient
          .updateStore(systemStoreName, new UpdateStoreQueryParams().setLargestUsedVersionNumber(largestUsedVersion));
      if (controllerResponse.isError()) {
        LOGGER.error(
            "{} Unable to set largest used store version for: {} as {} in all regions",
            SYSTEM_STORE_PUSH_TASK_LOG_PREFIX,
            systemStoreName,
            largestUsedVersion);
        return false;
      }

      VersionCreationResponse versionCreationResponse =
          parentControllerClient.emptyPush(systemStoreName, "SYSTEM_STORE_PUSH_" + System.currentTimeMillis(), 10000);
      // Kafka topic name in the above response is null, and it will be fixed with this code change.
      String topicName = Version.composeKafkaTopic(systemStoreName, versionCreationResponse.getVersion());
      // Polling job status to make sure the empty push hits every child colo
      int count = JOB_POLLING_RETRY_COUNT;
      while (true) {
        JobStatusQueryResponse jobStatusQueryResponse =
            parentControllerClient.retryableRequest(3, controllerClient -> controllerClient.queryJobStatus(topicName));
        printObject(jobStatusQueryResponse, System.out::print);
        if (jobStatusQueryResponse.isError()) {
          return false;
        }
        ExecutionStatus executionStatus = ExecutionStatus.valueOf(jobStatusQueryResponse.getStatus());
        if (executionStatus.isTerminal()) {
          if (executionStatus.isError()) {
            LOGGER.error("{} Push error for topic: {}", SYSTEM_STORE_PUSH_TASK_LOG_PREFIX, topicName);
            return false;
          }
          LOGGER.info("{} Push completed: {}", SYSTEM_STORE_PUSH_TASK_LOG_PREFIX, topicName);
          break;
        }
        Utils.sleep(TimeUnit.SECONDS.toMillis(JOB_POLLING_RETRY_PERIOD_IN_SECONDS));
        count--;
        if (count == 0) {
          LOGGER.error(
              "{} Push not finished: {} in {} seconds",
              SYSTEM_STORE_PUSH_TASK_LOG_PREFIX,
              topicName,
              JOB_POLLING_RETRY_COUNT * JOB_POLLING_RETRY_PERIOD_IN_SECONDS);
          return false;
        }
      }
    }
    return true;
  }

  int getStoreLargestUsedVersionNumber(ControllerClient controllerClient, String systemStoreName) {
    // Make sure store exist in region and return largest used version number.
    StoreResponse systemStoreResponse = controllerClient.getStore(systemStoreName);
    if (systemStoreResponse.isError()) {
      return -1;
    }
    return systemStoreResponse.getStore().getLargestUsedVersionNumber();
  }
}
