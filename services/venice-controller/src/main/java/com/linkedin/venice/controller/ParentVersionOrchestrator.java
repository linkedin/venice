package com.linkedin.venice.controller;

import static com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask.IGNORED_CURRENT_VERSION;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;

import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteAllVersions;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteOldVersion;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.RepushInfo;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Orchestrates version-related operations on the parent controller. The public {@link Admin} API remains implemented by
 * {@link VeniceParentHelixAdmin}; this class owns the extracted parent-specific version metadata behavior.
 */
class ParentVersionOrchestrator {
  private static final Logger LOGGER = LogManager.getLogger(ParentVersionOrchestrator.class);

  private final VeniceParentHelixAdmin parent;

  ParentVersionOrchestrator(VeniceParentHelixAdmin parent) {
    this.parent = parent;
  }

  int getCurrentVersion(String clusterName, String storeName) {
    throw new VeniceUnsupportedOperationException(
        "getCurrentVersion",
        "Please use getCurrentVersionsForMultiColos in Parent controller.");
  }

  Map<String, Integer> getCurrentVersionsForMultiColos(String clusterName, String storeName) {
    Map<String, ControllerClient> controllerClients = parent.getVeniceHelixAdmin().getControllerClientMap(clusterName);
    return getCurrentVersionForMultiRegions(clusterName, storeName, controllerClients);
  }

  RepushInfo getRepushInfo(String clusterName, String storeName, Optional<String> fabricName) {
    Map<String, ControllerClient> controllerClients = parent.getVeniceHelixAdmin().getControllerClientMap(clusterName);
    String systemSchemaClusterName = parent.getMultiClusterConfigs().getSystemSchemaClusterName();
    VeniceControllerClusterConfig systemSchemaClusterConfig =
        parent.getMultiClusterConfigs().getControllerConfig(systemSchemaClusterName);

    if (fabricName.isPresent()) {
      StoreResponse response = controllerClients.get(fabricName.get()).getStore(storeName);
      if (response.isError()) {
        throw new VeniceException(
            "Could not query store from colo: " + fabricName.get() + " for cluster: " + clusterName + ". "
                + response.getError());
      }
      return RepushInfo.createRepushInfo(
          response.getStore()
              .getVersion(response.getStore().getCurrentVersion())
              .orElseThrow(
                  () -> new VeniceException(
                      "Could not find current version " + response.getStore().getCurrentVersion() + " for store "
                          + storeName + " in colo " + fabricName.get() + " for cluster: " + clusterName)),
          response.getStore().getKafkaBrokerUrl(),
          systemSchemaClusterConfig.getClusterToD2Map().get(systemSchemaClusterName),
          systemSchemaClusterConfig.getChildControllerD2ZkHost(fabricName.get()));
    }
    // fabricName not present, get the largest version info among the child colos.
    Map<String, Integer> currentVersionsMap =
        getCurrentVersionForMultiRegions(clusterName, storeName, controllerClients);
    int largestVersion = Integer.MIN_VALUE;
    String colo = null;
    for (Map.Entry<String, Integer> mapEntry: currentVersionsMap.entrySet()) {
      if (mapEntry.getValue() > largestVersion) {
        largestVersion = mapEntry.getValue();
        colo = mapEntry.getKey();
      }
    }
    // Before extraction, this no-fabric path still referred back to fabricName.get() in the largest-colo error path,
    // which would throw because fabricName is absent here. Use the child colo selected from currentVersionsMap for both
    // diagnostics and child-controller D2 lookup instead.
    final String largestVersionColo = colo;
    StoreResponse response = controllerClients.get(largestVersionColo).getStore(storeName);
    if (response.isError()) {
      throw new VeniceException(
          "Could not query store from largest version colo: " + largestVersionColo + " for cluster: " + clusterName
              + ". " + response.getError());
    }
    return RepushInfo.createRepushInfo(
        response.getStore()
            .getVersion(response.getStore().getCurrentVersion())
            .orElseThrow(
                () -> new VeniceException(
                    "Could not find current version " + response.getStore().getCurrentVersion() + " for store "
                        + storeName + " in largest version colo " + largestVersionColo + " for cluster: "
                        + clusterName)),
        response.getStore().getKafkaBrokerUrl(),
        systemSchemaClusterConfig.getClusterToD2Map().get(systemSchemaClusterName),
        systemSchemaClusterConfig.getChildControllerD2ZkHost(largestVersionColo));
  }

  Map<String, String> getFutureVersionsForMultiColos(String clusterName, String storeName) {
    Map<String, ControllerClient> controllerClients = parent.getVeniceHelixAdmin().getControllerClientMap(clusterName);
    return parent.getVeniceHelixAdmin()
        .getFabricControllerClientProvider()
        .queryAllRegions(
            controllerClients,
            clusterName,
            5,
            controllerClient -> controllerClient.getFutureVersions(clusterName, storeName),
            response -> response.getStoreStatusMap().get(storeName),
            String.valueOf(IGNORED_CURRENT_VERSION));
  }

  /**
   * Multi-region safety check for a destructive PubSub operation on {@code topicName}. Resolves the owning cluster from
   * store config, then fans out to every region to gather the current, future, and backup versions. The operation is
   * blocked when the topic backs a non-deprecated version (current/future/backup) in any region, or when a region's
   * current version cannot be read (unreachable, {@link #IGNORED_CURRENT_VERSION}) — in which case it cannot be proven
   * safe. Orphaned topics (no resolvable store/cluster) are allowed, preserving legacy behavior.
   */
  TopicOperationSafetyVerdict checkTopicOperationSafety(String topicName) {
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    StoreConfig storeConfig = (storeName == null || storeName.isEmpty())
        ? null
        : parent.getVeniceHelixAdmin().getStoreConfigRepo().getStoreConfig(storeName).orElse(null);
    if (storeConfig == null || storeConfig.getCluster() == null || storeConfig.getCluster().isEmpty()) {
      return TopicOperationSafetyVerdict
          .allowed(topicName, storeName, null, "No owning store/cluster resolved for topic; operation allowed.");
    }
    String clusterName = storeConfig.getCluster();

    Map<String, Integer> currentVersions = getCurrentVersionsForMultiColos(clusterName, storeName);
    // region -> human-readable reason the operation is blocked there.
    Map<String, String> blockingRegions = new HashMap<>();

    // Regions we could not reach cannot be proven safe -> fail safe and block.
    for (Map.Entry<String, Integer> entry: currentVersions.entrySet()) {
      if (entry.getValue() == IGNORED_CURRENT_VERSION) {
        blockingRegions.put(entry.getKey(), "current version could not be read (region unreachable)");
      }
    }

    if (Version.isRealTimeTopic(topicName)) {
      // A real-time topic is shared across versions; it is unsafe to operate on while the store has any serving version
      // in any region.
      for (Map.Entry<String, Integer> entry: currentVersions.entrySet()) {
        int current = entry.getValue();
        if (current != IGNORED_CURRENT_VERSION && current != NON_EXISTING_VERSION) {
          blockingRegions.putIfAbsent(entry.getKey(), "store has current version " + current);
        }
      }
    } else {
      int targetVersion = Version.parseVersionFromKafkaTopicName(topicName);
      Map<String, String> futureVersions = getFutureVersionsForMultiColos(clusterName, storeName);
      Map<String, String> backupVersions = getBackupVersionsForMultiColos(clusterName, storeName);
      for (String region: currentVersions.keySet()) {
        if (blockingRegions.containsKey(region)) {
          continue; // already blocked (unreachable)
        }
        if (currentVersions.get(region) == targetVersion) {
          blockingRegions.put(region, "version " + targetVersion + " is the current version");
        } else if (parseVersionOrIgnored(futureVersions.get(region)) == targetVersion) {
          blockingRegions.put(region, "version " + targetVersion + " is a future version");
        } else if (parseVersionOrIgnored(backupVersions.get(region)) == targetVersion) {
          blockingRegions.put(region, "version " + targetVersion + " is a backup version");
        }
      }
    }

    if (blockingRegions.isEmpty()) {
      return TopicOperationSafetyVerdict
          .allowed(topicName, storeName, clusterName, "Topic does not back any non-deprecated version in any region.");
    }
    return TopicOperationSafetyVerdict.blocked(
        topicName,
        storeName,
        clusterName,
        "Topic backs a non-deprecated version (or could not be verified) in region(s): " + blockingRegions.keySet(),
        blockingRegions);
  }

  /**
   * Parse a version number from a multi-colo version string, returning {@link #IGNORED_CURRENT_VERSION} when the value
   * is absent/non-numeric (e.g. the error sentinel, or no future/backup version exists).
   */
  private static int parseVersionOrIgnored(String versionStr) {
    if (versionStr == null || versionStr.isEmpty()) {
      return IGNORED_CURRENT_VERSION;
    }
    try {
      return Integer.parseInt(versionStr.trim());
    } catch (NumberFormatException e) {
      return IGNORED_CURRENT_VERSION;
    }
  }

  Map<String, String> getBackupVersionsForMultiColos(String clusterName, String storeName) {
    Map<String, ControllerClient> controllerClients = parent.getVeniceHelixAdmin().getControllerClientMap(clusterName);
    return parent.getVeniceHelixAdmin()
        .getFabricControllerClientProvider()
        .queryAllRegions(
            controllerClients,
            clusterName,
            1,
            controllerClient -> controllerClient.getBackupVersions(clusterName, storeName),
            response -> response.getStoreStatusMap().get(storeName),
            String.valueOf(IGNORED_CURRENT_VERSION));
  }

  int getFutureVersion(String clusterName, String storeName) {
    return NON_EXISTING_VERSION;
  }

  int getBackupVersion(String clusterName, String storeName) {
    return NON_EXISTING_VERSION;
  }

  Map<String, Integer> getCurrentVersionForMultiRegions(
      String clusterName,
      String storeName,
      Map<String, ControllerClient> controllerClients) {
    return parent.getVeniceHelixAdmin()
        .getFabricControllerClientProvider()
        .queryAllRegions(
            controllerClients,
            clusterName,
            1,
            controllerClient -> controllerClient.getStore(storeName),
            response -> response.getStore().getCurrentVersion(),
            IGNORED_CURRENT_VERSION);
  }

  List<Version> deleteAllVersionsInStore(String clusterName, String storeName) {
    parent.acquireAdminMessageLock(clusterName, storeName);
    try {
      parent.getVeniceHelixAdmin().checkPreConditionForDeletion(clusterName, storeName);

      DeleteAllVersions deleteAllVersions = (DeleteAllVersions) AdminMessageType.DELETE_ALL_VERSIONS.getNewInstance();
      deleteAllVersions.clusterName = clusterName;
      deleteAllVersions.storeName = storeName;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.DELETE_ALL_VERSIONS.getValue();
      message.payloadUnion = deleteAllVersions;

      parent.sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
      return Collections.emptyList();
    } finally {
      parent.releaseAdminMessageLock(clusterName, storeName);
    }
  }

  void deleteOldVersionInStore(String clusterName, String storeName, int versionNum) {
    parent.acquireAdminMessageLock(clusterName, storeName);
    try {
      parent.getVeniceHelixAdmin().checkPreConditionForSingleVersionDeletion(clusterName, storeName, versionNum);

      DeleteOldVersion deleteOldVersion = (DeleteOldVersion) AdminMessageType.DELETE_OLD_VERSION.getNewInstance();
      deleteOldVersion.clusterName = clusterName;
      deleteOldVersion.storeName = storeName;
      deleteOldVersion.versionNum = versionNum;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.DELETE_OLD_VERSION.getValue();
      message.payloadUnion = deleteOldVersion;

      parent.sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
    } finally {
      parent.releaseAdminMessageLock(clusterName, storeName);
    }
  }

  List<Version> versionsForStore(String clusterName, String storeName) {
    return parent.getVeniceHelixAdmin().versionsForStore(clusterName, storeName);
  }

  void setStoreCurrentVersion(String clusterName, String storeName, int versionNumber) {
    throw new VeniceUnsupportedOperationException(
        "setStoreCurrentVersion",
        "Please use set-version only on child controllers, "
            + "setting version on parent is not supported, since the version list could be different fabric by fabric");
  }

  void setStoreLargestUsedVersion(String clusterName, String storeName, int versionNumber) {
    throw new VeniceUnsupportedOperationException(
        "setStoreLargestUsedVersion",
        "This is only supported in the Child Controller.");
  }

  void setStoreLargestUsedRTVersion(String clusterName, String storeName, int versionNumber) {
    throw new VeniceUnsupportedOperationException(
        "setStoreLargestUsedRTVersion",
        "This is only supported in the Child Controller.");
  }

  int getCurrentVersionInRegion(String clusterName, String storeName, String regionName) {
    Map<String, ControllerClient> controllerClients = parent.getVeniceHelixAdmin().getControllerClientMap(clusterName);
    ControllerClient regionClient = controllerClients.get(regionName);
    if (regionClient == null) {
      LOGGER.warn("No controller client for region: {} in cluster: {}", regionName, clusterName);
      return -1;
    }
    StoreResponse response = regionClient.getStore(storeName);
    if (response.isError()) {
      LOGGER.warn(
          "Failed to get store {} from region {} in cluster {}: {}",
          storeName,
          regionName,
          clusterName,
          response.getError());
      return -1;
    }
    return response.getStore().getCurrentVersion();
  }

  void updateStoreVersionStatus(String clusterName, String storeName, int version, VersionStatus status) {
    parent.getVeniceHelixAdmin().updateStoreVersionStatus(clusterName, storeName, version, status);
  }

  int getLargestUsedVersionFromStoreGraveyard(String clusterName, String storeName) {
    Map<String, ControllerClient> childControllers = parent.getVeniceHelixAdmin().getControllerClientMap(clusterName);
    int aggregatedLargestUsedVersionNumber = parent.getStoreGraveyard().getLargestUsedVersionNumber(storeName);
    for (Map.Entry<String, ControllerClient> controller: childControllers.entrySet()) {
      VersionResponse response = controller.getValue().getStoreLargestUsedVersion(clusterName, storeName);
      if (response.getVersion() > aggregatedLargestUsedVersionNumber) {
        aggregatedLargestUsedVersionNumber = response.getVersion();
      }
    }
    return aggregatedLargestUsedVersionNumber;
  }

  int getLargestUsedVersion(String clusterName, String storeName) {
    Map<String, ControllerClient> childControllers = parent.getVeniceHelixAdmin().getControllerClientMap(clusterName);
    int aggregatedLargestUsedVersionNumber = parent.getVeniceHelixAdmin().getLargestUsedVersion(clusterName, storeName);
    for (Map.Entry<String, ControllerClient> controller: childControllers.entrySet()) {
      VersionResponse response = controller.getValue().getStoreLargestUsedVersion(clusterName, storeName);
      if (response.getVersion() > aggregatedLargestUsedVersionNumber) {
        aggregatedLargestUsedVersionNumber = response.getVersion();
      }
    }
    return aggregatedLargestUsedVersionNumber;
  }
}
