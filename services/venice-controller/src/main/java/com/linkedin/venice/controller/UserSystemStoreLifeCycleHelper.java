package com.linkedin.venice.controller;

import static com.linkedin.venice.common.VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE;
import static com.linkedin.venice.common.VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE;
import static com.linkedin.venice.meta.Version.DEFAULT_RT_VERSION_NUMBER;

import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.authorization.Resource;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.PushMonitorDelegator;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is responsible for automatically create and delete per user store system store resources when the
 * corresponding user store is created or deleted.
 */
public class UserSystemStoreLifeCycleHelper {
  static final String AUTO_META_SYSTEM_STORE_PUSH_ID_PREFIX = "Auto_meta_system_store_empty_push_";
  private static final VeniceSystemStoreType[] aclRequiredSystemStores =
      new VeniceSystemStoreType[] { VeniceSystemStoreType.META_STORE, VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE };
  private static final Set<VeniceSystemStoreType> aclRequiredSystemStoresSet =
      new HashSet<>(Arrays.asList(aclRequiredSystemStores));
  private static final Logger LOGGER = LogManager.getLogger(UserSystemStoreLifeCycleHelper.class);

  private final Map<String, Set<VeniceSystemStoreType>> clusterToAutoCreateEnabledSystemStoresMap = new HashMap<>();
  private final VeniceParentHelixAdmin parentAdmin;
  private final Optional<AuthorizerService> authorizerService;

  public UserSystemStoreLifeCycleHelper(
      VeniceParentHelixAdmin parentAdmin,
      Optional<AuthorizerService> authorizerService,
      VeniceControllerMultiClusterConfig multiClusterConfig) {
    this.parentAdmin = parentAdmin;
    this.authorizerService = authorizerService;
    for (String cluster: multiClusterConfig.getClusters()) {
      VeniceControllerClusterConfig controllerConfig = multiClusterConfig.getControllerConfig(cluster);
      Set<VeniceSystemStoreType> autoCreateEnabledSystemStores = new HashSet<>();
      if (controllerConfig.isZkSharedMetaSystemSchemaStoreAutoCreationEnabled()
          && controllerConfig.isAutoMaterializeMetaSystemStoreEnabled()) {
        autoCreateEnabledSystemStores.add(VeniceSystemStoreType.META_STORE);
      }
      if (controllerConfig.isZkSharedDaVinciPushStatusSystemSchemaStoreAutoCreationEnabled()
          && controllerConfig.isAutoMaterializeDaVinciPushStatusSystemStoreEnabled()) {
        autoCreateEnabledSystemStores.add(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE);
      }
      clusterToAutoCreateEnabledSystemStoresMap.put(cluster, autoCreateEnabledSystemStores);
    }
  }

  public List<VeniceSystemStoreType> materializeSystemStoresForUserStore(String clusterName, String userStoreName) {
    List<VeniceSystemStoreType> createdSystemStoreTypes = new ArrayList<>();
    Set<VeniceSystemStoreType> autoCreateEnabledSystemStores =
        clusterToAutoCreateEnabledSystemStoresMap.get(clusterName);
    for (VeniceSystemStoreType systemStoreType: autoCreateEnabledSystemStores) {
      String systemStoreName = systemStoreType.getSystemStoreName(userStoreName);
      String pushJobId = AUTO_META_SYSTEM_STORE_PUSH_ID_PREFIX + System.currentTimeMillis();
      materializeSystemStore(parentAdmin, clusterName, systemStoreName, pushJobId);
      createdSystemStoreTypes.add(systemStoreType);
    }
    return createdSystemStoreTypes;
  }

  public static Version materializeSystemStore(
      VeniceParentHelixAdmin parentAdmin,
      String clusterName,
      String systemStoreName,
      String pushJobId) {
    Version version;
    final int systemStoreLargestUsedVersionNumber =
        parentAdmin.getLargestUsedVersionFromStoreGraveyard(clusterName, systemStoreName);

    int partitionCount = parentAdmin.calculateNumberOfPartitions(clusterName, systemStoreName);
    int replicationFactor = parentAdmin.getReplicationFactor(clusterName, systemStoreName);

    if (systemStoreLargestUsedVersionNumber == Store.NON_EXISTING_VERSION) {
      version = parentAdmin
          .incrementVersionIdempotent(clusterName, systemStoreName, pushJobId, partitionCount, replicationFactor);
    } else {
      version = parentAdmin.addVersionAndTopicOnly(
          clusterName,
          systemStoreName,
          pushJobId,
          systemStoreLargestUsedVersionNumber + 1,
          partitionCount,
          replicationFactor,
          Version.PushType.BATCH,
          false,
          false,
          null,
          Optional.empty(),
          -1,
          Optional.empty(),
          false,
          null,
          -1,
          DEFAULT_RT_VERSION_NUMBER);
    }
    parentAdmin.writeEndOfPush(clusterName, systemStoreName, version.getNumber(), true);
    return version;
  }

  public void maybeCreateSystemStoreWildcardAcl(String storeName) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (systemStoreType != null && aclRequiredSystemStoresSet.contains(systemStoreType)
        && authorizerService.isPresent()) {
      // Ensure the wild card acl regex is created for this system store
      authorizerService.get().setupResource(new Resource(storeName));
    }
  }

  public static void deleteSystemStore(
      VeniceHelixAdmin admin,
      ReadWriteStoreRepository storeRepository,
      PushMonitorDelegator pushMonitor,
      String clusterName,
      String systemStoreName,
      boolean isStoreMigrating,
      MetaStoreWriter metaStoreWriter,
      Logger LOGGER) {
    LOGGER.info("Start deleting system store: {}", systemStoreName);
    admin.deleteAllVersionsInStore(clusterName, systemStoreName);
    pushMonitor.cleanupStoreStatus(systemStoreName);
    if (!isStoreMigrating) {
      switch (VeniceSystemStoreType.getSystemStoreType(systemStoreName)) {
        case META_STORE:
          // Clean up venice writer before truncating RT topic
          metaStoreWriter.removeMetaStoreWriter(systemStoreName);
          break;
        case DAVINCI_PUSH_STATUS_STORE:
          admin.getPushStatusStoreWriter()
              .removePushStatusStoreVeniceWriter(DAVINCI_PUSH_STATUS_STORE.extractRegularStoreName(systemStoreName));
          break;
        case BATCH_JOB_HEARTBEAT_STORE:
          // TODO: do we need to do any clean up here? HEARTBEAT_STORE is not coupled with any specific user store.
          LOGGER.error(
              "Venice store {} has a coupled batch job heartbeat system store?",
              BATCH_JOB_HEARTBEAT_STORE.extractRegularStoreName(systemStoreName));
          break;
        default:
          throw new VeniceException("Unknown system store type: " + systemStoreName);
      }
      admin.truncateKafkaTopic(Utils.composeRealTimeTopic(systemStoreName));
    } else {
      LOGGER.info("The RT topic for: {} will not be deleted since the user store is migrating", systemStoreName);
    }
    Store systemStore = storeRepository.getStore(systemStoreName);
    if (systemStore != null) {
      admin.truncateOldTopics(clusterName, systemStore, true);
    }
    LOGGER.info("Finished deleting system store: {}", systemStoreName);
  }

  public static void maybeDeleteSystemStoresForUserStore(
      VeniceHelixAdmin admin,
      ReadWriteStoreRepository storeRepository,
      PushMonitorDelegator pushMonitor,
      String clusterName,
      Store userStore,
      MetaStoreWriter metaStoreWriter,
      Logger LOGGER) {
    if (userStore.isDaVinciPushStatusStoreEnabled()) {
      deleteSystemStore(
          admin,
          storeRepository,
          pushMonitor,
          clusterName,
          DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(userStore.getName()),
          userStore.isMigrating(),
          metaStoreWriter,
          LOGGER);
    }
    // We must delete meta system store at the end as deleting other system store will try to send update to meta system
    // store as well.
    if (userStore.isStoreMetaSystemStoreEnabled()) {
      deleteSystemStore(
          admin,
          storeRepository,
          pushMonitor,
          clusterName,
          VeniceSystemStoreType.META_STORE.getSystemStoreName(userStore.getName()),
          userStore.isMigrating(),
          metaStoreWriter,
          LOGGER);
    }
  }

  /**
   * This method checks if a specific system store type is enabled in a given user store.
   */
  public static boolean isSystemStoreTypeEnabledInUserStore(Store userStore, VeniceSystemStoreType systemStoreType) {
    switch (systemStoreType) {
      case META_STORE:
        return userStore.isStoreMetaSystemStoreEnabled();
      case DAVINCI_PUSH_STATUS_STORE:
        return userStore.isDaVinciPushStatusStoreEnabled();
      default:
        LOGGER.warn("System store type: {} is not user level system store, return false by default.", systemStoreType);
        return false;
    }
  }
}
