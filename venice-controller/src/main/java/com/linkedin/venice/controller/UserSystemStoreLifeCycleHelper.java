package com.linkedin.venice.controller;

import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.authorization.Resource;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.PushMonitorDelegator;
import com.linkedin.venice.system.store.MetaStoreWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.log4j.Logger;


/**
 * This class is responsible for automatically create and delete per user store system store resources when the
 * corresponding user store is created or deleted.
 */
public class UserSystemStoreLifeCycleHelper {
  private static final String AUTO_META_SYSTEM_STORE_PUSH_ID_PREFIX = "Auto_meta_system_store_empty_push_";
  private static final long DEFAULT_META_SYSTEM_STORE_SIZE = 1024 * 1024 * 1024;
  private static final VeniceSystemStoreType[] aclRequiredSystemStores =
      new VeniceSystemStoreType[]{VeniceSystemStoreType.META_STORE, VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE};
  private static final Set<VeniceSystemStoreType> aclRequiredSystemStoresSet =
      new HashSet<>(Arrays.asList(aclRequiredSystemStores));

  private final Map<String, Set<VeniceSystemStoreType>> clusterToAutoCreateEnabledSystemStoresMap = new HashMap<>();
  private final VeniceParentHelixAdmin parentAdmin;
  private final Optional<AuthorizerService> authorizerService;

  public UserSystemStoreLifeCycleHelper(VeniceParentHelixAdmin parentAdmin,
      Optional<AuthorizerService> authorizerService, VeniceControllerMultiClusterConfig multiClusterConfig) {
    this.parentAdmin = parentAdmin;
    this.authorizerService = authorizerService;
    for (String cluster : multiClusterConfig.getClusters()) {
      VeniceControllerConfig controllerConfig = multiClusterConfig.getControllerConfig(cluster);
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

  public void maybeMaterializeSystemStoresForUserStore(String userStoreName, String clusterName) {
    if (VeniceSystemStoreType.getSystemStoreType(userStoreName) != null) {
      // Don't materialize system stores for system stores.
      return;
    }
    Set<VeniceSystemStoreType> autoCreateEnabledSystemStores =
        clusterToAutoCreateEnabledSystemStoresMap.get(clusterName);
    for (VeniceSystemStoreType systemStoreType : autoCreateEnabledSystemStores) {
      String systemStoreName = systemStoreType.getSystemStoreName(userStoreName);
      String pushJobId = AUTO_META_SYSTEM_STORE_PUSH_ID_PREFIX + System.currentTimeMillis();
      Version version = parentAdmin.incrementVersionIdempotent(clusterName, systemStoreName, pushJobId,
          parentAdmin.calculateNumberOfPartitions(clusterName, systemStoreName, DEFAULT_META_SYSTEM_STORE_SIZE),
          parentAdmin.getReplicationFactor(clusterName, systemStoreName));
      parentAdmin.writeEndOfPush(clusterName, systemStoreName, version.getNumber(), true);
    }
  }

  public void maybeCreateSystemStoreWildcardAcl(String storeName) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (systemStoreType != null && aclRequiredSystemStoresSet.contains(systemStoreType)
        && authorizerService.isPresent()) {
      // Ensure the wild card acl regex is created for this system store
      authorizerService.get().setupResource(new Resource(storeName));
    }
  }

  public static void deleteSystemStore(VeniceHelixAdmin admin, ReadWriteStoreRepository storeRepository,
      PushMonitorDelegator pushMonitor, String clusterName, String systemStoreName, boolean isStoreMigrating,
      MetaStoreWriter metaStoreWriter, Logger logger) {
    logger.info("Start deleting system store: " + systemStoreName);
    admin.deleteAllVersionsInStore(clusterName, systemStoreName);
    pushMonitor.cleanupStoreStatus(systemStoreName);
    if (!isStoreMigrating) {
      if (VeniceSystemStoreType.getSystemStoreType(systemStoreName) == VeniceSystemStoreType.META_STORE) {
        // Clean up venice writer before truncating RT topic
        metaStoreWriter.removeMetaStoreWriter(systemStoreName);
      }
      admin.truncateKafkaTopic(Version.composeRealTimeTopic(systemStoreName));
    } else {
      logger.info("The RT topic for: " + systemStoreName + " will not be deleted since the user store is migrating");
    }
    Store systemStore = storeRepository.getStore(systemStoreName);
    if (systemStore != null) {
      admin.truncateOldTopics(clusterName, systemStore, true);
    }
    logger.info("Finished deleting system store: " + systemStoreName);
  }

  public static void maybeDeleteSystemStoresForUserStore(VeniceHelixAdmin admin,
      ReadWriteStoreRepository storeRepository, PushMonitorDelegator pushMonitor, String clusterName, Store userStore,
      MetaStoreWriter metaStoreWriter, Logger logger) {
    if (userStore.isStoreMetaSystemStoreEnabled()) {
      deleteSystemStore(admin, storeRepository, pushMonitor, clusterName,
          VeniceSystemStoreType.META_STORE.getSystemStoreName(userStore.getName()), userStore.isMigrating(),
          metaStoreWriter, logger);
    }
    if (userStore.isDaVinciPushStatusStoreEnabled()) {
      deleteSystemStore(admin, storeRepository, pushMonitor, clusterName,
          VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(userStore.getName()),
          userStore.isMigrating(), metaStoreWriter, logger);
    }
  }
}
