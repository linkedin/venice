package com.linkedin.venice.controller;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class UnusedValueSchemaCleanupService extends AbstractVeniceService {
  private final ScheduledExecutorService executor =
      Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("UnusedValueSchemaCleanupService"));
  private final VeniceControllerMultiClusterConfig multiClusterConfig;
  private final VeniceParentHelixAdmin veniceParentHelixAdmin;
  private final VeniceHelixAdmin admin;
  private final int scheduleIntervalMinutes;
  private boolean stop = false;
  public final int minSchemaCountToKeep;

  UnusedValueSchemaCleanupService(
      VeniceControllerMultiClusterConfig multiClusterConfig,
      VeniceHelixAdmin admin,
      VeniceParentHelixAdmin parentHelixAdmin) {
    this.multiClusterConfig = multiClusterConfig;
    this.admin = admin;
    this.scheduleIntervalMinutes = multiClusterConfig.getUnusedSchemaCleanupIntervalMinutes();
    this.minSchemaCountToKeep = multiClusterConfig.getMinSchemaCountToKeep();
    this.veniceParentHelixAdmin = parentHelixAdmin;
  }

  private Runnable getRunnableForSchemaCleanup() {
    return () -> {
      if (stop) {
        return;
      }
      for (String clusterName: multiClusterConfig.getClusters()) {
        boolean cleanupEnabled =
            multiClusterConfig.getControllerConfig(clusterName).isUnusedValueSchemaCleanupServiceEnabled();
        if (!cleanupEnabled) {
          continue;
        }
        // Get all stores for current cluster
        List<Store> stores = admin.getAllStores(clusterName);
        for (Store store: stores) {
          String storeName = store.getName();
          // Remove schema only for batch stores
          if (isHybridStore(store) || VeniceSystemStoreUtils.isSystemStore(storeName)) {
            continue;
          }
          Collection<SchemaEntry> allSchemas = veniceParentHelixAdmin.getValueSchemas(clusterName, storeName);

          if (allSchemas.size() < minSchemaCountToKeep) {
            continue;
          }

          Set<Integer> usedSchemaSet = veniceParentHelixAdmin.getInUseValueSchemaIds(clusterName, storeName);

          if (usedSchemaSet.isEmpty()) {
            continue;
          }
          int minSchemaIdInUse = Collections.min(usedSchemaSet);
          Set<Integer> schemasToDelete = new HashSet<>();

          // assumes `getValueSchemas` returns ascending schema ids so that the older schemas are deleted first
          for (SchemaEntry schemaEntry: allSchemas) {
            if (schemaEntry.getId() == store.getLatestSuperSetValueSchemaId()) {
              continue;
            }

            // delete only if its not used and less than minimum of used schema id
            if (!usedSchemaSet.contains(schemaEntry.getId()) && schemaEntry.getId() < minSchemaIdInUse) {
              schemasToDelete.add(schemaEntry.getId());
              // maintain minimum of SCHEMA_COUNT_THRESHOLD schemas in repo
              if (schemasToDelete.size() > allSchemas.size() - minSchemaCountToKeep) {
                break;
              }
            }
          }
          // delete from parent
          admin.deleteValueSchemas(clusterName, store.getName(), schemasToDelete);
          // delete from child colos
          veniceParentHelixAdmin.deleteValueSchemas(clusterName, store.getName(), schemasToDelete);
        }
      }
    };
  }

  private boolean isHybridStore(Store store) {
    if (store.getHybridStoreConfig() != null) {
      return true;
    }
    // check future/backup versions
    for (Version version: store.getVersions()) {
      if (version.getHybridStoreConfig() != null) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean startInner() throws Exception {
    executor.scheduleAtFixedRate(getRunnableForSchemaCleanup(), 0, scheduleIntervalMinutes, TimeUnit.MINUTES);
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    this.stop = true;
    executor.shutdownNow();
    executor.awaitTermination(30, TimeUnit.SECONDS);
  }
}
