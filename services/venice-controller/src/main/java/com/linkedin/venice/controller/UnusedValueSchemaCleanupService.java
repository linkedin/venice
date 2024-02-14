package com.linkedin.venice.controller;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.ReadWriteSchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This service runs in the parent controller to delete historical unused value schemas.
 * Currently it supports deletion of unused value schemas only for batch stores.
 */
public class UnusedValueSchemaCleanupService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(UnusedValueSchemaCleanupService.class);

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

          Set<Integer> inUseValueSchemaIds = veniceParentHelixAdmin.getInUseValueSchemaIds(clusterName, storeName);

          // if any of the child colo is unreachable, skip deletion.
          if (inUseValueSchemaIds.isEmpty()) {
            LOGGER.warn("Could not find in-use value schemas for store {}", storeName);
            continue;
          }

          Set<Integer> schemasToDelete = findSchemaIdsToDelete(
              allSchemas,
              store,
              admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository(),
              inUseValueSchemaIds);

          if (!schemasToDelete.isEmpty()) {
            // delete from child colos
            veniceParentHelixAdmin.deleteValueSchemas(clusterName, store.getName(), schemasToDelete);
            // delete from the parent colo
            admin.deleteValueSchemas(clusterName, store.getName(), schemasToDelete);
          }
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

  Set<Integer> findSchemaIdsToDelete(
      Collection<SchemaEntry> allSchemas,
      Store store,
      ReadWriteSchemaRepository schemaRepository,
      Set<Integer> inUseValueSchemaIds) {
    Set<Integer> schemasToDelete = new HashSet<>();

    // assumes `getValueSchemas` returns ascending schema ids so that the older schemas are deleted first
    for (SchemaEntry schemaEntry: allSchemas) {
      int schemaId = schemaEntry.getId();
      // skip latest value schema or super-set schema id
      if (schemaId == store.getLatestSuperSetValueSchemaId()
          || schemaRepository.getSupersetOrLatestValueSchema(store.getName()).getId() == schemaId) {
        continue;
      }

      // delete only if its not used
      if (!inUseValueSchemaIds.contains(schemaId)) {
        schemasToDelete.add(schemaId);
        // maintain minimum of SCHEMA_COUNT_THRESHOLD schemas in repo
        if (schemasToDelete.size() > allSchemas.size() - minSchemaCountToKeep) {
          break;
        }
      }
    }
    return schemasToDelete;
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
