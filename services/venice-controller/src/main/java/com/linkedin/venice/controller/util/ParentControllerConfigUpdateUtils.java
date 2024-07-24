package com.linkedin.venice.controller.util;

import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a utility class for Parent Controller store update logics.
 * The method here aims to take in current status and request params to determine if certain feature is updated / should
 * be updated based on some customized logics.
 */
public class ParentControllerConfigUpdateUtils {
  public static final Logger LOGGER = LogManager.getLogger(ParentControllerConfigUpdateUtils.class);
  public static final WriteComputeSchemaConverter updateSchemaConverter = WriteComputeSchemaConverter.getInstance();

  /**
   * This method takes in current status and request and try to determine whether to change partial update config.
   * The check logic is:
   * Step (1): If there is explict request, we will respect the request and maybe update config if new request value is
   * different from existing config value. In this step, if we are enabling partial update, we will also perform a dry
   * run to validate schema. If validation fails, it will throw exception and fail the whole request.
   * Step (2): If there is NO explict request and store is being converted into hybrid store, we will check the cluster
   * config and store's latest A/A config to see whether we should by default enable partial update. If so, we will also
   * perform a dry on to validate schema. If validation fails, it will swallow the exception and log warning message. It
   * will not turn on partial update and will not fail the whole request.
   */
  public static boolean checkAndMaybeApplyPartialUpdateConfig(
      VeniceParentHelixAdmin parentHelixAdmin,
      String clusterName,
      String storeName,
      Optional<Boolean> partialUpdateRequest,
      UpdateStore setStore,
      boolean storeBeingConvertedToHybrid) {
    Store currentStore = parentHelixAdmin.getVeniceHelixAdmin().getStore(clusterName, storeName);
    VeniceControllerClusterConfig controllerConfig =
        parentHelixAdmin.getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getConfig();
    boolean partialUpdateConfigChanged = false;
    setStore.writeComputationEnabled = currentStore.isWriteComputationEnabled();
    if (partialUpdateRequest.isPresent()) {
      setStore.writeComputationEnabled = partialUpdateRequest.get();
      if (partialUpdateRequest.get() && !currentStore.isWriteComputationEnabled()) {
        // Dry-run generating update schemas before sending admin messages to enable partial update because
        // update schema generation may fail due to some reasons. If that happens, abort the store update process.
        addUpdateSchemaForStore(parentHelixAdmin, clusterName, storeName, true);
      }
      // Explicit request to change partial update config has the highest priority.
      return true;
    }
    /**
     * If a store:
     *     (1) Is being converted to hybrid;
     *     (2) Is not partial update enabled for now;
     *     (3) Does not request to change partial update config;
     * It means partial update is not enabled, and there is no explict intention to change it. In this case, we will
     * check cluster default config based on the replication policy to determine whether to try to enable partial update.
     */
    final boolean shouldEnablePartialUpdateBasedOnClusterConfig =
        storeBeingConvertedToHybrid && (setStore.activeActiveReplicationEnabled
            ? controllerConfig.isEnablePartialUpdateForHybridActiveActiveUserStores()
            : controllerConfig.isEnablePartialUpdateForHybridNonActiveActiveUserStores());
    if (!currentStore.isWriteComputationEnabled() && shouldEnablePartialUpdateBasedOnClusterConfig) {
      LOGGER.info("Controller will try to enable partial update based on cluster config for store: " + storeName);
      /**
       * When trying to turn on partial update based on cluster config, if schema generation failed, we will not fail the
       * whole request, but just do NOT turn on partial update, as other config update should still be respected.
       */
      try {
        addUpdateSchemaForStore(parentHelixAdmin, clusterName, storeName, true);
        setStore.writeComputationEnabled = true;
        partialUpdateConfigChanged = true;
      } catch (Exception e) {
        LOGGER.warn(
            "Caught exception when trying to enable partial update base on cluster config, will not enable partial update for store: "
                + storeName,
            e);
      }
    }
    return partialUpdateConfigChanged;
  }

  public static boolean checkAndMaybeApplyChunkingConfigChange(
      VeniceParentHelixAdmin parentHelixAdmin,
      String clusterName,
      String storeName,
      Optional<Boolean> chunkingRequest,
      UpdateStore setStore) {
    Store currentStore = parentHelixAdmin.getVeniceHelixAdmin().getStore(clusterName, storeName);
    setStore.chunkingEnabled = currentStore.isChunkingEnabled();
    if (chunkingRequest.isPresent()) {
      setStore.chunkingEnabled = chunkingRequest.get();
      // Explicit request to change chunking config has the highest priority.
      return true;
    }
    // If partial update is just enabled, we will by default enable chunking, if no explict request to update chunking
    // config.
    if (!currentStore.isWriteComputationEnabled() && setStore.writeComputationEnabled
        && !currentStore.isChunkingEnabled()) {
      setStore.chunkingEnabled = true;
      return true;
    }
    return false;
  }

  public static boolean checkAndMaybeApplyRmdChunkingConfigChange(
      VeniceParentHelixAdmin parentHelixAdmin,
      String clusterName,
      String storeName,
      Optional<Boolean> rmdChunkingRequest,
      UpdateStore setStore) {
    Store currentStore = parentHelixAdmin.getVeniceHelixAdmin().getStore(clusterName, storeName);
    setStore.rmdChunkingEnabled = currentStore.isRmdChunkingEnabled();
    if (rmdChunkingRequest.isPresent()) {
      setStore.rmdChunkingEnabled = rmdChunkingRequest.get();
      // Explicit request to change RMD chunking config has the highest priority.
      return true;
    }
    // If partial update is just enabled and A/A is enabled, we will by default enable RMD chunking, if no explict
    // request to update RMD chunking config.
    if (!currentStore.isWriteComputationEnabled() && setStore.writeComputationEnabled
        && setStore.activeActiveReplicationEnabled && !currentStore.isRmdChunkingEnabled()) {
      setStore.rmdChunkingEnabled = true;
      return true;
    }
    return false;
  }

  public static void addUpdateSchemaForStore(
      VeniceParentHelixAdmin parentHelixAdmin,
      String clusterName,
      String storeName,
      boolean dryRun) {
    Collection<SchemaEntry> valueSchemaEntries = parentHelixAdmin.getValueSchemas(clusterName, storeName);
    List<SchemaEntry> updateSchemaEntries = new ArrayList<>(valueSchemaEntries.size());
    int maxId = valueSchemaEntries.stream().map(SchemaEntry::getId).max(Comparator.naturalOrder()).get();
    for (SchemaEntry valueSchemaEntry: valueSchemaEntries) {
      try {
        Schema updateSchema = updateSchemaConverter.convertFromValueRecordSchema(valueSchemaEntry.getSchema());
        updateSchemaEntries.add(new SchemaEntry(valueSchemaEntry.getId(), updateSchema));
      } catch (Exception e) {
        // Allow failure in update schema generation in all schema except the latest value schema
        if (valueSchemaEntry.getId() == maxId) {
          throw new VeniceException(
              "For store " + storeName + " cannot generate update schema for value schema ID :"
                  + valueSchemaEntry.getId() + ", top level field probably missing defaults.",
              e);
        }
      }
    }
    // Add update schemas only after all update schema generation succeeded.
    if (dryRun) {
      return;
    }
    for (SchemaEntry updateSchemaEntry: updateSchemaEntries) {
      parentHelixAdmin
          .addDerivedSchema(clusterName, storeName, updateSchemaEntry.getId(), updateSchemaEntry.getSchemaStr());
    }
  }
}
