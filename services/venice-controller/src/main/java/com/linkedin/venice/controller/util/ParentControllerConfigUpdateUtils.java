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


public class ParentControllerConfigUpdateUtils {
  public static final Logger LOGGER = LogManager.getLogger(ParentControllerConfigUpdateUtils.class);
  public static final WriteComputeSchemaConverter updateSchemaConverter = WriteComputeSchemaConverter.getInstance();

  public static boolean checkAndMaybeApplyPartialUpdateConfig(
      VeniceParentHelixAdmin parentHelixAdmin,
      String clusterName,
      String storeName,
      Optional<Boolean> partialUpdateRequest,
      UpdateStore setStore,
      boolean storeBeingConvertedToHybrid) {
    Store currentStore = parentHelixAdmin.getVeniceHelixAdmin().getStore(clusterName, storeName);
    VeniceControllerClusterConfig clusterConfig =
        parentHelixAdmin.getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getConfig();
    boolean partialUpdateConfigChanged = false;
    if (partialUpdateRequest.isPresent()) {
      if (partialUpdateRequest.get() != currentStore.isWriteComputationEnabled()) {
        partialUpdateConfigChanged = true;
        setStore.writeComputationEnabled = partialUpdateRequest.get();
        if (partialUpdateRequest.get()) {
          // Dry-run generating Write Compute schemas before sending admin messages to enable Write Compute because
          // Write
          // Compute schema generation may fail due to some reasons. If that happens, abort the store update process.
          addUpdateSchemaForStore(parentHelixAdmin, clusterName, storeName, true);
        }
      }
    }
    /**
     * Explicit request to change partial update config has the highest priority.
     */
    if (partialUpdateConfigChanged) {
      return true;
    }
    /**
     * If a store: (1) Is being converted to hybrid (2) Is not partial update enabled for now. (3) Does not change
     * partial update config in this request.
     * It means partial update is not enabled, and there is no explict intention to change it. In this case, we will
     * look up cluster config and based on the replication policy to enable partial update implicitly.
     */
    final boolean shouldEnablePartialUpdateBasedOnClusterConfig =
        storeBeingConvertedToHybrid && (setStore.activeActiveReplicationEnabled
            ? clusterConfig.isEnablePartialUpdateForHybridActiveActiveUserStores()
            : clusterConfig.isEnablePartialUpdateForHybridNonActiveActiveUserStores());
    if (!currentStore.isWriteComputationEnabled() && shouldEnablePartialUpdateBasedOnClusterConfig) {
      LOGGER.info("Controller will try to enable partial update based on cluster config for store: " + storeName);
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
        // Allow failure in write-compute schema generation in all schema except the latest value schema
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
