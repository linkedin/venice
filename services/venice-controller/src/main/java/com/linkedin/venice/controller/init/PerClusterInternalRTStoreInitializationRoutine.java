package com.linkedin.venice.controller.init;

import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PerClusterInternalRTStoreInitializationRoutine implements ClusterLeaderInitializationRoutine {
  private static final Logger LOGGER = LogManager.getLogger(PerClusterInternalRTStoreInitializationRoutine.class);

  private final Function<String, String> clusterToStoreNameSupplier;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final VeniceHelixAdmin admin;
  private final Schema keySchema;
  private final AvroProtocolDefinition protocolDefinition;

  public PerClusterInternalRTStoreInitializationRoutine(
      AvroProtocolDefinition protocolDefinition,
      Function<String, String> clusterToStoreNameSupplier,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      VeniceHelixAdmin admin,
      Schema keySchema) {
    this.protocolDefinition = protocolDefinition;
    this.clusterToStoreNameSupplier = clusterToStoreNameSupplier;
    this.multiClusterConfigs = multiClusterConfigs;
    this.admin = admin;
    this.keySchema = keySchema;
  }

  /**
   * @see ClusterLeaderInitializationRoutine#execute(String)
   */
  @Override
  public void execute(String clusterName) {
    String storeName = clusterToStoreNameSupplier.apply(clusterName);
    Map<Integer, Schema> protocolSchemaMap = Utils.getAllSchemasFromResources(protocolDefinition);
    Store store = admin.getStore(clusterName, storeName);
    if (store == null) {
      String firstValueSchema = protocolSchemaMap.get(1).toString();
      admin.createStore(
          clusterName,
          storeName,
          VeniceConstants.SYSTEM_STORE_OWNER,
          keySchema.toString(),
          firstValueSchema,
          true);
      store = admin.getStore(clusterName, storeName);
      if (store == null) {
        throw new VeniceException("Unable to create or fetch store " + storeName);
      }
    } else {
      LOGGER.info("Internal store {} already exists in cluster {}", storeName, clusterName);
      /**
       * Only verify the key schema if it is explicitly specified by the caller, and we don't care
       * about the dummy key schema.
       */
      SchemaEntry keySchemaEntry = admin.getKeySchema(clusterName, storeName);
      if (!keySchemaEntry.getSchema().equals(keySchema)) {
        LOGGER.error(
            "Key Schema of '{}' in cluster: {} is already registered but it is "
                + "INCONSISTENT with the local definition.\n" + "Already registered: {}\n" + "Local definition: {}",
            storeName,
            clusterName,
            keySchemaEntry.getSchema().toString(true),
            keySchema);
      }
    }

    /**
     * Old or new, perhaps there are new system schemas the cluster doesn't know about yet...
     * Let's make sure all currently known schemas are registered, excluding any experimental schemas
     * (above the current version).
     */
    Collection<SchemaEntry> schemaEntries = admin.getValueSchemas(clusterName, storeName);
    Map<Integer, Schema> knownSchemaMap = new HashMap<>();
    schemaEntries.forEach(schemaEntry -> knownSchemaMap.put(schemaEntry.getId(), schemaEntry.getSchema()));

    for (int valueSchemaVersion = 1; valueSchemaVersion <= protocolDefinition
        .getCurrentProtocolVersion(); valueSchemaVersion++) {
      Schema schemaInLocalResources = protocolSchemaMap.get(valueSchemaVersion);
      if (schemaInLocalResources == null) {
        throw new VeniceException(
            "Invalid protocol definition: '" + protocolDefinition.name() + "' does not have a version "
                + valueSchemaVersion + " even though that is inferior to the current version ("
                + protocolDefinition.getCurrentProtocolVersion() + ").");
      }

      Schema knownSchema = knownSchemaMap.get(valueSchemaVersion);

      if (knownSchema == null) {
        try {
          admin.addValueSchemaInternal(
              clusterName,
              storeName,
              schemaInLocalResources.toString(),
              valueSchemaVersion,
              DirectionalSchemaCompatibilityType.NONE,
              false);
        } catch (Exception e) {
          LOGGER.error(
              "Caught Exception when attempting to register '{}' schema version '{}'. Will bubble up.",
              protocolDefinition.name(),
              valueSchemaVersion,
              e);
          throw e;
        }
        LOGGER.info("Added new schema v{} to system store '{}'.", valueSchemaVersion, storeName);
      } else {
        if (knownSchema.equals(schemaInLocalResources)) {
          LOGGER.info(
              "Schema v{} in system store '{}' is already registered and consistent with the local definition.",
              valueSchemaVersion,
              storeName);
        } else {
          LOGGER.warn(
              "Schema v{} in system store '{}' is already registered but it is INCONSISTENT with the local definition.\n"
                  + "Already registered: {}\n" + "Local definition: {}",
              valueSchemaVersion,
              storeName,
              knownSchema.toString(true),
              schemaInLocalResources.toString(true));
        }
      }
    }

    if (!store.isHybrid()) {
      UpdateStoreQueryParams updateStoreQueryParams = new UpdateStoreQueryParams().setHybridOffsetLagThreshold(100L)
          .setHybridRewindSeconds(TimeUnit.DAYS.toSeconds(7));
      admin.updateStore(clusterName, storeName, updateStoreQueryParams);
      store = admin.getStore(clusterName, storeName);
      if (!store.isHybrid()) {
        throw new VeniceException("Unable to update store " + storeName + " to a hybrid store");
      }
      LOGGER.info("Enabled hybrid for internal store " + storeName + " in cluster " + clusterName);
    }

    if (store.getCurrentVersion() <= 0) {
      int partitionCount = multiClusterConfigs.getControllerConfig(clusterName).getMinNumberOfPartitions();
      int replicationFactor = admin.getReplicationFactor(clusterName, storeName);
      Version version = admin.incrementVersionIdempotent(
          clusterName,
          storeName,
          Version.guidBasedDummyPushId(),
          partitionCount,
          replicationFactor);
      // SOP is already sent by incrementVersionIdempotent. No need to write again.
      admin.writeEndOfPush(clusterName, storeName, version.getNumber(), false);
      store = admin.getStore(clusterName, storeName);
      if (store.getVersions().isEmpty()) {
        throw new VeniceException("Unable to initialize a version for store " + storeName);
      }
      LOGGER.info("Created a version for internal store {} in cluster {}", storeName, clusterName);
    }
  }
}
