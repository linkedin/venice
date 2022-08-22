package com.linkedin.venice.controller.init;

import static com.linkedin.venice.ConfigKeys.*;

import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SystemSchemaInitializationRoutine implements ClusterLeaderInitializationRoutine {
  private static final Logger LOGGER = LogManager.getLogger(SystemSchemaInitializationRoutine.class);
  private static final String DEFAULT_KEY_SCHEMA_STR = "\"int\"";

  private final AvroProtocolDefinition protocolDefinition;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final VeniceHelixAdmin admin;
  private final Optional<Schema> keySchema;
  private final Optional<UpdateStoreQueryParams> storeMetadataUpdate;
  private final boolean autoRegisterDerivedComputeSchema;

  public SystemSchemaInitializationRoutine(
      AvroProtocolDefinition protocolDefinition,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      VeniceHelixAdmin admin) {
    this(protocolDefinition, multiClusterConfigs, admin, Optional.empty(), Optional.empty(), false);
  }

  public SystemSchemaInitializationRoutine(
      AvroProtocolDefinition protocolDefinition,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      VeniceHelixAdmin admin,
      Optional<Schema> keySchema,
      Optional<UpdateStoreQueryParams> storeMetadataUpdate,
      boolean autoRegisterDerivedComputeSchema) {
    this.protocolDefinition = protocolDefinition;
    this.multiClusterConfigs = multiClusterConfigs;
    this.admin = admin;
    this.keySchema = keySchema;
    this.storeMetadataUpdate = storeMetadataUpdate;
    this.autoRegisterDerivedComputeSchema = autoRegisterDerivedComputeSchema;
  }

  /**
   * @see ClusterLeaderInitializationRoutine#execute(String)
   */
  @Override
  public void execute(String clusterToInit) {
    String intendedCluster = multiClusterConfigs.getSystemSchemaClusterName();
    if (intendedCluster.equals(clusterToInit)) {
      String systemStoreName = protocolDefinition.getSystemStoreName();
      Map<Integer, Schema> protocolSchemaMap = Utils.getAllSchemasFromResources(protocolDefinition);

      // Sanity check to make sure the store is not already created in another cluster.
      try {
        Pair<String, String> clusterNameAndD2 = admin.discoverCluster(systemStoreName);
        String currSystemStoreCluster = clusterNameAndD2.getFirst();
        if (!currSystemStoreCluster.equals(intendedCluster)) {
          LOGGER.warn(
              "The system store for '" + protocolDefinition.name() + "' already exists in cluster '"
                  + currSystemStoreCluster + "', which is inconsistent with the config '"
                  + CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME + "' which specifies that it should be in cluster '"
                  + intendedCluster + "'. Will abort the initialization routine.");
          return;
        }

      } catch (VeniceNoStoreException e) {
        /** Young cluster, never knew about system schemas! Let's create the special system store. */
        Store store = admin.getStore(clusterToInit, systemStoreName);
        if (null == store) {
          /**
           * At this point, this branch of the if should always be exercised since cluster discovery thinks
           * this store does not exist.
           */
          Schema firstValueSchema = protocolSchemaMap.get(1);
          if (null == firstValueSchema) {
            throw new VeniceException(
                "Invalid protocol definition: '" + protocolDefinition.name() + "' does not have a version 1");
          }
          String firstKeySchemaStr = keySchema.isPresent() ? keySchema.get().toString() : DEFAULT_KEY_SCHEMA_STR;
          String firstValueSchemaStr = firstValueSchema.toString();
          admin.createStore(
              clusterToInit,
              systemStoreName,
              VeniceConstants.SYSTEM_STORE_OWNER,
              firstKeySchemaStr,
              firstValueSchemaStr,
              true);
          // Update the default store config
          storeMetadataUpdate.ifPresent(
              updateStoreQueryParams -> admin.updateStore(clusterToInit, systemStoreName, updateStoreQueryParams));

          LOGGER.info("System store '" + systemStoreName + "' has been created.");
        } else {
          /**
           * Unexpected, but should not be a problem, so we can still continue with the verification that
           * schemas are properly registered...
           */
          LOGGER.info(
              "Unexpected: The system store '" + systemStoreName + "' was not found in cluster discovery but"
                  + " it was then found when querying directly for it...");
        }
      }

      if (keySchema.isPresent()) {
        /**
         * Only verify the key schema if it is explicitly specified by the caller, and we don't care about the dummy key schema.
         */
        SchemaEntry keySchemaEntry = admin.getKeySchema(clusterToInit, systemStoreName);
        if (!keySchemaEntry.getSchema().equals(keySchema.get())) {
          LOGGER.error(
              "Key Schema of '" + systemStoreName + "' in cluster: " + clusterToInit
                  + " is already registered but it is INCONSISTENT with the local definition.\n"
                  + "Already registered: " + keySchemaEntry.getSchema().toString(true) + "\n" + "Local definition: "
                  + keySchema.get().toString(true));
        }
      }

      /**
       * Old or new, perhaps there are new system schemas the cluster doesn't know about yet...
       * Let's make sure all currently known schemas are registered, excluding any experimental schemas
       * (above the current version).
       */
      Collection<SchemaEntry> schemaEntries = admin.getValueSchemas(clusterToInit, systemStoreName);
      Map<Integer, Schema> knownSchemaMap = new HashMap<>();
      schemaEntries.forEach(schemaEntry -> knownSchemaMap.put(schemaEntry.getId(), schemaEntry.getSchema()));

      for (int valueSchemaVersion = 1; valueSchemaVersion <= protocolDefinition
          .getCurrentProtocolVersion(); valueSchemaVersion++) {
        Schema schemaInLocalResources = protocolSchemaMap.get(valueSchemaVersion);
        if (null == schemaInLocalResources) {
          throw new VeniceException(
              "Invalid protocol definition: '" + protocolDefinition.name() + "' does not have a version "
                  + valueSchemaVersion + " even though that is inferior to the current version ("
                  + protocolDefinition.getCurrentProtocolVersion() + ").");
        }

        Schema knownSchema = knownSchemaMap.get(valueSchemaVersion);

        if (null == knownSchema) {
          try {
            admin.addValueSchema(
                clusterToInit,
                systemStoreName,
                schemaInLocalResources.toString(),
                valueSchemaVersion,
                DirectionalSchemaCompatibilityType.NONE,
                false);
          } catch (Exception e) {
            LOGGER.error(
                "Caught Exception when attempting to register '" + protocolDefinition.name() + "' schema version '"
                    + valueSchemaVersion + "'. Will bubble up.");
            throw e;
          }
          LOGGER.info("Added new schema v" + valueSchemaVersion + " to '" + systemStoreName + "'.");
        } else {
          if (knownSchema.equals(schemaInLocalResources)) {
            LOGGER.info(
                "Schema v" + valueSchemaVersion + " in '" + systemStoreName
                    + "' is already registered and consistent with the local definition.");
          } else {
            LOGGER.warn(
                "Schema v" + valueSchemaVersion + " in '" + systemStoreName
                    + "' is already registered but it is INCONSISTENT with the local definition.\n"
                    + "Already registered: " + knownSchema.toString(true) + "\n" + "Local definition: "
                    + schemaInLocalResources.toString(true));
          }
        }
        if (autoRegisterDerivedComputeSchema) {
          // Check and register Write Compute schema
          String writeComputeSchema =
              WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(schemaInLocalResources).toString();
          Pair<Integer, Integer> derivedSchemaInfo =
              admin.getDerivedSchemaId(clusterToInit, systemStoreName, writeComputeSchema);
          if (derivedSchemaInfo.getFirst() == SchemaData.INVALID_VALUE_SCHEMA_ID) {
            /**
             * The derived schema doesn't exist right now, try to register it.
             */
            try {
              admin.addDerivedSchema(clusterToInit, systemStoreName, valueSchemaVersion, writeComputeSchema);
            } catch (Exception e) {
              LOGGER.error(
                  "Caught Exception when attempting to register the derived compute schema for '"
                      + protocolDefinition.name() + "' schema version '" + valueSchemaVersion + "'. Will bubble up.");
              throw e;
            }
            LOGGER.info(
                "Added the derived compute schema for the new schema v" + valueSchemaVersion + " to '" + systemStoreName
                    + "'.");
          }
        }
      }
    }
  }

  @Override
  public String toString() {
    return "SystemSchemaInitializationRoutine{" + "protocolDefinition=" + protocolDefinition + '}';
  }
}
