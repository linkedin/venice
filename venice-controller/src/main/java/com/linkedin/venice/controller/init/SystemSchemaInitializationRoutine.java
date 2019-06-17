package com.linkedin.venice.controller.init;

import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;


public class SystemSchemaInitializationRoutine implements ClusterLeaderInitializationRoutine {
  private static final Logger LOGGER = Logger.getLogger(SystemSchemaInitializationRoutine.class);

  private final AvroProtocolDefinition protocolDefinition;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final VeniceHelixAdmin admin;

  public SystemSchemaInitializationRoutine(AvroProtocolDefinition protocolDefinition, VeniceControllerMultiClusterConfig multiClusterConfigs, VeniceHelixAdmin admin) {
    this.protocolDefinition = protocolDefinition;
    this.multiClusterConfigs = multiClusterConfigs;
    this.admin = admin;
  }

  @Override
  public void execute(String clusterToInit) {
    String intendedCluster = multiClusterConfigs.getSystemSchemaClusterName();
    if (intendedCluster.equals(clusterToInit)) {
      String systemStoreName = getSystemStoreName(protocolDefinition);
      Map<Integer, Schema> protocolSchemaMap = Utils.getAllSchemasFromResources(protocolDefinition);

      // Sanity check to make sure the store is not already created in another cluster.
      try {
        Pair<String, String> clusterNameAndD2 = admin.discoverCluster(systemStoreName);
        String cluster = clusterNameAndD2.getFirst();
        if (!cluster.equals(intendedCluster)) {
          LOGGER.warn("The system store for '" + protocolDefinition.name() + "' already exists in cluster '"
              + cluster + "', which is inconsistent with the config '" + CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME
              + "' which specifies that it should be in cluster '" + intendedCluster
              + "'. Will abort the initialization routine.");
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
          Schema firstSchema = protocolSchemaMap.get(1);
          if (null == firstSchema) {
            throw new VeniceException("Invalid protocol definition: '" + protocolDefinition.name() + "' does not have a version 1");
          }
          String firstKeySchemaStr = "\"int\""; // ignored
          String firstSchemaStr = firstSchema.toString();
          admin.addStore(clusterToInit, systemStoreName, VeniceConstants.SYSTEM_STORE_OWNER, firstKeySchemaStr,
              firstSchemaStr);

          LOGGER.info("System store '" + systemStoreName + "' has been created.");
        } else {
          /**
           * Unexpected, but should not be a problem, so we can still continue with the verification that
           * schemas are properly registered...
           */
          LOGGER.info("Unexpected: The system store '" + systemStoreName + "' was not found in cluster discovery but"
              + " it was then found when querying directly for it...");
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

      for (int schemaVersion = 1; schemaVersion <= protocolDefinition.getCurrentProtocolVersion(); schemaVersion++) {
        Schema schemaInLocalResources = protocolSchemaMap.get(schemaVersion);
        if (null == schemaInLocalResources) {
          throw new VeniceException("Invalid protocol definition: '" + protocolDefinition.name()
              + "' does not have a version " + schemaVersion + " even though that is inferior to the current version ("
              + protocolDefinition.getCurrentProtocolVersion() + ").");
        }

        Schema knownSchema = knownSchemaMap.get(schemaVersion);

        if (null == knownSchema) {
          try {
            admin.addValueSchema(
                clusterToInit,
                systemStoreName,
                schemaInLocalResources.toString(),
                schemaVersion);
          } catch (Exception e) {
            LOGGER.error("Caught Exception when attempting to register '" + protocolDefinition.name()
                + "' schema version '" + schemaVersion + "'. Will bubble up.");
            throw e;
          }
          LOGGER.info("Added new schema v" + schemaVersion + " to '" + systemStoreName + "'.");
        } else {
          boolean schemasAreEqual = knownSchema.equals(schemaInLocalResources);
          if (schemasAreEqual) {
            LOGGER.info("Schema v" + schemaVersion + " in '" + systemStoreName +
                "' is already registered and consistent with the local definition.");
          } else {
            LOGGER.warn("Schema v" + schemaVersion + " in '" + systemStoreName
                + "' is already registered but it is INCONSISTENT with the local definition.\n"
                + "Already registered: " + knownSchema.toString(true) + "\n"
                + "Local definition: " + schemaInLocalResources.toString(true));
          }
        }
      }
    }
  }

  public static String getSystemStoreName(AvroProtocolDefinition protocolDefinition) {
    return String.format(Store.SYSTEM_STORE_FORMAT, protocolDefinition.name());
  }
}
