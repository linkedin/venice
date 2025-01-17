package com.linkedin.venice.controller.init;

import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.Utils;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.commons.lang.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class contains the logic to set up system stores. Currently, it only handles RT system stores that are either
 * shared between all clusters (push job details and batch job heartbeat system store) or one system store per cluster
 * (participant store).
 *
 * There are some differences in the initialization of system store
 */
public final class SystemStoreInitializationHelper {
  private static final Logger LOGGER = LogManager.getLogger(SystemStoreInitializationHelper.class);
  // Visible for testing
  static final String DEFAULT_KEY_SCHEMA_STR = "\"int\"";

  // How much time to wait between checks of store updates
  private static Duration delayBetweenStoreUpdateRetries = Duration.ofSeconds(10);

  private SystemStoreInitializationHelper() {
  }

  /**
   * The main function that initializes and configures shared system stores
   * @param clusterName The cluster where the system store exists
   * @param systemStoreName The name of the system store
   * @param protocolDefinition The {@link AvroProtocolDefinition} of the value schemas of the system store
   * @param keySchema The Key Schema of the system store. If it is {@code null}, an int schema is used by default
   * @param updateStoreCheckSupplier A function that decides if an update store operation should be performed on the
   *                                 system store
   * @param updateStoreQueryParams The update store operation that needs to be applied on the store. Can be {@literal null}
   * @param admin {@link com.linkedin.venice.controller.VeniceParentHelixAdmin} if this is the parent controller. {@link com.linkedin.venice.controller.VeniceHelixAdmin} otherwise
   * @param multiClusterConfigs The controller configs
   */
  public static void setupSystemStore(
      String clusterName,
      String systemStoreName,
      AvroProtocolDefinition protocolDefinition,
      Schema keySchema,
      Function<Store, Boolean> updateStoreCheckSupplier,
      UpdateStoreQueryParams updateStoreQueryParams,
      Admin admin,
      VeniceControllerMultiClusterConfig multiClusterConfigs) {
    LOGGER.info("Setting up system store: {} in cluster: {}", systemStoreName, clusterName);
    Map<Integer, Schema> protocolSchemaMap = Utils.getAllSchemasFromResources(protocolDefinition);
    Store store = admin.getStore(clusterName, systemStoreName);
    String keySchemaString = keySchema != null ? keySchema.toString() : DEFAULT_KEY_SCHEMA_STR;
    if (store == null) {
      String firstValueSchema = protocolSchemaMap.get(1).toString();
      admin.createStore(
          clusterName,
          systemStoreName,
          VeniceConstants.SYSTEM_STORE_OWNER,
          keySchemaString,
          firstValueSchema,
          true);
      try {
        store = RetryUtils.executeWithMaxAttempt(() -> {
          Store internalStore = admin.getStore(clusterName, systemStoreName);
          Validate.notNull(internalStore);
          return internalStore;
        }, 5, delayBetweenStoreUpdateRetries, Collections.singletonList(IllegalArgumentException.class));
      } catch (IllegalArgumentException e) {
        throw new VeniceException("Unable to create or fetch store " + systemStoreName);
      }
    } else {
      LOGGER.info("Internal store: {} already exists in cluster: {}", systemStoreName, clusterName);
      if (keySchema != null) {
        /**
         * Only verify the key schema if it is explicitly specified by the caller, and we don't care
         * about the dummy key schema.
         */
        SchemaEntry keySchemaEntry = admin.getKeySchema(clusterName, systemStoreName);
        if (!keySchemaEntry.getSchema().equals(keySchema)) {
          LOGGER.error(
              "Key Schema of '{}' in cluster: {} is already registered but it is "
                  + "INCONSISTENT with the local definition.\n" + "Already registered: {}\n" + "Local definition: {}",
              systemStoreName,
              clusterName,
              keySchemaEntry.getSchema().toString(true),
              keySchema);
        }
      }
    }

    /**
     * Old or new, perhaps there are new system schemas the cluster doesn't know about yet...
     * Let's make sure all currently known schemas are registered, excluding any experimental schemas
     * (above the current version).
     */
    Collection<SchemaEntry> schemaEntries = admin.getValueSchemas(clusterName, systemStoreName);
    Map<Integer, Schema> knownSchemaMap =
        schemaEntries.stream().collect(Collectors.toMap(SchemaEntry::getId, SchemaEntry::getSchema));

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
          admin.addValueSchema(
              clusterName,
              systemStoreName,
              schemaInLocalResources.toString(),
              valueSchemaVersion,
              DirectionalSchemaCompatibilityType.NONE);
        } catch (Exception e) {
          LOGGER.error(
              "Caught Exception when attempting to register '{}' schema version '{}'. Will bubble up.",
              protocolDefinition.name(),
              valueSchemaVersion,
              e);
          throw e;
        }
        LOGGER.info("Added new schema v{} to system store '{}'.", valueSchemaVersion, systemStoreName);
      } else {
        if (knownSchema.equals(schemaInLocalResources)) {
          LOGGER.info(
              "Schema v{} in system store '{}' is already registered and consistent with the local definition.",
              valueSchemaVersion,
              systemStoreName);
        } else {
          LOGGER.warn(
              "Schema v{} in system store '{}' is already registered but it is INCONSISTENT with the local definition.\n"
                  + "Already registered: {}\n" + "Local definition: {}",
              valueSchemaVersion,
              systemStoreName,
              knownSchema.toString(true),
              schemaInLocalResources.toString(true));
        }
      }
    }

    if (updateStoreQueryParams != null && updateStoreCheckSupplier.apply(store)) {
      admin.updateStore(clusterName, systemStoreName, updateStoreQueryParams);

      store = RetryUtils.executeWithMaxAttempt(() -> {
        Store internalStore = admin.getStore(clusterName, systemStoreName);

        // This assumes "updateStoreCheckSupplier" inverts after applying the update. This works for the current set
        // of system store initializations, but might not be right for every case. TODO: Find a better way to do this.
        if (updateStoreCheckSupplier.apply(internalStore)) {
          throw new VeniceException("Unable to update store " + systemStoreName);
        }

        return internalStore;
      }, 5, delayBetweenStoreUpdateRetries, Collections.singletonList(VeniceException.class));

      LOGGER.info("Updated internal store " + systemStoreName + " in cluster " + clusterName);
    }

    long onlineVersionCount =
        store.getVersions().stream().filter(version -> version.getStatus() == VersionStatus.ONLINE).count();
    if (onlineVersionCount == 0) {
      int partitionCount = multiClusterConfigs.getControllerConfig(clusterName).getMinNumberOfPartitions();
      int replicationFactor = admin.getReplicationFactor(clusterName, systemStoreName);
      Version version = admin.incrementVersionIdempotent(
          clusterName,
          systemStoreName,
          Version.guidBasedDummyPushId(),
          partitionCount,
          replicationFactor);
      // SOP is already sent by incrementVersionIdempotent. No need to write again.
      admin.writeEndOfPush(clusterName, systemStoreName, version.getNumber(), false);
      // Wait for version to be created
      RetryUtils.executeWithMaxAttempt(() -> {
        Store internalStore = admin.getStore(clusterName, systemStoreName);

        if (internalStore.getVersions().isEmpty()) {
          throw new VeniceException("Unable to initialize a version for store " + systemStoreName);
        }
      }, 5, delayBetweenStoreUpdateRetries, Collections.singletonList(VeniceException.class));

      LOGGER.info("Created a version for internal store {} in cluster {}", systemStoreName, clusterName);
    }

    LOGGER.info("System store: {} in cluster: {} is set up", systemStoreName, clusterName);
  }

  // Visible for testing
  static void setDelayBetweenStoreUpdateRetries(Duration delayForTests) {
    delayBetweenStoreUpdateRetries = delayForTests;
  }
}
