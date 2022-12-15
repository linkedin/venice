package com.linkedin.venice.init;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class provides the basic framework necessary to create new schema system stores and register previously unknown
 * schemas for schema system stores. The scope of an object of this class is restricted to one schema system store.
 */
public abstract class AbstractSystemSchemaInitializer {
  private static final Logger LOGGER = LogManager.getLogger(AbstractSystemSchemaInitializer.class);

  private final VeniceSystemStoreType systemStore;
  private final Optional<UpdateStoreQueryParams> storeMetadataUpdate;
  private final boolean autoRegisterDerivedComputeSchema;
  private final String schemaStoreName;
  private final String schemaSystemStoreClusterName;

  public static final UpdateStoreQueryParams DEFAULT_USER_SYSTEM_STORE_UPDATE_QUERY_PARAMS =
      new UpdateStoreQueryParams().setHybridRewindSeconds(TimeUnit.DAYS.toSeconds(1)) // 1 day rewind
          .setHybridOffsetLagThreshold(1)
          .setHybridTimeLagThreshold(-1) // Explicitly disable hybrid time lag measurement on system store
          .setLeaderFollowerModel(true)
          .setWriteComputationEnabled(true)
          .setPartitionCount(1);

  public AbstractSystemSchemaInitializer(
      VeniceSystemStoreType systemStore,
      String schemaSystemStoreClusterName,
      UpdateStoreQueryParams storeMetadataUpdate,
      boolean autoRegisterDerivedComputeSchema) {
    this.systemStore = systemStore;
    this.storeMetadataUpdate = Optional.ofNullable(storeMetadataUpdate);
    this.autoRegisterDerivedComputeSchema = autoRegisterDerivedComputeSchema;
    this.schemaStoreName = systemStore.getZkSharedStoreName();
    this.schemaSystemStoreClusterName = schemaSystemStoreClusterName;
  }

  /**
   * Get the name of the schema system store that is being initialized
   * @return name of the schema system store
   */
  public final String getSchemaStoreName() {
    return schemaStoreName;
  }

  /**
   * Get the cluster where the schema system store is expected to be present
   * @return The expected cluster where the schema system store should exist
   */
  public final String getSchemaSystemStoreClusterName() {
    return schemaSystemStoreClusterName;
  }

  /**
   * Sometimes, due to configuration changes or errors, the expected cluster might be different from where the system
   * store actually exists. In such cases, the implementation should use other means to check the cluster where the
   * schema system store actually exists
   * @return The actual cluster where the schema system store should exist
   */
  protected abstract String getActualSystemStoreCluster();

  /**
   * Check whether the schema system store that is being initialized already exists
   * @return {@code true} if the schema system store exists; {@code false} otherwise
   */
  protected abstract boolean doesStoreExist();

  /**
   * A flag denoting whether previously non-existent system stores should be created or not.
   * @return whether previously non-existent system stores should be created or not
   */
  protected abstract boolean shouldCreateMissingSchemaStores();

  /**
   * Create the schema system store.
   * @param keySchema The key schema associated with the schema system store
   * @param firstValueSchema The first value schema associated with the schema system store
   */
  protected abstract void createSchemaStore(Schema keySchema, Schema firstValueSchema);

  /**
   * Update the store configs using the {@link UpdateStoreQueryParams} argument
   * @param updateStoreQueryParams The store config updates to apply to the store
   */
  protected abstract void updateStore(UpdateStoreQueryParams updateStoreQueryParams);

  /**
   * Get the key schema registered with the store
   * @return The registered key schema
   */
  protected abstract SchemaEntry getRegisteredKeySchema();

  /**
   * Get all the value schemas registered with the store
   * @return A collection of all value schemas associated with the store
   */
  protected abstract Collection<SchemaEntry> getAllRegisteredValueSchemas();

  /**
   * Register the specified value schema using the specified value schema version
   * @param schema The value schema to register
   * @param version The schema version to register the value schema at
   */
  protected abstract void registerValueSchema(Schema schema, int version);

  /**
   * Get the derived schema id and associated value schema id if the derived schema has been registered
   * @param derivedSchema The derived schema to get the registered ids
   * @return Return an object with the registered value schema id and the derived schema id if the derived schema is
   *         already registered; otherwise, return the object with the schema ids set to {@link SchemaData.INVALID_VALUE_SCHEMA_ID}
   */
  protected abstract ValueAndDerivedSchemaId getDerivedSchemaInfo(Schema derivedSchema);

  /**
   * Register the derived schema with the specified value schema version
   * @param valueSchemaVersion The value schema version associated with the derived schema
   * @param writeComputeSchema The derived schema to register
   */
  protected abstract void registerDerivedSchema(int valueSchemaVersion, Schema writeComputeSchema);

  /**
   * Run any post registration verification steps
   */
  public abstract boolean isSchemaInitialized();

  /**
   * The entry point for triggering the Schema initialization routine
   */
  public void execute() {
    AvroProtocolDefinition valueSchemaProtocol = systemStore.getValueSchemaProtocol();
    AvroProtocolDefinition keySchemaProtocol = systemStore.getKeySchemaProtocol();
    Map<Integer, Schema> protocolSchemaMap = Utils.getAllSchemasFromResources(valueSchemaProtocol);

    // Sanity check to make sure the store is not already created in another cluster.
    String currSystemStoreCluster = getActualSystemStoreCluster();
    if (currSystemStoreCluster != null) {
      if (!currSystemStoreCluster.equals(schemaSystemStoreClusterName)) {
        LOGGER.warn(
            "The system store for '{}' already exists in cluster '{}', "
                + "which is inconsistent with the config '{}' which specifies that it "
                + "should be in cluster '{}'. Will abort the initialization routine.",
            systemStore.name(),
            currSystemStoreCluster,
            CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME,
            schemaSystemStoreClusterName);
        return;
      }
    } else if (shouldCreateMissingSchemaStores()) {
      /** Young cluster, never knew about system schemas! Let's create the special system store. */
      if (!doesStoreExist()) {
        /**
         * At this point, this branch of the 'if' should always be exercised since cluster discovery thinks
         * this store does not exist.
         */
        Schema firstValueSchema = protocolSchemaMap.get(1);
        if (firstValueSchema == null) {
          throw new VeniceException(
              "Invalid protocol definition: '" + valueSchemaProtocol.name() + "' does not have a version 1");
        }
        createSchemaStore(keySchemaProtocol.getCurrentProtocolVersionSchema(), firstValueSchema);
        // Update the default store config
        storeMetadataUpdate.ifPresent(this::updateStore);

        LOGGER.info("System store '{}' has been created.", schemaStoreName);
      } else {
        /**
         * Unexpected, but should not be a problem, so we can still continue with the verification that
         * schemas are properly registered...
         */
        LOGGER.info(
            "Unexpected: The system store '{}' was not found in cluster discovery but"
                + " it was then found when querying directly for it...",
            schemaStoreName);
      }
    }

    if (keySchemaProtocol.equals(AvroProtocolDefinition.DEFAULT_SYSTEM_STORE_KEY_SCHEMA)) {
      /**
       * Only verify the key schema if it is explicitly specified by the caller, and we don't care
       * about the dummy key schema.
       */
      SchemaEntry keySchemaEntry = getRegisteredKeySchema();
      if (!keySchemaEntry.getSchema().equals(keySchemaProtocol.getCurrentProtocolVersionSchema())) {
        LOGGER.error(
            "Key Schema of '{}' in cluster: {} is already registered but it is "
                + "INCONSISTENT with the local definition.\n" + "Already registered: {}\n" + "Local definition: {}",
            schemaStoreName,
            schemaSystemStoreClusterName,
            keySchemaEntry.getSchema().toString(true),
            keySchemaProtocol.getCurrentProtocolVersionSchema().toString(true));
      }
    }

    /**
     * Old or new, perhaps there are new system schemas the cluster doesn't know about yet...
     * Let's make sure all currently known schemas are registered, excluding any experimental schemas
     * (above the current version).
     */
    Collection<SchemaEntry> schemaEntries = getAllRegisteredValueSchemas();
    Map<Integer, Schema> knownSchemaMap = new HashMap<>();
    schemaEntries.forEach(schemaEntry -> knownSchemaMap.put(schemaEntry.getId(), schemaEntry.getSchema()));

    for (int valueSchemaVersion = 1; valueSchemaVersion <= systemStore.getValueSchemaProtocol()
        .getCurrentProtocolVersion(); valueSchemaVersion++) {
      Schema schemaInLocalResources = protocolSchemaMap.get(valueSchemaVersion);
      if (schemaInLocalResources == null) {
        throw new VeniceException(
            "Invalid protocol definition: '" + systemStore.getValueSchemaProtocol().name()
                + "' does not have a version " + valueSchemaVersion
                + " even though that is inferior to the current version ("
                + systemStore.getValueSchemaProtocol().getCurrentProtocolVersion() + ").");
      }

      Schema knownSchema = knownSchemaMap.get(valueSchemaVersion);

      if (knownSchema == null) {
        try {
          registerValueSchema(schemaInLocalResources, valueSchemaVersion);
        } catch (Exception e) {
          LOGGER.error(
              "Caught Exception when attempting to register '{}' schema version '{}'. Will bubble up.",
              systemStore.name(),
              valueSchemaVersion,
              e);
          throw e;
        }
        LOGGER.info("Added new schema v{} to system store '{}'.", valueSchemaVersion, schemaStoreName);
      } else {
        if (knownSchema.equals(schemaInLocalResources)) {
          LOGGER.info(
              "Schema v{} in system store '{}' is already registered and consistent with the local definition.",
              valueSchemaVersion,
              schemaStoreName);
        } else {
          LOGGER.warn(
              "Schema v{} in system store '{}' is already registered but it is INCONSISTENT with the local definition.\n"
                  + "Already registered: {}\n" + "Local definition: {}",
              valueSchemaVersion,
              schemaStoreName,
              knownSchema.toString(true),
              schemaInLocalResources.toString(true));
        }
      }
      if (autoRegisterDerivedComputeSchema) {
        // Check and register Write Compute schema
        Schema writeComputeSchema =
            WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(schemaInLocalResources);
        ValueAndDerivedSchemaId derivedSchemaInfo = getDerivedSchemaInfo(writeComputeSchema);
        if (derivedSchemaInfo.valueSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
          /**
           * The derived schema doesn't exist right now, try to register it.
           */
          try {
            registerDerivedSchema(valueSchemaVersion, writeComputeSchema);
          } catch (Exception e) {
            LOGGER.error(
                "Caught Exception when attempting to register the derived compute schema for '{}' schema version '{}'. Will bubble up.",
                systemStore.getValueSchemaProtocol().name(),
                valueSchemaVersion,
                e);
            throw e;
          }
          LOGGER.info(
              "Added the derived compute schema for the new schema v{} to system store '{}'.",
              valueSchemaVersion,
              schemaStoreName);
        }
      }
    }
  }

  @Override
  public String toString() {
    return "AbstractSystemSchemaInitializer{" + "systemStore=" + systemStore + '}';
  }

  protected static class ValueAndDerivedSchemaId {
    final int valueSchemaId;
    final int derivedSchemaId;

    public ValueAndDerivedSchemaId(int valueSchemaId, int derivedSchemaId) {
      this.valueSchemaId = valueSchemaId;
      this.derivedSchemaId = derivedSchemaId;
    }
  }
}
