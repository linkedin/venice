package com.linkedin.venice.controller.util;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.supersetschema.SupersetSchemaGenerator;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a utility class for Primary Controller store update logics.
 * Primary controller is the Parent controller in a multi-region deployment, and it is the Child Controller in a single-region deployment.
 * The method here aims to take in current status and request params to determine if certain feature is updated / should
 * be updated based on some customized logics.
 */
public class PrimaryControllerConfigUpdateUtils {
  public static final Logger LOGGER = LogManager.getLogger(PrimaryControllerConfigUpdateUtils.class);
  public static final WriteComputeSchemaConverter UPDATE_SCHEMA_CONVERTER = WriteComputeSchemaConverter.getInstance();

  /**
   * A store can have various schemas that are inferred based on the store's other properties (store configs, existing schemas, etc)
   * This function is expected to register all such inferred schemas and it should be invoked on updates to the store's
   * configs or schemas.
   *
   * This should only be executed in the primary controller. In a multi-region mode, the child controller is expected to
   * get these updates via the admin channel.
   */
  public static void registerInferredSchemas(Admin admin, String clusterName, String storeName) {
    if (!UpdateStoreUtils.isInferredStoreUpdateAllowed(admin, storeName)) {
      return;
    }

    Store store = admin.getStore(clusterName, storeName);

    /**
     * Register new superset schemas if either of the following conditions are met:
     * 1. There is an existing superset schema
     * 2. Read computation is enabled
     * 3. Write computation is enabled
     */
    if (store.isReadComputationEnabled() || store.isWriteComputationEnabled()
        || store.getLatestSuperSetValueSchemaId() != SchemaData.INVALID_VALUE_SCHEMA_ID) {
      addSupersetSchemaForStore(admin, clusterName, store);
    }

    if (store.isWriteComputationEnabled()) {
      // Register partial update schemas (aka derived schemas)
      addUpdateSchemaForStore(admin, clusterName, storeName, false);
    }

    if (store.isActiveActiveReplicationEnabled()) {
      // Register RMD schemas
      updateReplicationMetadataSchemaForAllValueSchema(admin, clusterName, storeName);
    }
  }

  private static void addSupersetSchemaForStore(Admin admin, String clusterName, Store store) {
    String storeName = store.getName();
    SupersetSchemaGenerator supersetSchemaGenerator = admin.getSupersetSchemaGenerator(clusterName);
    SchemaEntry supersetSchemaEntry =
        supersetSchemaGenerator.generateSupersetSchemaFromSchemas(admin.getValueSchemas(clusterName, storeName));
    admin.addSupersetSchema(
        clusterName,
        storeName,
        null,
        SchemaData.INVALID_VALUE_SCHEMA_ID,
        supersetSchemaEntry.getSchemaStr(),
        supersetSchemaEntry.getId());
  }

  public static void addUpdateSchemaForStore(Admin admin, String clusterName, String storeName, boolean dryRun) {
    Collection<SchemaEntry> valueSchemaEntries = admin.getValueSchemas(clusterName, storeName);
    List<SchemaEntry> updateSchemaEntries = new ArrayList<>(valueSchemaEntries.size());
    int maxId = valueSchemaEntries.stream().map(SchemaEntry::getId).max(Comparator.naturalOrder()).get();
    for (SchemaEntry valueSchemaEntry: valueSchemaEntries) {
      try {
        Schema updateSchema = UPDATE_SCHEMA_CONVERTER.convertFromValueRecordSchema(valueSchemaEntry.getSchema());
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
      admin.addDerivedSchema(clusterName, storeName, updateSchemaEntry.getId(), updateSchemaEntry.getSchemaStr());
    }
  }

  public static void updateReplicationMetadataSchemaForAllValueSchema(
      Admin admin,
      String clusterName,
      String storeName) {
    final Collection<SchemaEntry> valueSchemas = admin.getValueSchemas(clusterName, storeName);
    for (SchemaEntry valueSchemaEntry: valueSchemas) {
      updateReplicationMetadataSchema(
          admin,
          clusterName,
          storeName,
          valueSchemaEntry.getSchema(),
          valueSchemaEntry.getId());
    }
  }

  private static void updateReplicationMetadataSchema(
      Admin admin,
      String clusterName,
      String storeName,
      Schema valueSchema,
      int valueSchemaId) {
    final int rmdVersionId = AdminUtils.getRmdVersionID(admin, storeName, clusterName);
    final boolean valueSchemaAlreadyHasRmdSchema =
        checkIfValueSchemaAlreadyHasRmdSchema(admin, clusterName, storeName, valueSchemaId, rmdVersionId);
    if (valueSchemaAlreadyHasRmdSchema) {
      LOGGER.info(
          "Store {} in cluster {} already has a replication metadata schema for its value schema with ID {} and "
              + "replication metadata version ID {}. So skip updating this value schema's RMD schema.",
          storeName,
          clusterName,
          valueSchemaId,
          rmdVersionId);
      return;
    }
    String replicationMetadataSchemaStr =
        RmdSchemaGenerator.generateMetadataSchema(valueSchema, rmdVersionId).toString();
    admin.addReplicationMetadataSchema(
        clusterName,
        storeName,
        valueSchemaId,
        rmdVersionId,
        replicationMetadataSchemaStr);
  }

  private static boolean checkIfValueSchemaAlreadyHasRmdSchema(
      Admin admin,
      String clusterName,
      String storeName,
      final int valueSchemaID,
      final int replicationMetadataVersionId) {
    Collection<RmdSchemaEntry> schemaEntries = admin.getHelixVeniceClusterResources(clusterName)
        .getSchemaRepository()
        .getReplicationMetadataSchemas(storeName);
    for (RmdSchemaEntry rmdSchemaEntry: schemaEntries) {
      if (rmdSchemaEntry.getValueSchemaID() == valueSchemaID
          && rmdSchemaEntry.getId() == replicationMetadataVersionId) {
        return true;
      }
    }
    return false;
  }
}
