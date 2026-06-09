package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteUnusedValueSchemas;
import com.linkedin.venice.controller.kafka.protocol.admin.DerivedSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.MetadataSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.SupersetSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.supersetschema.DefaultSupersetSchemaGenerator;
import com.linkedin.venice.controller.supersetschema.SupersetSchemaGenerator;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.AvroSchemaUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Orchestrates schema-related operations (value schema, superset schema, derived/write-compute schema and replication
 * metadata schema) on the parent controller. The orchestration involves sending admin messages and waiting for them to
 * be consumed, plus reading back from the child {@link VeniceHelixAdmin}.
 *
 * <p>This class is intentionally package-private and reaches the parent's package-private methods through the injected
 * {@link VeniceParentHelixAdmin} back-reference, since a same-package reference cannot read the parent's private
 * fields.</p>
 */
class ParentSchemaOrchestrator {
  private static final Logger LOGGER = LogManager.getLogger(ParentSchemaOrchestrator.class);

  private final VeniceParentHelixAdmin parent;
  private final StoreSchemaManager storeSchemaManager;
  private final WriteComputeSchemaConverter writeComputeSchemaConverter;
  private final Optional<SupersetSchemaGenerator> externalSupersetSchemaGenerator;
  private final SupersetSchemaGenerator defaultSupersetSchemaGenerator = new DefaultSupersetSchemaGenerator();

  ParentSchemaOrchestrator(
      VeniceParentHelixAdmin parent,
      StoreSchemaManager storeSchemaManager,
      WriteComputeSchemaConverter writeComputeSchemaConverter,
      Optional<SupersetSchemaGenerator> externalSupersetSchemaGenerator) {
    this.parent = parent;
    this.storeSchemaManager = storeSchemaManager;
    this.writeComputeSchemaConverter = writeComputeSchemaConverter;
    this.externalSupersetSchemaGenerator = externalSupersetSchemaGenerator;
  }

  SupersetSchemaGenerator getSupersetSchemaGenerator(String clusterName) {
    if (externalSupersetSchemaGenerator.isPresent() && parent.getMultiClusterConfigs()
        .getControllerConfig(clusterName)
        .isParentExternalSupersetSchemaGenerationEnabled()) {
      return externalSupersetSchemaGenerator.get();
    }
    return defaultSupersetSchemaGenerator;
  }

  void addSupersetSchemaForStore(String clusterName, String storeName, boolean activeActiveReplicationEnabled) {
    // Generate a superset schema and add it.
    SchemaEntry supersetSchemaEntry = getSupersetSchemaGenerator(clusterName)
        .generateSupersetSchemaFromSchemas(parent.getVeniceHelixAdmin().getValueSchemas(clusterName, storeName));
    final Schema supersetSchema = supersetSchemaEntry.getSchema();
    final int supersetSchemaID = supersetSchemaEntry.getId();
    addValueSchemaEntry(clusterName, storeName, supersetSchema.toString(), supersetSchemaID, true);

    if (activeActiveReplicationEnabled) {
      updateReplicationMetadataSchema(clusterName, storeName, supersetSchema, supersetSchemaID);
    }
  }

  SchemaEntry addValueSchema(
      String clusterName,
      String storeName,
      String newValueSchemaStr,
      DirectionalSchemaCompatibilityType expectedCompatibilityType) {
    parent.acquireAdminMessageLock(clusterName, storeName);
    try {
      newValueSchemaStr = storeSchemaManager.normalizeSchemaForMigration(clusterName, storeName, newValueSchemaStr);
      final int newValueSchemaId = storeSchemaManager.checkPreConditionForAddValueSchemaAndGetNewSchemaId(
          clusterName,
          storeName,
          newValueSchemaStr,
          expectedCompatibilityType);

      /**
       * If we find this is an exactly duplicate schema, return the existing schema id;
       * else add the schema with possible doc field change.
       */
      if (newValueSchemaId == SchemaData.DUPLICATE_VALUE_SCHEMA_CODE) {
        return new SchemaEntry(
            parent.getVeniceHelixAdmin().getValueSchemaId(clusterName, storeName, newValueSchemaStr),
            newValueSchemaStr);
      }

      return addValueSchema(clusterName, storeName, newValueSchemaStr, newValueSchemaId, expectedCompatibilityType);
    } finally {
      parent.releaseAdminMessageLock(clusterName, storeName);
    }
  }

  private SchemaEntry addValueAndSupersetSchemaEntries(
      String clusterName,
      String storeName,
      SchemaEntry newValueSchemaEntry,
      SchemaEntry newSupersetSchemaEntry,
      final boolean isWriteComputationEnabled) {
    validateNewSupersetAndValueSchemaEntries(storeName, clusterName, newValueSchemaEntry, newSupersetSchemaEntry);
    LOGGER.info(
        "Adding value schema {} and superset schema {} to store: {} in cluster: {}",
        newValueSchemaEntry,
        newSupersetSchemaEntry,
        storeName,
        clusterName);

    SupersetSchemaCreation supersetSchemaCreation =
        (SupersetSchemaCreation) AdminMessageType.SUPERSET_SCHEMA_CREATION.getNewInstance();
    supersetSchemaCreation.clusterName = clusterName;
    supersetSchemaCreation.storeName = storeName;
    SchemaMeta valueSchemaMeta = new SchemaMeta();
    valueSchemaMeta.definition = newValueSchemaEntry.getSchemaStr();
    valueSchemaMeta.schemaType = SchemaType.AVRO_1_4.getValue();
    supersetSchemaCreation.valueSchema = valueSchemaMeta;
    supersetSchemaCreation.valueSchemaId = newValueSchemaEntry.getId();

    SchemaMeta supersetSchemaMeta = new SchemaMeta();
    supersetSchemaMeta.definition = newSupersetSchemaEntry.getSchemaStr();
    supersetSchemaMeta.schemaType = SchemaType.AVRO_1_4.getValue();
    supersetSchemaCreation.supersetSchema = supersetSchemaMeta;
    supersetSchemaCreation.supersetSchemaId = newSupersetSchemaEntry.getId();

    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.SUPERSET_SCHEMA_CREATION.getValue();
    message.payloadUnion = supersetSchemaCreation;

    parent.sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
    // Need to add RMD schemas for both new value schema and new superset schema.
    updateReplicationMetadataSchema(
        clusterName,
        storeName,
        newValueSchemaEntry.getSchema(),
        newValueSchemaEntry.getId());
    updateReplicationMetadataSchema(
        clusterName,
        storeName,
        newSupersetSchemaEntry.getSchema(),
        newSupersetSchemaEntry.getId());
    if (isWriteComputationEnabled) {
      Schema newValueWriteComputeSchema =
          writeComputeSchemaConverter.convertFromValueRecordSchema(newValueSchemaEntry.getSchema());
      Schema newSuperSetWriteComputeSchema =
          writeComputeSchemaConverter.convertFromValueRecordSchema(newSupersetSchemaEntry.getSchema());
      addDerivedSchema(clusterName, storeName, newValueSchemaEntry.getId(), newValueWriteComputeSchema.toString());
      addDerivedSchema(
          clusterName,
          storeName,
          newSupersetSchemaEntry.getId(),
          newSuperSetWriteComputeSchema.toString());
    }
    parent.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setLatestSupersetSchemaId(newSupersetSchemaEntry.getId()));
    return newValueSchemaEntry;
  }

  private void validateNewSupersetAndValueSchemaEntries(
      String storeName,
      String clusterName,
      SchemaEntry newValueSchemaEntry,
      SchemaEntry newSupersetSchemaEntry) {
    if (newValueSchemaEntry.getId() == newSupersetSchemaEntry.getId()) {
      throw new IllegalArgumentException(
          String.format(
              "Superset schema ID and value schema ID are expected to be different for store %s in cluster %s. "
                  + "Got ID: %d",
              storeName,
              clusterName,
              newValueSchemaEntry.getId()));
    }
    if (AvroSchemaUtils
        .compareSchemaIgnoreFieldOrder(newValueSchemaEntry.getSchema(), newSupersetSchemaEntry.getSchema())) {
      throw new IllegalArgumentException(
          String.format(
              "Superset and value schemas are expected to be different for store %s in cluster %s. Got schema: %s",
              storeName,
              clusterName,
              newValueSchemaEntry.getSchema()));
    }
  }

  private SchemaEntry addValueSchemaEntry(
      String clusterName,
      String storeName,
      String valueSchemaStr,
      final int newValueSchemaId,
      final boolean doUpdateSupersetSchemaID) {
    LOGGER.info("Adding value schema: {} to store: {} in cluster: {}", valueSchemaStr, storeName, clusterName);

    ValueSchemaCreation valueSchemaCreation =
        (ValueSchemaCreation) AdminMessageType.VALUE_SCHEMA_CREATION.getNewInstance();
    valueSchemaCreation.clusterName = clusterName;
    valueSchemaCreation.storeName = storeName;
    SchemaMeta schemaMeta = new SchemaMeta();
    schemaMeta.definition = valueSchemaStr;
    schemaMeta.schemaType = SchemaType.AVRO_1_4.getValue();
    valueSchemaCreation.schema = schemaMeta;
    valueSchemaCreation.schemaId = newValueSchemaId;

    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.VALUE_SCHEMA_CREATION.getValue();
    message.payloadUnion = valueSchemaCreation;
    parent.sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);

    // defensive code checking
    int actualValueSchemaId = parent.getVeniceHelixAdmin().getValueSchemaId(clusterName, storeName, valueSchemaStr);
    if (actualValueSchemaId != newValueSchemaId) {
      throw new VeniceException(
          "Something bad happens, the expected new value schema id is: " + newValueSchemaId + ", but got: "
              + actualValueSchemaId);
    }

    if (doUpdateSupersetSchemaID) {
      parent.updateStore(
          clusterName,
          storeName,
          new UpdateStoreQueryParams().setLatestSupersetSchemaId(newValueSchemaId));
    }

    return new SchemaEntry(actualValueSchemaId, valueSchemaStr);
  }

  SchemaEntry addValueSchema(
      String clusterName,
      String storeName,
      String newValueSchemaStr,
      int schemaId,
      DirectionalSchemaCompatibilityType expectedCompatibilityType) {
    parent.acquireAdminMessageLock(clusterName, storeName);
    try {
      newValueSchemaStr = storeSchemaManager.normalizeSchemaForMigration(clusterName, storeName, newValueSchemaStr);
      Schema newValueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(newValueSchemaStr);

      final Store store = parent.getVeniceHelixAdmin().getStore(clusterName, storeName);
      // Use the existing superset schema, or the latest value schema if no superset exists, as the base for generating
      // the next superset schema.
      Schema existingValueSchema = storeSchemaManager.getSupersetOrLatestValueSchema(clusterName, store);

      // Update superset schema if:
      // 1. Compute is enabled (existing behavior), OR
      // 2. A superset schema already exists (always keep it updated even if compute is disabled)
      // Check if a superset schema already exists for this store
      final boolean doUpdateSupersetSchemaID;
      boolean supersetSchemaAlreadyExists =
          store.getLatestSuperSetValueSchemaId() != SchemaData.INVALID_VALUE_SCHEMA_ID;
      if (existingValueSchema != null
          && (store.isReadComputationEnabled() || store.isWriteComputationEnabled() || supersetSchemaAlreadyExists)) {
        SupersetSchemaGenerator supersetSchemaGenerator = getSupersetSchemaGenerator(clusterName);
        Schema newSuperSetSchema = supersetSchemaGenerator.generateSupersetSchema(existingValueSchema, newValueSchema);
        String newSuperSetSchemaStr = newSuperSetSchema.toString();
        if (supersetSchemaGenerator.compareSchema(newSuperSetSchema, newValueSchema)) {
          doUpdateSupersetSchemaID = true;

        } else if (supersetSchemaGenerator.compareSchema(newSuperSetSchema, existingValueSchema)) {
          doUpdateSupersetSchemaID = false;

        } else if (store.isSystemStore()) {
          /**
           * Do not register superset schema for system store for now. Because some system stores specify the schema ID
           * explicitly, which may conflict with the superset schema generated internally, the new value schema registration
           * could fail.
           *
           * TODO: Design a long-term plan.
           */
          doUpdateSupersetSchemaID = false;

        } else {
          // Register superset schema only if it does not match with existing or new schema.

          // validate compatibility of the new superset schema
          storeSchemaManager.checkPreConditionForAddValueSchemaAndGetNewSchemaId(
              clusterName,
              storeName,
              newSuperSetSchemaStr,
              expectedCompatibilityType);
          // Check if the superset schema already exists or not. If exists use the same ID, else bump the value ID by
          // one.
          int supersetSchemaId = storeSchemaManager.getValueSchemaIdIgnoreFieldOrder(
              clusterName,
              storeName,
              newSuperSetSchemaStr,
              (s1, s2) -> supersetSchemaGenerator.compareSchema(s1, s2) ? 0 : 1);
          if (supersetSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
            supersetSchemaId = schemaId + 1;
          }
          return addValueAndSupersetSchemaEntries(
              clusterName,
              storeName,
              new SchemaEntry(schemaId, newValueSchema),
              new SchemaEntry(supersetSchemaId, newSuperSetSchema),
              store.isWriteComputationEnabled());
        }
      } else {
        doUpdateSupersetSchemaID = false;
      }
      SchemaEntry addedSchemaEntry =
          addValueSchemaEntry(clusterName, storeName, newValueSchemaStr, schemaId, doUpdateSupersetSchemaID);

      /**
       * if active-active replication is enabled for the store then generate and register the new Replication metadata schema
       * for this newly added value schema.
       */
      if (store.isActiveActiveReplicationEnabled()) {
        updateReplicationMetadataSchema(clusterName, storeName, addedSchemaEntry.getSchema(), addedSchemaEntry.getId());
      }
      if (store.isWriteComputationEnabled()) {
        Schema newWriteComputeSchema =
            writeComputeSchemaConverter.convertFromValueRecordSchema(addedSchemaEntry.getSchema());
        addDerivedSchema(clusterName, storeName, addedSchemaEntry.getId(), newWriteComputeSchema.toString());
      }

      return addedSchemaEntry;
    } finally {
      parent.releaseAdminMessageLock(clusterName, storeName);
    }
  }

  void deleteValueSchemas(String clusterName, String storeName, Set<Integer> unusedValueSchemaIds) {
    Set<Integer> inuseValueSchemaIds = parent.getInUseValueSchemaIds(clusterName, storeName);
    if (inuseValueSchemaIds.isEmpty()) {
      return;
    }
    boolean isCommon = unusedValueSchemaIds.stream().anyMatch(inuseValueSchemaIds::contains);
    if (isCommon) {
      LOGGER
          .error("For store {} cannot delete value schema ids {} as they being used.", storeName, unusedValueSchemaIds);
      return;
    }
    parent.getVeniceHelixAdmin().checkControllerLeadershipFor(clusterName);
    DeleteUnusedValueSchemas deleteValueSchemas =
        (DeleteUnusedValueSchemas) AdminMessageType.DELETE_UNUSED_VALUE_SCHEMA.getNewInstance();
    deleteValueSchemas.setClusterName(clusterName);
    deleteValueSchemas.setStoreName(storeName);
    deleteValueSchemas.setSchemaIds(new ArrayList<>(unusedValueSchemaIds));

    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.DELETE_UNUSED_VALUE_SCHEMA.getValue();
    message.payloadUnion = deleteValueSchemas;

    parent.sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
  }

  DerivedSchemaEntry addDerivedSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      String derivedSchemaStr) {
    parent.acquireAdminMessageLock(clusterName, storeName);
    try {
      int newDerivedSchemaId = storeSchemaManager.checkPreConditionForAddDerivedSchemaAndGetNewSchemaId(
          clusterName,
          storeName,
          valueSchemaId,
          derivedSchemaStr);

      // if we find this is a duplicate schema, return the existing schema id
      if (newDerivedSchemaId == SchemaData.DUPLICATE_VALUE_SCHEMA_CODE) {
        return new DerivedSchemaEntry(
            valueSchemaId,
            parent.getVeniceHelixAdmin()
                .getDerivedSchemaId(clusterName, storeName, derivedSchemaStr)
                .getGeneratedSchemaVersion(),
            derivedSchemaStr);
      }

      LOGGER.info(
          "Adding derived schema: {} to store: {}, version: {} in cluster: {}",
          derivedSchemaStr,
          storeName,
          valueSchemaId,
          clusterName);

      DerivedSchemaCreation derivedSchemaCreation =
          (DerivedSchemaCreation) AdminMessageType.DERIVED_SCHEMA_CREATION.getNewInstance();
      derivedSchemaCreation.clusterName = clusterName;
      derivedSchemaCreation.storeName = storeName;
      SchemaMeta schemaMeta = new SchemaMeta();
      schemaMeta.definition = derivedSchemaStr;
      schemaMeta.schemaType = SchemaType.AVRO_1_4.getValue();
      derivedSchemaCreation.schema = schemaMeta;
      derivedSchemaCreation.valueSchemaId = valueSchemaId;
      derivedSchemaCreation.derivedSchemaId = newDerivedSchemaId;

      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.DERIVED_SCHEMA_CREATION.getValue();
      message.payloadUnion = derivedSchemaCreation;

      parent.sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);

      return new DerivedSchemaEntry(valueSchemaId, newDerivedSchemaId, derivedSchemaStr);
    } finally {
      parent.releaseAdminMessageLock(clusterName, storeName);
    }
  }

  RmdSchemaEntry addReplicationMetadataSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      int replicationMetadataVersionId,
      String replicationMetadataSchemaStr) {
    parent.acquireAdminMessageLock(clusterName, storeName);
    try {
      RmdSchemaEntry rmdSchemaEntry =
          new RmdSchemaEntry(valueSchemaId, replicationMetadataVersionId, replicationMetadataSchemaStr);
      final boolean replicationMetadataSchemaAlreadyPresent =
          storeSchemaManager.checkIfMetadataSchemaAlreadyPresent(clusterName, storeName, rmdSchemaEntry);
      if (replicationMetadataSchemaAlreadyPresent) {
        LOGGER.info(
            "Replication metadata schema already exists for store: {} in cluster: {} metadataSchema: {} "
                + "replicationMetadataVersionId: {} valueSchemaId: {}",
            storeName,
            clusterName,
            replicationMetadataSchemaStr,
            replicationMetadataVersionId,
            valueSchemaId);
        return rmdSchemaEntry;
      }

      LOGGER.info(
          "Adding Replication metadata schema for store: {} in cluster: {} metadataSchema: {} "
              + "replicationMetadataVersionId: {} valueSchemaId: {}",
          storeName,
          clusterName,
          replicationMetadataSchemaStr,
          replicationMetadataVersionId,
          valueSchemaId);

      MetadataSchemaCreation replicationMetadataSchemaCreation =
          (MetadataSchemaCreation) AdminMessageType.REPLICATION_METADATA_SCHEMA_CREATION.getNewInstance();
      replicationMetadataSchemaCreation.clusterName = clusterName;
      replicationMetadataSchemaCreation.storeName = storeName;
      replicationMetadataSchemaCreation.valueSchemaId = valueSchemaId;
      SchemaMeta schemaMeta = new SchemaMeta();
      schemaMeta.definition = replicationMetadataSchemaStr;
      schemaMeta.schemaType = SchemaType.AVRO_1_4.getValue();
      replicationMetadataSchemaCreation.metadataSchema = schemaMeta;
      replicationMetadataSchemaCreation.timestampMetadataVersionId = replicationMetadataVersionId;

      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.REPLICATION_METADATA_SCHEMA_CREATION.getValue();
      message.payloadUnion = replicationMetadataSchemaCreation;

      parent.sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);

      // Be defensive and check that RMD schema has been added indeed. Do a loose validation parsing as stores can have
      // older schemas considered wrong with respect to the current avro version
      final Schema expectedRmdSchema =
          AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(replicationMetadataSchemaStr);
      validateRmdSchemaIsAddedAsExpected(
          clusterName,
          storeName,
          valueSchemaId,
          replicationMetadataVersionId,
          expectedRmdSchema);
      return new RmdSchemaEntry(valueSchemaId, replicationMetadataVersionId, replicationMetadataSchemaStr);
    } catch (Exception e) {
      LOGGER.error(
          "Error when adding replication metadata schema for store: {}, value schema id: {}",
          storeName,
          valueSchemaId,
          e);
      throw e;
    } finally {
      parent.releaseAdminMessageLock(clusterName, storeName);
    }
  }

  private void validateRmdSchemaIsAddedAsExpected(
      String clusterName,
      String storeName,
      int valueSchemaID,
      int rmdVersionID,
      Schema expectedRmdSchema) {
    final Schema addedRmdSchema = parent.getVeniceHelixAdmin()
        .getReplicationMetadataSchema(clusterName, storeName, valueSchemaID, rmdVersionID)
        .orElse(null);
    if (addedRmdSchema == null) {
      throw new VeniceException(
          String.format(
              "No replication metadata schema found for store %s in cluster %s with value "
                  + "schema ID %s and RMD protocol version ID %d",
              storeName,
              clusterName,
              valueSchemaID,
              rmdVersionID));
    }
    if (!AvroSchemaUtils.compareSchemaIgnoreFieldOrder(addedRmdSchema, expectedRmdSchema)) {
      throw new VeniceException(
          String.format(
              "For store %s in cluster %s with value schema ID %d and RMD protocol"
                  + " version ID %d. Expected RMD schema %s. But got RMD schema: %s",
              storeName,
              clusterName,
              valueSchemaID,
              rmdVersionID,
              expectedRmdSchema.toString(true),
              addedRmdSchema.toString(true)));
    }
  }

  void updateReplicationMetadataSchemaForAllValueSchema(String clusterName, String storeName) {
    final Collection<SchemaEntry> valueSchemas = parent.getVeniceHelixAdmin().getValueSchemas(clusterName, storeName);
    for (SchemaEntry valueSchemaEntry: valueSchemas) {
      updateReplicationMetadataSchema(clusterName, storeName, valueSchemaEntry.getSchema(), valueSchemaEntry.getId());
    }
  }

  void updateReplicationMetadataSchema(String clusterName, String storeName, Schema valueSchema, int valueSchemaId) {
    final int rmdVersionId = parent.getRmdVersionID(storeName, clusterName);
    final boolean valueSchemaAlreadyHasRmdSchema =
        storeSchemaManager.checkIfValueSchemaAlreadyHasRmdSchema(clusterName, storeName, valueSchemaId, rmdVersionId);
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
    addReplicationMetadataSchema(clusterName, storeName, valueSchemaId, rmdVersionId, replicationMetadataSchemaStr);
  }
}
