package com.linkedin.venice.controller;

import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;
import static com.linkedin.venice.utils.AvroSchemaUtils.isValidAvroSchema;

import com.linkedin.avroutil1.compatibility.AvroIncompatibleSchemaException;
import com.linkedin.avroutil1.compatibility.RandomRecordGenerator;
import com.linkedin.avroutil1.compatibility.RecordGenerationConfig;
import com.linkedin.venice.exceptions.InvalidVeniceSchemaException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.meta.ReadWriteSchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.AvroSchemaUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Encapsulates the (child controller) store schema operations: key/value/derived/replication-metadata schema
 * registration, reads, validation, and value-schema cleanup. Extracted from {@link VeniceHelixAdmin} to keep schema
 * concerns cohesive and independently testable. The public {@code Admin} schema methods on {@link VeniceHelixAdmin}
 * delegate here; this class reaches back into the admin only for the cluster-leadership check and per-cluster
 * resources/meta-store accessors.
 */
class StoreSchemaManager {
  private static final Logger LOGGER = LogManager.getLogger(StoreSchemaManager.class);
  private static final int RECORD_COUNT = 10;

  private final VeniceHelixAdmin admin;

  StoreSchemaManager(VeniceHelixAdmin admin) {
    this.admin = admin;
  }

  Optional<Schema> getReplicationMetadataSchema(
      String clusterName,
      String storeName,
      int valueSchemaID,
      int rmdVersionID) {
    admin.checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    SchemaEntry schemaEntry = schemaRepo.getReplicationMetadataSchema(storeName, valueSchemaID, rmdVersionID);
    if (schemaEntry == null) {
      return Optional.empty();
    } else {
      return Optional.of(schemaEntry.getSchema());
    }
  }

  Set<Integer> getInUseValueSchemaIds(String clusterName, String storeName) {
    if (admin.isParent()) {
      return Collections.emptySet();
    }

    Store store = admin.getStore(clusterName, storeName);
    Set<Integer> schemaIds = new HashSet<>();

    // Fetch value schema id used by all existing store version
    for (Version version: store.getVersions()) {
      Map<String, String> map = new HashMap<>(2);
      map.put(KEY_STRING_STORE_NAME, storeName);
      map.put(MetaStoreWriter.KEY_STRING_VERSION_NUMBER, Integer.toString(version.getNumber()));
      StoreMetaKey key = MetaStoreDataType.VALUE_SCHEMAS_WRITTEN_PER_STORE_VERSION.getStoreMetaKey(map);
      StoreMetaValue metaValue = admin.getMetaStoreValue(key, storeName);

      if (metaValue == null) {
        String msg = "Could not find in-use value schema for store " + storeName;
        LOGGER.warn(msg);
        throw new VeniceException(msg);
      }
      schemaIds.addAll(metaValue.storeValueSchemaIdsWrittenPerStoreVersion);
    }
    return schemaIds;
  }

  void deleteValueSchemas(String clusterName, String storeName, Set<Integer> unusedValueSchemaIds) {
    Set<Integer> inuseValueSchemaIds = getInUseValueSchemaIds(clusterName, storeName);
    boolean isCommon = unusedValueSchemaIds.stream().anyMatch(inuseValueSchemaIds::contains);
    if (isCommon) {
      String msg = "For store " + storeName + " cannot delete value schema ids they being used. schema ids: "
          + unusedValueSchemaIds;
      LOGGER.error(msg);
      throw new VeniceException(msg);
    }
    // delete the unused schemas.
    unusedValueSchemaIds.forEach(id -> {
      admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository().removeValueSchema(storeName, id);
      LOGGER.info("Removed value schema with ID " + id + " for store " + storeName);
    });
  }

  SchemaEntry getKeySchema(String clusterName, String storeName) {
    admin.checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    return schemaRepo.getKeySchema(storeName);
  }

  Collection<SchemaEntry> getValueSchemas(String clusterName, String storeName) {
    admin.checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    return schemaRepo.getValueSchemas(storeName);
  }

  Collection<DerivedSchemaEntry> getDerivedSchemas(String clusterName, String storeName) {
    admin.checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    return schemaRepo.getDerivedSchemas(storeName);
  }

  int getValueSchemaId(String clusterName, String storeName, String valueSchemaStr) {
    admin.checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    int schemaId = schemaRepo.getValueSchemaId(storeName, valueSchemaStr);
    // validate the schema as VPJ uses this method to fetch the value schema. Fail loudly if the schema user trying
    // to push is bad.
    if (schemaId != SchemaData.INVALID_VALUE_SCHEMA_ID) {
      AvroSchemaUtils.validateAvroSchemaStr(valueSchemaStr);
    }
    return schemaId;
  }

  GeneratedSchemaID getDerivedSchemaId(String clusterName, String storeName, String schemaStr) {
    admin.checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    GeneratedSchemaID schemaID = schemaRepo.getDerivedSchemaId(storeName, schemaStr);
    // validate the schema as VPJ uses this method to fetch the value schema. Fail loudly if the schema user trying
    // to push is bad.
    if (schemaID.isValid()) {
      AvroSchemaUtils.validateAvroSchemaStr(schemaStr);
    }
    return schemaID;
  }

  SchemaEntry getValueSchema(String clusterName, String storeName, int id) {
    admin.checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    return schemaRepo.getValueSchema(storeName, id);
  }

  private void validateValueSchemaUsingRandomGenerator(String schemaStr, String clusterName, String storeName) {
    VeniceControllerClusterConfig config = admin.getHelixVeniceClusterResources(clusterName).getConfig();
    if (!config.isControllerSchemaValidationEnabled()) {
      return;
    }

    ReadWriteSchemaRepository schemaRepository =
        admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    Collection<SchemaEntry> schemaEntries = schemaRepository.getValueSchemas(storeName);
    AvroSerializer serializer;
    Schema existingSchema = null;

    Schema newSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr);
    RandomRecordGenerator recordGenerator = new RandomRecordGenerator();
    RecordGenerationConfig genConfig = RecordGenerationConfig.newConfig().withAvoidNulls(true);

    for (int i = 0; i < RECORD_COUNT; i++) {
      // check if new records written with new schema can be read using existing older schema
      // Object record =
      Object record = recordGenerator.randomGeneric(newSchema, genConfig);
      serializer = new AvroSerializer<>(newSchema);
      byte[] bytes = serializer.serialize(record);
      for (SchemaEntry schemaEntry: schemaEntries) {
        try {
          existingSchema = schemaEntry.getSchema();
          if (!isValidAvroSchema(existingSchema)) {
            LOGGER.warn("Skip validating ill-formed schema for store: {}", storeName);
            continue;
          }
          RecordDeserializer<Object> deserializer =
              SerializerDeserializerFactory.getAvroGenericDeserializer(newSchema, existingSchema);
          deserializer.deserialize(bytes);
        } catch (Exception e) {
          if (e instanceof AvroIncompatibleSchemaException) {
            LOGGER.warn("Found incompatible avro schema with bad union branch for store: {}", storeName, e);
            continue;
          }
          throw new InvalidVeniceSchemaException(
              "Error while trying to add new schema: " + schemaStr + "  for store " + storeName
                  + " as it is incompatible with existing schema: " + existingSchema,
              e);
        }
      }
    }

    // check if records written with older schema can be read using the new schema
    for (int i = 0; i < RECORD_COUNT; i++) {
      for (SchemaEntry schemaEntry: schemaEntries) {
        try {
          Object record = recordGenerator.randomGeneric(schemaEntry.getSchema(), genConfig);
          serializer = new AvroSerializer(schemaEntry.getSchema());
          byte[] bytes = serializer.serialize(record);
          existingSchema = schemaEntry.getSchema();
          if (!isValidAvroSchema(existingSchema)) {
            LOGGER.warn("Skip validating ill-formed schema for store: {}", storeName);
            continue;
          }
          RecordDeserializer<Object> deserializer =
              SerializerDeserializerFactory.getAvroGenericDeserializer(existingSchema, newSchema);
          deserializer.deserialize(bytes);
        } catch (Exception e) {
          if (e instanceof AvroIncompatibleSchemaException) {
            LOGGER.warn("Found incompatible avro schema with bad union branch for store: {}", storeName, e);
            continue;
          }
          throw new InvalidVeniceSchemaException(
              "Error while trying to add new schema: " + schemaStr + "  for store " + storeName
                  + " as it is incompatible with existing schema: " + existingSchema,
              e);
        }
      }
    }
  }

  /**
   * If {@code storeName} is migrating into {@code clusterName}, accept schemas that fail strict parse only because of
   * {@code validateNumericDefaultValueTypes} (e.g. legacy {@code {"type":"float","default":0}}) by walking the JSON
   * and coercing numeric defaults to the declared field type. The output is strict-parse-clean, which keeps downstream
   * consumers that strict-parse (DaVinci's {@code SchemaUtils.annotateValueSchema}, VPJ, Samza producer) working.
   *
   * Re-strict-parses the coerced output as a defensive check so anything beyond the numeric-default tier (bad names,
   * dangling content, union default not first branch) still fails loudly.
   *
   * For non-migration calls, and for migration calls whose input is already strict-clean, returns the input unchanged —
   * so this can be wired into entry points idempotently.
   *
   * @return possibly-coerced schema string that is guaranteed to pass strict parsing.
   */
  String normalizeSchemaForMigration(String clusterName, String storeName, String schemaStr) {
    ZkStoreConfigAccessor accessor = admin.getStoreConfigAccessor(clusterName);
    if (!accessor.containsConfig(storeName)) {
      return schemaStr;
    }
    StoreConfig cfg = accessor.getStoreConfig(storeName);
    if (cfg == null || !clusterName.equals(cfg.getMigrationDestCluster())) {
      return schemaStr;
    }
    // Migration context. If strict already passes, leave the string unchanged so we don't
    // introduce gratuitous diffs against the source schema.
    try {
      AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr);
      return schemaStr;
    } catch (Exception strictFailure) {
      LOGGER.info(
          "Strict parse failed for store {} migrating into cluster {}; attempting numeric-default coercion.",
          storeName,
          clusterName,
          strictFailure);
      String coerced = AvroSchemaParseUtils.coerceNumericDefaultsToFieldType(schemaStr);
      // Defensive: anything LOOSE_NUMERICS would have been lenient about (union default not first
      // branch, bad names, etc.) is outside the coercion scope and must still fail strict. When it
      // does, surface the *original* strict failure too — it's the one the operator needs to see.
      try {
        AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(coerced);
      } catch (Exception coercedFailure) {
        coercedFailure.addSuppressed(strictFailure);
        throw coercedFailure;
      }
      if (!coerced.equals(schemaStr)) {
        LOGGER.info(
            "Coerced numeric default(s) in value schema for store {} migrating into cluster {}.",
            storeName,
            clusterName);
      }
      return coerced;
    }
  }

  SchemaEntry addValueSchema(
      String clusterName,
      String storeName,
      String valueSchemaStr,
      DirectionalSchemaCompatibilityType expectedCompatibilityType) {
    admin.checkControllerLeadershipFor(clusterName);
    valueSchemaStr = normalizeSchemaForMigration(clusterName, storeName, valueSchemaStr);
    ReadWriteSchemaRepository schemaRepository =
        admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    SchemaEntry schemaEntry = schemaRepository.addValueSchema(storeName, valueSchemaStr, expectedCompatibilityType);
    // For duplicates, addValueSchema returns DUPLICATE_VALUE_SCHEMA_CODE; look up the real id so callers
    // (e.g. SchemaRoutes) always receive a concrete schema id in the response.
    int returnId = schemaEntry.getId() == SchemaData.DUPLICATE_VALUE_SCHEMA_CODE
        ? schemaRepository.getValueSchemaId(storeName, valueSchemaStr)
        : schemaEntry.getId();
    return new SchemaEntry(returnId, valueSchemaStr);
  }

  SchemaEntry addValueSchema(
      String clusterName,
      String storeName,
      String valueSchemaStr,
      int schemaId,
      DirectionalSchemaCompatibilityType compatibilityType) {
    admin.checkControllerLeadershipFor(clusterName);
    valueSchemaStr = normalizeSchemaForMigration(clusterName, storeName, valueSchemaStr);
    ReadWriteSchemaRepository schemaRepository =
        admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    int newValueSchemaId =
        schemaRepository.preCheckValueSchemaAndGetNextAvailableId(storeName, valueSchemaStr, compatibilityType);
    if (newValueSchemaId != SchemaData.DUPLICATE_VALUE_SCHEMA_CODE && newValueSchemaId != schemaId) {
      throw new VeniceException(
          "Inconsistent value schema id between the caller and the local schema repository."
              + " Expected new schema id of " + schemaId + " but the next available id from the local repository is "
              + newValueSchemaId + " for store " + storeName + " in cluster " + clusterName + " Schema: "
              + valueSchemaStr);
    }
    return schemaRepository.addValueSchema(storeName, valueSchemaStr, newValueSchemaId);
  }

  DerivedSchemaEntry addDerivedSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      String derivedSchemaStr) {
    admin.checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepository =
        admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    schemaRepository.addDerivedSchema(storeName, derivedSchemaStr, valueSchemaId);

    return new DerivedSchemaEntry(
        valueSchemaId,
        schemaRepository.getDerivedSchemaId(storeName, derivedSchemaStr).getGeneratedSchemaVersion(),
        derivedSchemaStr);
  }

  DerivedSchemaEntry addDerivedSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      int derivedSchemaId,
      String derivedSchemaStr) {
    admin.checkControllerLeadershipFor(clusterName);
    return admin.getHelixVeniceClusterResources(clusterName)
        .getSchemaRepository()
        .addDerivedSchema(storeName, derivedSchemaStr, valueSchemaId, derivedSchemaId);
  }

  DerivedSchemaEntry removeDerivedSchema(String clusterName, String storeName, int valueSchemaId, int derivedSchemaId) {
    admin.checkControllerLeadershipFor(clusterName);
    return admin.getHelixVeniceClusterResources(clusterName)
        .getSchemaRepository()
        .removeDerivedSchema(storeName, valueSchemaId, derivedSchemaId);
  }

  SchemaEntry addSupersetSchema(
      String clusterName,
      String storeName,
      String valueSchema,
      int valueSchemaId,
      String supersetSchemaStr,
      int supersetSchemaId) {
    admin.checkControllerLeadershipFor(clusterName);
    valueSchema = normalizeSchemaForMigration(clusterName, storeName, valueSchema);
    supersetSchemaStr = normalizeSchemaForMigration(clusterName, storeName, supersetSchemaStr);
    ReadWriteSchemaRepository schemaRepository =
        admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository();

    final SchemaEntry existingSupersetSchemaEntry = schemaRepository.getValueSchema(storeName, supersetSchemaId);
    if (existingSupersetSchemaEntry == null) {
      // If the new superset schema does not exist in the schema repo, add it
      LOGGER.info("Adding superset schema: {} for store: {}", supersetSchemaStr, storeName);
      schemaRepository.addValueSchema(storeName, supersetSchemaStr, supersetSchemaId);

    } else {
      final Schema newSupersetSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(supersetSchemaStr);
      if (!AvroSchemaUtils.compareSchemaIgnoreFieldOrder(existingSupersetSchemaEntry.getSchema(), newSupersetSchema)) {
        throw new VeniceException(
            "Existing schema with id " + existingSupersetSchemaEntry.getId() + " does not match with new schema "
                + supersetSchemaStr);
      }
    }

    // add the value schema
    return schemaRepository.addValueSchema(storeName, valueSchema, valueSchemaId);
  }

  int getValueSchemaIdIgnoreFieldOrder(
      String clusterName,
      String storeName,
      String valueSchemaStr,
      Comparator<Schema> schemaComparator) {
    admin.checkControllerLeadershipFor(clusterName);
    SchemaEntry valueSchemaEntry = new SchemaEntry(SchemaData.UNKNOWN_SCHEMA_ID, valueSchemaStr);

    for (SchemaEntry schemaEntry: getValueSchemas(clusterName, storeName)) {
      if (schemaComparator.compare(schemaEntry.getSchema(), valueSchemaEntry.getSchema()) == 0) {
        return schemaEntry.getId();
      }
    }
    return SchemaData.INVALID_VALUE_SCHEMA_ID;

  }

  int checkPreConditionForAddValueSchemaAndGetNewSchemaId(
      String clusterName,
      String storeName,
      String valueSchemaStr,
      DirectionalSchemaCompatibilityType expectedCompatibilityType) {
    valueSchemaStr = normalizeSchemaForMigration(clusterName, storeName, valueSchemaStr);
    AvroSchemaUtils.validateAvroSchemaStr(valueSchemaStr);
    AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr);
    validateValueSchemaUsingRandomGenerator(valueSchemaStr, clusterName, storeName);
    admin.checkControllerLeadershipFor(clusterName);
    return admin.getHelixVeniceClusterResources(clusterName)
        .getSchemaRepository()
        .preCheckValueSchemaAndGetNextAvailableId(storeName, valueSchemaStr, expectedCompatibilityType);
  }

  int checkPreConditionForAddDerivedSchemaAndGetNewSchemaId(
      String clusterName,
      String storeName,
      int valueSchemaId,
      String derivedSchemaStr) {
    admin.checkControllerLeadershipFor(clusterName);
    return admin.getHelixVeniceClusterResources(clusterName)
        .getSchemaRepository()
        .preCheckDerivedSchemaAndGetNextAvailableId(storeName, valueSchemaId, derivedSchemaStr);
  }

  Collection<RmdSchemaEntry> getReplicationMetadataSchemas(String clusterName, String storeName) {
    admin.checkControllerLeadershipFor(clusterName);
    ReadWriteSchemaRepository schemaRepo = admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    return schemaRepo.getReplicationMetadataSchemas(storeName);
  }

  boolean checkIfValueSchemaAlreadyHasRmdSchema(
      String clusterName,
      String storeName,
      final int valueSchemaID,
      final int replicationMetadataVersionId) {
    admin.checkControllerLeadershipFor(clusterName);
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

  boolean checkIfMetadataSchemaAlreadyPresent(String clusterName, String storeName, RmdSchemaEntry rmdSchemaEntry) {
    admin.checkControllerLeadershipFor(clusterName);
    try {
      Collection<RmdSchemaEntry> schemaEntries = admin.getHelixVeniceClusterResources(clusterName)
          .getSchemaRepository()
          .getReplicationMetadataSchemas(storeName);
      for (RmdSchemaEntry schemaEntry: schemaEntries) {
        if (schemaEntry.equals(rmdSchemaEntry)) {
          return true;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Exception in checkIfMetadataSchemaAlreadyPresent ", e);
    }
    return false;
  }

  RmdSchemaEntry addReplicationMetadataSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      int replicationMetadataVersionId,
      String replicationMetadataSchemaStr) {
    admin.checkControllerLeadershipFor(clusterName);

    RmdSchemaEntry rmdSchemaEntry =
        new RmdSchemaEntry(valueSchemaId, replicationMetadataVersionId, replicationMetadataSchemaStr);
    if (checkIfMetadataSchemaAlreadyPresent(clusterName, storeName, rmdSchemaEntry)) {
      LOGGER.info(
          "Timestamp metadata schema Already present: for store: {} in cluster: {} metadataSchema: {} "
              + "replicationMetadataVersionId: {} valueSchemaId: {}",
          storeName,
          clusterName,
          replicationMetadataSchemaStr,
          replicationMetadataVersionId,
          valueSchemaId);
      return rmdSchemaEntry;
    }

    return admin.getHelixVeniceClusterResources(clusterName)
        .getSchemaRepository()
        .addReplicationMetadataSchema(
            storeName,
            valueSchemaId,
            replicationMetadataSchemaStr,
            replicationMetadataVersionId);
  }

  Schema getSupersetOrLatestValueSchema(String clusterName, Store store) {
    ReadWriteSchemaRepository schemaRepository =
        admin.getHelixVeniceClusterResources(clusterName).getSchemaRepository();
    SchemaEntry existingSchema = schemaRepository.getSupersetOrLatestValueSchema(store.getName());
    return existingSchema == null ? null : existingSchema.getSchema();
  }
}
