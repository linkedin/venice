package com.linkedin.venice.helix;

import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.exceptions.SchemaDuplicateException;
import com.linkedin.venice.exceptions.SchemaIncompatibilityException;
import com.linkedin.venice.exceptions.StoreKeySchemaExistException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadWriteSchemaRepository;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.MetadataSchemaEntry;
import com.linkedin.venice.schema.DerivedSchemaEntry;
import com.linkedin.venice.schema.MetadataVersionId;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.log4j.Logger;

/**
 * This class is used to add schema entries for stores.
 * There are 4 types of schema entries
 *
 * 1. Key schema
 *    ZK Path: ${cluster_name}/Stores/${store_name}/key-schema/1
 *    Each store only has 1 key schema and the schema is immutable.
 *
 * 2. Value schema
 *    ZK Path: ${cluster_name}/Stores/${store_name}/value-schema/${value_schema_id}
 *    Value schemas are evolvable. Stores can have multiple value schemas and each
 *    value schema is forwards/backwards compatible with others.
 * 3. Derived schema
 *    ZK Path: ${cluster_name}/Stores/${store_name}/derived-schema/${value_schema_id}_${derived_schema_id}
 *    Each value schema can have multiple derived schemas. check out
 *    {@link DerivedSchemaEntry} for more
 *    details.
 *
 * 3. Metadata schema
 *  *    ZK Path: ${cluster_name}/Stores/${store_name}/metadata-schema/${value_schema_id}_${metadata_version_id}
 *  *
 * Check out {@link SchemaEntrySerializer} and {@link DerivedSchemaEntrySerializer}
 * to see how schemas are ser-ded.
 *
 * ReadWriteSchemaRepository doesn't cache existing schemas locally and it always
 * queries ZK for currently values. This is a different behavior compared to
 * {@link com.linkedin.venice.meta.ReadOnlyStoreRepository} where values always
 * get cached and future update callbacks are registered.
 *
 * Notice: Users should not instantiate this class elsewhere than in the leader
 * Controller and there should be always only 1 ReadWriteSchemaRepository per cluster.
 * Instantiating multiple ReadWriteSchemaRepository will lead to race conditions in
 * ZK.
 */
public class HelixReadWriteSchemaRepository implements ReadWriteSchemaRepository {
  private Logger logger = Logger.getLogger(HelixReadWriteSchemaRepository.class);

  private final HelixSchemaAccessor accessor;

  // Store repository to check store related info
  private ReadWriteStoreRepository storeRepository;

  private final String clusterName;
  private final Optional<MetaStoreWriter> metaStoreWriter;

  public HelixReadWriteSchemaRepository(ReadWriteStoreRepository storeRepository, ZkClient zkClient,
      HelixAdapterSerializer adapter, String clusterName, Optional<MetaStoreWriter> metaStoreWriter) {
    this.storeRepository = storeRepository;
    this.accessor = new HelixSchemaAccessor(zkClient, adapter, clusterName);
    this.clusterName = clusterName;
    this.metaStoreWriter = metaStoreWriter;
  }

  /**
   * Get key schema for the given store.
   * Fetch from zookeeper directly.
   *
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   * @return
   *    null if key schema doesn't exist;
   *    schema entry if exists;
   */
  @Override
  public SchemaEntry getKeySchema(String storeName) {
    preCheckStoreCondition(storeName);

    return accessor.getKeySchema(storeName);
  }

  /**
   * Get value schema for the given store and schema id.
   * Fetch from zookeeper directly.
   *
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   * @return
   *    null if the schema doesn't exist;
   *    schema entry if exists;
   */
  @Override
  public SchemaEntry getValueSchema(String storeName, int id) {
    preCheckStoreCondition(storeName);

    return accessor.getValueSchema(storeName, String.valueOf(id));
  }

  /**
   * Check whether the given value schema id exists in the given store or not.
   * Fetch from zookeeper directly.
   *
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   * @return
   *    null if the schema doesn't exist;
   *    schema entry if exists;
   */
  @Override
  public boolean hasValueSchema(String storeName, int id) {
    return null != getValueSchema(storeName, id);
  }

  /**
   * This function is used to retrieve value schema id for the given store and schema. Attempts to get the schema that
   * matches exactly. If multiple matching schemas are found then the id of the latest added schema is returned.
   * If the store has auto-register schema from pushjob enabled then if the schema's differ by default value or doc field,
   * they are treated as different schema.
   *
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   * @throws {@link org.apache.avro.SchemaParseException} if the schema is invalid;
   * @return
   *    {@link com.linkedin.venice.schema.SchemaData#INVALID_VALUE_SCHEMA_ID}, if the schema doesn't exist in the given store;
   *    schema id (int), if the schema exists in the given store;
   */
  @Override
  public int getValueSchemaId(String storeName, String valueSchemaStr) {
    preCheckStoreCondition(storeName);

    Store store = storeRepository.getStoreOrThrow(storeName);
    Collection<SchemaEntry> valueSchemas = getValueSchemas(storeName);
    SchemaEntry valueSchemaEntry = new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, valueSchemaStr);

    /**
     * If the store is set to auto-register schema from pushjob, then do exact match (ie compare doc/default value).
     *    if no such schema exists, return INVALID_VALUE_SCHEMA_ID which would trigger auto register the new schema
     *    in the KakfaPushJob#validateValueSchema.
     * else try to do canonical match ignoring doc/default value and return the largest schema id from the list of such matches.
     */
    if (store.isSchemaAutoRegisterFromPushJobEnabled()) {
      List<SchemaEntry> matches = AvroSchemaUtils.filterSchemas(valueSchemaEntry, valueSchemas);

      return matches.isEmpty() ? SchemaData.INVALID_VALUE_SCHEMA_ID : getSchemaEntryWithLargestId(matches).getId();
    }

    return getValueSchemaIdCanonicalMatch(storeName, valueSchemas, valueSchemaEntry);
  }

  private int getValueSchemaIdCanonicalMatch(String storeName, Collection<SchemaEntry> valueSchemas, SchemaEntry valueSchemaEntry) {
    List<SchemaEntry> canonicalizedMatches = AvroSchemaUtils.filterCanonicalizedSchemas(valueSchemaEntry, valueSchemas);
    int schemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
    if (!canonicalizedMatches.isEmpty()) {
      if (canonicalizedMatches.size() == 1) {
        schemaId = canonicalizedMatches.iterator().next().getId();
      } else {
        List<SchemaEntry> exactMatches = AvroSchemaUtils.filterSchemas(valueSchemaEntry, canonicalizedMatches);
        if (exactMatches.isEmpty()) {
          schemaId = getSchemaEntryWithLargestId(canonicalizedMatches).getId();
        } else {
          schemaId = getSchemaEntryWithLargestId(exactMatches).getId();
        }
      }
    }
    return schemaId;
  }

  private SchemaEntry getSchemaEntryWithLargestId(Collection<SchemaEntry> schemas) {
    SchemaEntry largestIdSchema = schemas.iterator().next();
    for (SchemaEntry schema : schemas) {
      if (schema.getId() > largestIdSchema.getId()) {
        largestIdSchema = schema;
      }
    }
    return largestIdSchema;
  }

  /**
   * This function is used to retrieve all the value schemas for the given store.
   * Fetch from zookeeper directly.
   *
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   */
  @Override
  public Collection<SchemaEntry> getValueSchemas(String storeName) {
    preCheckStoreCondition(storeName);

    return accessor.getAllValueSchemas(storeName);
  }

  @Override
  public Collection<DerivedSchemaEntry> getDerivedSchemas(String storeName) {
    preCheckStoreCondition(storeName);

    return accessor.getAllDerivedSchemas(storeName);
  }

  @Override
  public SchemaEntry getLatestValueSchema(String storeName) {
    Collection<SchemaEntry>  valueSchemas = getValueSchemas(storeName);

    Store store = storeRepository.getStoreOrThrow(storeName);
    if (store.getLatestSuperSetValueSchemaId() != SchemaData.INVALID_VALUE_SCHEMA_ID) {
      int latestValueSchemaId = store.getLatestSuperSetValueSchemaId();
      return accessor.getValueSchema(storeName, String.valueOf(latestValueSchemaId));
    }

    int maxValueSchemaId = -1;
    SchemaEntry latestSchema = null;
    for (SchemaEntry schema : valueSchemas) {
      if (schema.getId() > maxValueSchemaId) {
        maxValueSchemaId = schema.getId();
        latestSchema = schema;
      }
    }
    return latestSchema;
  }

  @Override
  public DerivedSchemaEntry getLatestDerivedSchema(String storeName, int valueSchemaId) {
    preCheckStoreCondition(storeName);
    List<DerivedSchemaEntry> derivedSchemaList = getDerivedSchemaMap(storeName).get(valueSchemaId);

    if (derivedSchemaList == null || derivedSchemaList.isEmpty()) {
      throw new VeniceException("No derived schema found corresponding to store: " + storeName);
    }

    return derivedSchemaList.stream().max(Comparator.comparing(DerivedSchemaEntry::getId)).get();
  }

  @Override
  public DerivedSchemaEntry getDerivedSchema(String storeName, int valueSchemaId, int derivedSchemaId) {
    preCheckStoreCondition(storeName);

    String idPairStr = valueSchemaId + HelixSchemaAccessor.MULTIPART_SCHEMA_VERSION_DELIMITER + derivedSchemaId;

    return accessor.getDerivedSchema(storeName, idPairStr);
  }

  /**
   * Set up key schema for the given store.
   *
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException}, if store doesn't exist;
   * @throws {@link com.linkedin.venice.exceptions.StoreKeySchemaExistException}, if key schema already exists;
   * @throws {@link org.apache.avro.SchemaParseException}, if key schema is invalid;
   * @throws {@link com.linkedin.venice.exceptions.VeniceException}, if zookeeper update fails;
   */
  @Override
  public SchemaEntry initKeySchema(String storeName, String schemaStr) {
    preCheckStoreCondition(storeName);
    SchemaEntry keySchemaEntry = new SchemaEntry(Integer.parseInt(HelixSchemaAccessor.KEY_SCHEMA_ID), schemaStr);
    SchemaEntry existingKeySchema = getKeySchema(storeName);

    if (null != existingKeySchema) {
      if (existingKeySchema.equals(keySchemaEntry)) {
        return existingKeySchema;
      } else {
        throw StoreKeySchemaExistException.newExceptionForStore(storeName);
      }
    }

    accessor.createKeySchema(storeName, keySchemaEntry);
    return keySchemaEntry;
  }

  /**
   * Add new value schema for the given store.
   *
   * @throws {@link com.linkedin.venice.exceptions.VeniceNoStoreException} if the store doesn't exist;
   * @throws {@link org.apache.avro.SchemaParseException}, if key schema is invalid;
   * @throws {@link SchemaIncompatibilityException}, if the new schema is
   *  incompatible with the previous value schemas;
   * @throws {@link com.linkedin.venice.exceptions.VeniceException}, if updating zookeeper fails;
   * @return schema entry if the schema is successfully added or already exists.
   */
  @Override
  public synchronized SchemaEntry addValueSchema(String storeName, String schemaStr, DirectionalSchemaCompatibilityType expectedCompatibilityType) {
    return addValueSchema(storeName, schemaStr, preCheckValueSchemaAndGetNextAvailableId(storeName, schemaStr, expectedCompatibilityType));
  }

  @Override
  public synchronized SchemaEntry addValueSchema(String storeName, String schemaStr, int schemaId) {
    SchemaEntry newValueSchemaEntry = new SchemaEntry(schemaId, schemaStr);

    if (schemaId == SchemaData.DUPLICATE_VALUE_SCHEMA_CODE) {
      int dupSchemaId = getNextAvailableSchemaId(getValueSchemas(storeName), newValueSchemaEntry,
          DirectionalSchemaCompatibilityType.FULL);
      if (dupSchemaId == SchemaData.DUPLICATE_VALUE_SCHEMA_CODE) {
        logger.info("Value schema already exists. Skipping adding it to the schema repository. Schema: " + schemaStr);
      } else { // there is some doc field update
        newValueSchemaEntry = new SchemaEntry(dupSchemaId, schemaStr);
        accessor.addValueSchema(storeName, newValueSchemaEntry);
        logger.info(
            "Adding similar schema to the schema repository for doc field update. Schema: " + schemaStr);
      }
    } else {
      accessor.addValueSchema(storeName, newValueSchemaEntry);
    }
    // Check whether meta system store is enabled or not
    Store store = storeRepository.getStoreOrThrow(storeName);
    if (store.isStoreMetaSystemStoreEnabled() && metaStoreWriter.isPresent()) {
      metaStoreWriter.get().writeStoreValueSchemas(storeName, getValueSchemas(storeName));
    }
    return newValueSchemaEntry;
  }

  /**
   * Check if the incoming schema is a valid schema and return the next available schema ID.
   *
   * Venice pre-checks 3 things:
   * 1. If the store is existing or not
   * 2. If the incoming schema contains any reserved fields
   * 3. If the incoming schema is duplicate with current's.
   *
   * @return next available ID if it's a valid schema or {@link SchemaData#DUPLICATE_VALUE_SCHEMA_CODE}
   * if it's a duplicate
   */
  @Override
  public int preCheckValueSchemaAndGetNextAvailableId(String storeName, String valueSchemaStr, DirectionalSchemaCompatibilityType expectedCompatibilityType) {
    preCheckStoreCondition(storeName);

    SchemaEntry valueSchemaEntry = new SchemaEntry(SchemaData.UNKNOWN_SCHEMA_ID, valueSchemaStr);

    // Make sure the value schema doesn't contain the reserved field name in the top level.
    if (valueSchemaEntry.getSchema().getType() == Schema.Type.RECORD &&
        null != valueSchemaEntry.getSchema().getField(VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)) {
      throw new VeniceException("Field name: " + VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME + " is reserved,"
          + " please don't use it in the value schema");
    }

    return getNextAvailableSchemaId(getValueSchemas(storeName), valueSchemaEntry, expectedCompatibilityType);
  }

  @Override
  public int preCheckDerivedSchemaAndGetNextAvailableId(String storeName, int valueSchemaId, String derivedSchemaStr) {
    preCheckStoreCondition(storeName);

    DerivedSchemaEntry derivedSchemaEntry =
        new DerivedSchemaEntry(valueSchemaId, SchemaData.UNKNOWN_SCHEMA_ID, derivedSchemaStr);

    return getNextAvailableSchemaId(getDerivedSchemaMap(storeName).get(valueSchemaId),
        derivedSchemaEntry, DirectionalSchemaCompatibilityType.BACKWARD);
  }

  @Override
  public DerivedSchemaEntry addDerivedSchema(String storeName, String schemaStr, int valueSchemaId) {
    preCheckStoreCondition(storeName);

    DerivedSchemaEntry newDerivedSchemaEntry =
        new DerivedSchemaEntry(valueSchemaId, SchemaData.UNKNOWN_SCHEMA_ID, schemaStr);
    return addDerivedSchema(storeName, schemaStr, valueSchemaId,
        getNextAvailableSchemaId(getDerivedSchemaMap(storeName).get(valueSchemaId), newDerivedSchemaEntry,
            DirectionalSchemaCompatibilityType.NONE));
  }

  @Override
  public DerivedSchemaEntry addDerivedSchema(String storeName, String schemaStr, int valueSchemaId, int derivedSchemaId) {
    DerivedSchemaEntry newDerivedSchemaEntry =
        new DerivedSchemaEntry(valueSchemaId, derivedSchemaId, schemaStr);

    if (derivedSchemaId == SchemaData.DUPLICATE_VALUE_SCHEMA_CODE) {
      logger.info("derived schema is already existing. Skip adding it to repository. Schema: " + schemaStr);
    } else {
      accessor.addDerivedSchema(storeName, newDerivedSchemaEntry);
    }

    return newDerivedSchemaEntry;
  }

  @Override
  public DerivedSchemaEntry removeDerivedSchema(String storeName, int valueSchemaId, int derivedSchemaId) {
    DerivedSchemaEntry derivedSchemaEntry = getDerivedSchema(storeName, valueSchemaId, derivedSchemaId);
    String idPairStr = valueSchemaId + HelixSchemaAccessor.MULTIPART_SCHEMA_VERSION_DELIMITER + derivedSchemaId;

    if (derivedSchemaEntry == null) {
      logger.info("ignore removing derived schema for store: " + storeName + " id pair: " + idPairStr
      + " because it doesn't exist");
      return null;
    }

    accessor.removeDerivedSchema(storeName, idPairStr);
    return derivedSchemaEntry;
  }

  private int getNextAvailableSchemaId(Collection<? extends SchemaEntry> schemaEntries, SchemaEntry newSchemaEntry,
      DirectionalSchemaCompatibilityType expectedCompatibilityType) {
    int newValueSchemaId;
    try {
      if (schemaEntries == null || schemaEntries.isEmpty()) {
        newValueSchemaId = HelixSchemaAccessor.VALUE_SCHEMA_STARTING_ID;
      } else {
        newValueSchemaId = schemaEntries.stream().map(schemaEntry -> {
          if (schemaEntry.equals(newSchemaEntry)
              && !AvroSchemaUtils.hasDocFieldChange(newSchemaEntry.getSchema(), schemaEntry.getSchema())) {
            throw new SchemaDuplicateException(schemaEntry, newSchemaEntry);
          }
          if (!schemaEntry.isNewSchemaCompatible(newSchemaEntry, expectedCompatibilityType)) {
            throw new SchemaIncompatibilityException(schemaEntry, newSchemaEntry);
          }

          return schemaEntry.getId();
        }).max(Integer::compare).get() + 1;
      }
    } catch (SchemaDuplicateException e) {
      logger.warn(e.getMessage());
      newValueSchemaId = SchemaData.DUPLICATE_VALUE_SCHEMA_CODE;
    }

    return newValueSchemaId;
  }

  @Override
  public int getValueSchemaIdIgnoreFieldOrder(String storeName, SchemaEntry newSchemaEntry) {
    for (SchemaEntry schemaEntry : getValueSchemas(storeName)) {
      if (AvroSchemaUtils.compareSchemaIgnoreFieldOrder(schemaEntry.getSchema(), newSchemaEntry.getSchema())) {
        return schemaEntry.getId();
      }
    }
    return SchemaData.INVALID_VALUE_SCHEMA_ID;
  }

  @Override
  public Pair<Integer, Integer> getDerivedSchemaId(String storeName, String derivedSchemaStr) {
    Schema derivedSchema = Schema.parse(derivedSchemaStr);
    for (DerivedSchemaEntry derivedSchemaEntry : getDerivedSchemaMap(storeName).values().stream()
        .flatMap(List::stream).collect(Collectors.toList())) {
      if (derivedSchemaEntry.getSchema().equals(derivedSchema)) {
        return new Pair<>(derivedSchemaEntry.getValueSchemaId(), derivedSchemaEntry.getId());
      }
    }

    return new Pair<>(SchemaData.INVALID_VALUE_SCHEMA_ID, SchemaData.INVALID_VALUE_SCHEMA_ID);
  }

  private Map<Integer, List<DerivedSchemaEntry>> getDerivedSchemaMap(String storeName) {
    preCheckStoreCondition(storeName);

    Map<Integer, List<DerivedSchemaEntry>> derivedSchemaEntryMap = new HashMap<>();
    accessor.getAllDerivedSchemas(storeName).forEach(derivedSchemaEntry ->
      derivedSchemaEntryMap.computeIfAbsent(derivedSchemaEntry.getValueSchemaId(), id -> new ArrayList<>())
          .add(derivedSchemaEntry));

    return derivedSchemaEntryMap;
  }

  @Override
  public Collection<MetadataSchemaEntry> getMetadataSchemas(String storeName) {
    preCheckStoreCondition(storeName);

    return accessor.getAllMetadataSchemas(storeName);
  }

  @Override
  public MetadataSchemaEntry getMetadataSchema(String storeName, int valueSchemaId, int timestampMetadataVersionId) {
    preCheckStoreCondition(storeName);

    String idPairStr = valueSchemaId + HelixSchemaAccessor.MULTIPART_SCHEMA_VERSION_DELIMITER + timestampMetadataVersionId;

    return accessor.getMetadataSchema(storeName, idPairStr);
  }

  @Override
  public MetadataVersionId getMetadataVersionId(String storeName, String metadataSchemaStr) {
    Schema metadataSchema = Schema.parse(metadataSchemaStr);
    for (MetadataSchemaEntry metadataSchemaEntry : getMetadataSchemaMap(storeName).values().stream()
        .flatMap(List::stream).collect(Collectors.toList())) {
      if (metadataSchemaEntry.getSchema().equals(metadataSchema)) {
        return new MetadataVersionId(metadataSchemaEntry.getValueSchemaId(), metadataSchemaEntry.getId());
      }
    }

    return new MetadataVersionId(SchemaData.INVALID_VALUE_SCHEMA_ID, SchemaData.INVALID_VALUE_SCHEMA_ID);
  }

  private Map<Integer, List<MetadataSchemaEntry>> getMetadataSchemaMap(String storeName) {
    preCheckStoreCondition(storeName);

    Map<Integer, List<MetadataSchemaEntry>> metadataSchemaEntryMap = new HashMap<>();
    accessor.getAllMetadataSchemas(storeName).forEach(metadataSchemaEntry ->
        metadataSchemaEntryMap.computeIfAbsent(metadataSchemaEntry.getValueSchemaId(), id -> new ArrayList<>())
            .add(metadataSchemaEntry));

    return metadataSchemaEntryMap;
  }

  @Override
  public MetadataSchemaEntry addMetadataSchema(String storeName, int valueSchemaId, String metadataSchemaStr,  int timestampMetadataVersionId) {
    MetadataSchemaEntry metadataSchemaEntry =
        new MetadataSchemaEntry(valueSchemaId, timestampMetadataVersionId, metadataSchemaStr);

    if (timestampMetadataVersionId == SchemaData.DUPLICATE_VALUE_SCHEMA_CODE) {
      logger.info("metadata schema is already existing. Skip adding it to repository. Schema: " + metadataSchemaStr);
    } else {
      accessor.addMetadataSchema(storeName, metadataSchemaEntry);
    }

    return metadataSchemaEntry;
  }

  @Override
  public void refresh() {
  }

  @Override
  public void clear() {
  }

  private void preCheckStoreCondition(String storeName) {
    if (!storeRepository.hasStore(storeName)) {
      throw new VeniceNoStoreException(storeName);
    }
  }
}
