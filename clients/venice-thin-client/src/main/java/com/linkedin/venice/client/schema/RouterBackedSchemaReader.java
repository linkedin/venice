package com.linkedin.venice.client.schema;

import static com.linkedin.venice.client.store.ClientConfig.DEFAULT_SCHEMA_REFRESH_PERIOD;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.InternalAvroStoreClient;
import com.linkedin.venice.controllerapi.MultiSchemaIdResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class RouterBackedSchemaReader implements SchemaReader {
  public static final String TYPE_KEY_SCHEMA = "key_schema";
  public static final String TYPE_VALUE_SCHEMA = "value_schema";
  public static final String TYPE_ALL_VALUE_SCHEMA_IDS = "all_value_schema_ids";
  public static final String TYPE_UPDATE_SCHEMA = "update_schema";
  private static final Logger LOGGER = LogManager.getLogger(RouterBackedSchemaReader.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static final Function<SchemaEntry, Schema> SCHEMA_EXTRACTOR = SchemaEntry::getSchema;
  private static final Function<SchemaEntry, Integer> SCHEMA_ID_EXTRACTOR = SchemaEntry::getId;
  private static final SchemaEntry NOT_EXIST_VALUE_SCHEMA_ENTRY =
      new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, Schema.create(Schema.Type.NULL));
  private static final DerivedSchemaEntry NOT_EXIST_UPDATE_SCHEMA_ENTRY = new DerivedSchemaEntry(
      SchemaData.INVALID_VALUE_SCHEMA_ID,
      SchemaData.INVALID_VALUE_SCHEMA_ID,
      Schema.create(Schema.Type.NULL));

  private final Optional<Schema> readerSchema;
  private volatile Schema keySchema;
  private final Map<Integer, SchemaEntry> valueSchemaEntryMap = new VeniceConcurrentHashMap<>();
  private final Cache<Schema, Integer> valueSchemaToCanonicalSchemaId = Caffeine.newBuilder().maximumSize(1000).build();
  private final Cache<Schema, Integer> canonicalValueSchemaMapR = Caffeine.newBuilder().maximumSize(1000).build();
  private final Map<Integer, DerivedSchemaEntry> valueSchemaIdToUpdateSchemaEntryMap = new VeniceConcurrentHashMap<>();
  private final AtomicReference<SchemaEntry> latestValueSchemaEntry = new AtomicReference<>();
  private final AtomicInteger supersetSchemaIdAtomic = new AtomicInteger(SchemaData.INVALID_VALUE_SCHEMA_ID);
  private final AtomicBoolean shouldRefreshLatestValueSchemaEntry = new AtomicBoolean(false);
  private final String storeName;
  private final InternalAvroStoreClient storeClient;
  private final boolean externalClient;
  /**
   * In Venice, schemas are Avro schemas that allow setting arbitrary field-level attributes.
   * Internally, Venice may choose to add new schemas (e.g. superset schema) to support some features. However, Venice
   * cannot handle attributes cleanly since they could have some semantic meaning.
   *
   * The @{code preferredSchemaFilter} allows users to specify a predicate that clients will use when deciding which
   * schema are the latest value and update schemas.
   */
  private final Predicate<Schema> preferredSchemaFilter;
  private final ScheduledExecutorService refreshSchemaExecutor;
  private final ScheduledFuture schemaRefreshFuture;
  private final ICProvider icProvider;

  RouterBackedSchemaReader(Supplier<InternalAvroStoreClient> clientSupplier) throws VeniceClientException {
    this(clientSupplier, Optional.empty(), Optional.empty());
  }

  public RouterBackedSchemaReader(
      Supplier<InternalAvroStoreClient> clientSupplier,
      Optional<Schema> readerSchema,
      Optional<Predicate<Schema>> preferredSchemaFilter) {
    this(clientSupplier, readerSchema, preferredSchemaFilter, (ICProvider) null);
  }

  public RouterBackedSchemaReader(
      Supplier<InternalAvroStoreClient> clientSupplier,
      Optional<Schema> readerSchema,
      Optional<Predicate<Schema>> preferredSchemaFilter,
      ICProvider icProvider) {
    this(clientSupplier, readerSchema, preferredSchemaFilter, DEFAULT_SCHEMA_REFRESH_PERIOD, icProvider);
  }

  public RouterBackedSchemaReader(
      Supplier<InternalAvroStoreClient> clientSupplier,
      Optional<Schema> readerSchema,
      Optional<Predicate<Schema>> preferredSchemaFilter,
      Duration valueSchemaRefreshPeriod) {
    this(clientSupplier, readerSchema, preferredSchemaFilter, valueSchemaRefreshPeriod, null);
  }

  public RouterBackedSchemaReader(
      Supplier<InternalAvroStoreClient> clientSupplier,
      Optional<Schema> readerSchema,
      Optional<Predicate<Schema>> preferredSchemaFilter,
      Duration valueSchemaRefreshPeriod,
      ICProvider icProvider) {
    this(clientSupplier.get(), false, readerSchema, preferredSchemaFilter, valueSchemaRefreshPeriod, icProvider);
  }

  public RouterBackedSchemaReader(
      InternalAvroStoreClient storeClient,
      Optional<Schema> readerSchema,
      Optional<Predicate<Schema>> preferredSchemaFilter,
      Duration valueSchemaRefreshPeriod,
      ICProvider icProvider) {
    this(storeClient, true, readerSchema, preferredSchemaFilter, valueSchemaRefreshPeriod, icProvider);
  }

  private RouterBackedSchemaReader(
      InternalAvroStoreClient storeClient,
      boolean externalClient,
      Optional<Schema> readerSchema,
      Optional<Predicate<Schema>> preferredSchemaFilter,
      Duration valueSchemaRefreshPeriod,
      ICProvider icProvider) {
    this.storeClient = storeClient;
    this.externalClient = externalClient;
    this.storeName = this.storeClient.getStoreName();
    this.readerSchema = readerSchema;
    this.preferredSchemaFilter = preferredSchemaFilter.orElse(schema -> false);
    readerSchema.ifPresent(AvroSchemaUtils::validateAvroSchemaStr);
    this.icProvider = icProvider;

    if (valueSchemaRefreshPeriod.toMillis() > 0) {
      this.refreshSchemaExecutor =
          Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("schema-refresh"));
      Runnable schemaRefresher = this::refreshAllSchemas;
      // Fetch schemas once on start up
      schemaRefresher.run();
      // Schedule periodic schema refresh
      this.schemaRefreshFuture = refreshSchemaExecutor.scheduleAtFixedRate(
          schemaRefresher,
          valueSchemaRefreshPeriod.getSeconds(),
          valueSchemaRefreshPeriod.getSeconds(),
          TimeUnit.SECONDS);
    } else {
      this.refreshSchemaExecutor = null;
      this.schemaRefreshFuture = null;
    }
  }

  @Override
  public Schema getKeySchema() {
    if (keySchema != null) {
      return keySchema;
    }
    synchronized (this) {
      if (keySchema != null) {
        return keySchema;
      }
      SchemaEntry keySchemaEntry = fetchKeySchema();
      if (keySchemaEntry == null) {
        throw new VeniceClientException("Key Schema of store: " + this.storeName + " doesn't exist");
      }
      keySchema = keySchemaEntry.getSchema();
      return keySchema;
    }
  }

  @Override
  public Schema getValueSchema(int id) {
    // Should call with refresh as any router transient error can lead to null schema stored in the map
    // `valueSchemaEntryMap`
    SchemaEntry valueSchemaEntry = maybeFetchValueSchemaEntryById(id, true);
    if (!isValidSchemaEntry(valueSchemaEntry)) {
      LOGGER.warn("Got null value schema from Venice for store: {} and id: {}", storeName, id);
      return null;
    }
    return valueSchemaEntry.getSchema();
  }

  @Override
  public Schema getLatestValueSchema() throws VeniceClientException {
    SchemaEntry latest = maybeFetchLatestValueSchemaEntry();
    // Defensive coding, in theory, there will be at least one schema, so latest value schema won't be null after
    // refresh.
    return latest == null ? null : SCHEMA_EXTRACTOR.apply(latest);
  }

  @Override
  public Integer getLatestValueSchemaId() throws VeniceClientException {
    SchemaEntry latest = maybeFetchLatestValueSchemaEntry();
    // Defensive coding, in theory, there will be at least one schema, so latest value schema won't be null after
    // refresh.
    return latest == null ? null : SCHEMA_ID_EXTRACTOR.apply(latest);
  }

  @Override
  public int getValueSchemaId(Schema schema) {
    // Optimization to not compute the canonical form if we have previously seen the value schema.
    Integer cachedValueSchemaId = valueSchemaToCanonicalSchemaId.getIfPresent(schema);
    if (cachedValueSchemaId != null && isValidSchemaId(cachedValueSchemaId)) {
      return cachedValueSchemaId;
    }

    String canonicalSchemaStr = AvroCompatibilityHelper.toParsingForm(schema);
    Schema canonicalSchema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(canonicalSchemaStr);
    Integer valueSchemaId = canonicalValueSchemaMapR.getIfPresent(canonicalSchema);
    if (valueSchemaId == null || !isValidSchemaId(valueSchemaId)) {
      // Perform one time refresh of all schemas.
      updateAllValueSchemas(false);
    }
    valueSchemaId = canonicalValueSchemaMapR.getIfPresent(canonicalSchema);
    if (valueSchemaId != null && isValidSchemaId(valueSchemaId)) {
      return valueSchemaId;
    }
    cacheValueAndCanonicalSchemas(schema, canonicalSchema, NOT_EXIST_UPDATE_SCHEMA_ENTRY.getValueSchemaID());
    throw new VeniceClientException("Could not find schema: " + schema + ". for store " + storeName);
  }

  @Override
  public Schema getUpdateSchema(int valueSchemaId) {
    DerivedSchemaEntry updateSchemaEntry = maybeUpdateAndFetchUpdateSchemaEntryById(valueSchemaId, true);
    if (isValidSchemaEntry(updateSchemaEntry)) {
      return updateSchemaEntry.getSchema();
    }
    LOGGER.warn("Got null update schema from Venice for store: {} and value schema id: {}", storeName, valueSchemaId);
    return null;
  }

  @Override
  public DerivedSchemaEntry getLatestUpdateSchema() {
    SchemaEntry latestValueSchema = maybeFetchLatestValueSchemaEntry();
    if (latestValueSchema == null) {
      LOGGER.warn("Got null latest value schema from Venice for store: {}.", storeName);
      return null;
    }
    DerivedSchemaEntry updateSchemaEntry = maybeUpdateAndFetchUpdateSchemaEntryById(latestValueSchema.getId(), false);
    if (isValidSchemaEntry(updateSchemaEntry)) {
      return updateSchemaEntry;
    }
    LOGGER.warn(
        "Got null update schema from Venice for store: {} for latest value schema id: {}",
        storeName,
        latestValueSchema.getId());
    return null;
  }

  @Override
  public void close() throws IOException {
    if (schemaRefreshFuture != null) {
      schemaRefreshFuture.cancel(true);
    }
    if (refreshSchemaExecutor != null) {
      refreshSchemaExecutor.shutdownNow();
      try {
        refreshSchemaExecutor.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOGGER.warn("Caught InterruptedException while closing the Venice producer ExecutorService", e);
      }
    }
    if (!externalClient) {
      IOUtils.closeQuietly(storeClient, LOGGER::error);
    }
  }

  private SchemaEntry fetchSingleSchema(String requestPath, boolean isValueSchema) throws VeniceClientException {
    SchemaResponse schemaResponse;
    try {
      schemaResponse = fetchSingleSchemaResponse(requestPath);
    } catch (Exception e) {
      LOGGER.error(
          "Caught exception while fetching single schema from path: " + requestPath + " for store: " + storeName,
          e);
      return null;
    }

    if (schemaResponse == null) {
      LOGGER.warn(
          "Got null schema response while fetching single schema from path: " + requestPath + " for store: "
              + storeName);
      return null;
    }

    Schema writerSchema = isValueSchema
        ? preemptiveSchemaVerification(
            AvroCompatibilityHelper.parse(schemaResponse.getSchemaStr()),
            schemaResponse.getSchemaStr(),
            schemaResponse.getId())
        : AvroCompatibilityHelper.parse(schemaResponse.getSchemaStr());

    return new SchemaEntry(schemaResponse.getId(), writerSchema);
  }

  /**
   * This method will try to update all value schemas.
   * Based on Router endpoint availability, it should first try the new method: (1) Fetch all value schema IDs first,
   * (2) then fetch each individual schema one by one. If the forceRefresh is true, it will query all known value schema
   * IDs, otherwise, it will only query value schemas that are not populated in cached schema map.
   * If the new router endpoint /value_schema_ids/ is not available, it should fall back to query all value schemas at once.
   */
  private void updateAllValueSchemas(boolean forceRefresh) {
    try {
      Set<Integer> valueSchemaIdSet;
      try {
        valueSchemaIdSet = fetchAllValueSchemaIdsFromRouter();
      } catch (Exception e) {
        LOGGER.warn(
            "Caught exception when trying to fetch all value schema IDs from router, will fetch all value schema entries instead.",
            e);
        // Fall back to fetch all value schema.
        for (SchemaEntry valueSchemaEntry: fetchAllValueSchemaEntriesFromRouter()) {
          if (!isValidSchemaEntry(valueSchemaEntry)) {
            continue;
          }
          valueSchemaEntryMap.put(valueSchemaEntry.getId(), valueSchemaEntry);
          cacheValueAndCanonicalSchemas(valueSchemaEntry.getSchema(), valueSchemaEntry.getId());
        }
        return;
      }

      for (int id: valueSchemaIdSet) {
        maybeFetchValueSchemaEntryById(id, forceRefresh);
      }
    } catch (Exception ex) {
      throw new VeniceClientException(
          "Got exception while trying to fetch all value schemas for store: " + storeName,
          ex);
    }
  }

  /**
   * Update all the update schemas based on all the known value schema IDs. This method is only used in refresh thread,
   * so we will force update all the update schemas.
   */
  private void updateAllUpdateSchemas() {
    for (Map.Entry<Integer, SchemaEntry> entry: valueSchemaEntryMap.entrySet()) {
      if (!isValidSchemaEntry(entry.getValue())) {
        continue;
      }
      try {
        maybeUpdateAndFetchUpdateSchemaEntryById(entry.getKey(), true);
      } catch (Exception ex) {
        throw new VeniceClientException(
            "Got exception while trying to fetch all update schemas for store: " + storeName,
            ex);
      }
    }
  }

  /**
   * This method updates all value schema entries and latest value schema entry.
   * It will first try to update all value schemas. Based on forceRefresh flag, it will either refresh the unknown schema
   * ID or all value schema IDs.
   * After it refresh all the value schemas, it will derive the latest value schema based on the below story:
   * (1) Scan all the value schema, determine the preferred schema with preferredSchemaFilter.
   * -- (a) If superset schema is preferred schema, it will pick it, otherwise it will pick the one with the highest schema ID.
   * (2) latest value schema can then be determined by the below rule:
   * -- (a) If there is preferred schema, we will use preferred schema as the latest value schema.
   * -- (b) If it has superset schema, we will use superset schema.
   * -- (c) Otherwise, we will use the largest schema id.
   */
  private void updateAllValueSchemaEntriesAndLatestValueSchemaEntry(boolean forceRefresh) {
    // This means all value schemas are refreshed, and we can derive latest value schema ID from local data.
    updateAllValueSchemas(forceRefresh);

    /**
     * All the below logics are to derive the latest value schema ID.
     * It would be great to move the work into controller and ZK storage.
     */
    int supersetSchemaId = supersetSchemaIdAtomic.get();
    int maxPreferredSchemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
    int maxSchemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
    boolean supersetSchemaIsPreferredSchema = false;
    // Compute preferred schema ID.
    for (Map.Entry<Integer, SchemaEntry> entry: valueSchemaEntryMap.entrySet()) {
      if (!isValidSchemaEntry(entry.getValue())) {
        continue;
      }
      int schemaId = entry.getKey();
      if (schemaId > maxSchemaId) {
        maxSchemaId = schemaId;
      }
      if (preferredSchemaFilter.test(entry.getValue().getSchema())) {
        if (schemaId == supersetSchemaId) {
          supersetSchemaIsPreferredSchema = true;
        }
        if (maxPreferredSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID || maxPreferredSchemaId < schemaId) {
          maxPreferredSchemaId = schemaId;
        }
      }
    }
    // Based on preferred schema ID and superset schema ID, compute the latest value schema ID.
    boolean hasSupersetSchema = supersetSchemaId != SchemaData.INVALID_VALUE_SCHEMA_ID;
    boolean hasPreferredSchema = maxPreferredSchemaId != SchemaData.INVALID_VALUE_SCHEMA_ID;
    final int latestSchemaId;
    if (hasPreferredSchema) {
      if (supersetSchemaIsPreferredSchema) {
        latestSchemaId = supersetSchemaId;
      } else {
        latestSchemaId = maxPreferredSchemaId;
      }
    } else {
      if (hasSupersetSchema) {
        latestSchemaId = supersetSchemaId;
      } else {
        latestSchemaId = maxSchemaId;
      }
    }
    if (latestSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
      return;
    }
    // Perform latest value schema entry update.
    if (latestValueSchemaEntry.get() == null || latestSchemaId == supersetSchemaId
        || latestSchemaId > latestValueSchemaEntry.get().getId()) {
      latestValueSchemaEntry.set(valueSchemaEntryMap.get(latestSchemaId));
    }
  }

  private SchemaEntry maybeFetchLatestValueSchemaEntry() {
    SchemaEntry latest = latestValueSchemaEntry.get();
    if (latest == null || shouldRefreshLatestValueSchemaEntry.get()) {
      /**
       * Every time it sees latestValueSchemaEntry is null or the flag to update latest schema entry is set to true,
       * it will try to update it once.
       * The update is expensive, but we expect it to be filled after the first fetch, as each store should have at least
       * one active value schema.
       */
      synchronized (this) {
        if (latest != null && !shouldRefreshLatestValueSchemaEntry.get() && isValidSchemaEntry(latest)) {
          return latest;
        }
        updateAllValueSchemaEntriesAndLatestValueSchemaEntry(false);
        shouldRefreshLatestValueSchemaEntry.compareAndSet(true, false);
        latestValueSchemaEntry.set(latestValueSchemaEntry.get());
        latest = latestValueSchemaEntry.get();
      }
    }
    if (latest == null || !isValidSchemaEntry(latest)) {
      throw new VeniceClientException("Failed to get latest value schema for store: " + storeName);
    }
    return latest;
  }

  /**
   * This method will fetch the value schema by ID, and potentially will update the cached schema map.
   * If the schema:
   * (1) Exists in the cached schema map, we will directly return it.
   * (2) Does not exist in the cached schema map, we will perform one time single schema fetch to request it from router,
   * and update the result in the cached schema map.
   * (3) If the schema was queried previously and not found, we will:
   * -- (a) Return not found, if forceRefresh flag == false.
   * -- (b) Perform one time router query, update and return the result if forceRefresh flag == true.
   */
  private SchemaEntry maybeFetchValueSchemaEntryById(int valueSchemaId, boolean forceRefresh) {
    if (forceRefresh) {
      SchemaEntry oldEntry = valueSchemaEntryMap.get(valueSchemaId);
      // Do not need to refresh if the schema id is already present in the schema repo.
      if (oldEntry != null && isValidSchemaEntry(oldEntry)) {
        cacheValueAndCanonicalSchemas(oldEntry.getSchema(), valueSchemaId);
        return oldEntry;
      }
      SchemaEntry entry = fetchValueSchemaEntryFromRouter(valueSchemaId);
      if (entry == null) {
        valueSchemaEntryMap.put(valueSchemaId, NOT_EXIST_VALUE_SCHEMA_ENTRY);
        return NOT_EXIST_VALUE_SCHEMA_ENTRY;
      } else {
        valueSchemaEntryMap.put(valueSchemaId, entry);
        shouldRefreshLatestValueSchemaEntry.compareAndSet(false, true);
        cacheValueAndCanonicalSchemas(entry.getSchema(), valueSchemaId);
        return entry;
      }
    } else {
      return valueSchemaEntryMap.computeIfAbsent(valueSchemaId, (schemaId) -> {
        SchemaEntry entry = fetchValueSchemaEntryFromRouter(valueSchemaId);
        if (entry == null) {
          return NOT_EXIST_VALUE_SCHEMA_ENTRY;
        }
        // Every time when we fetch a new value schema to cache during non-force-refresh logic, we should try to mark
        // the flag as true.
        shouldRefreshLatestValueSchemaEntry.compareAndSet(false, true);
        cacheValueAndCanonicalSchemas(entry.getSchema(), valueSchemaId);
        return entry;
      });
    }
  }

  /**
   * This method will fetch the latest update schema by ID, and potentially will update the cached schema map.
   * If the schema:
   * (1) Exists in the cached schema map, we will directly return it.
   * (2) Does not exist in the cached schema map, we will perform one time single schema fetch to request it from router,
   * and update the result in the cached schema map.
   * (3) If the schema was queried previously and not found, we will:
   * -- (a) Return not found, if forceRefresh flag == false.
   * -- (b) Perform one time router query, update and return the result if forceRefresh flag == true.
   */
  private DerivedSchemaEntry maybeUpdateAndFetchUpdateSchemaEntryById(int valueSchemaId, boolean forceRefresh) {
    if (forceRefresh) {
      DerivedSchemaEntry derivedSchemaEntry = fetchUpdateSchemaEntryFromRouter(valueSchemaId);
      if (derivedSchemaEntry == null) {
        valueSchemaIdToUpdateSchemaEntryMap.put(valueSchemaId, NOT_EXIST_UPDATE_SCHEMA_ENTRY);
        return NOT_EXIST_UPDATE_SCHEMA_ENTRY;
      } else {
        valueSchemaIdToUpdateSchemaEntryMap.put(valueSchemaId, derivedSchemaEntry);
        return derivedSchemaEntry;
      }
    } else {
      return valueSchemaIdToUpdateSchemaEntryMap.computeIfAbsent(valueSchemaId, (id) -> {
        DerivedSchemaEntry derivedSchemaEntry = fetchUpdateSchemaEntryFromRouter(id);
        if (derivedSchemaEntry == null) {
          return NOT_EXIST_UPDATE_SCHEMA_ENTRY;
        }
        return derivedSchemaEntry;
      });
    }
  }

  private SchemaEntry fetchValueSchemaEntryFromRouter(int valueSchemaId) {
    String requestPath = TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId;
    return fetchSingleSchema(requestPath, true);
  }

  private DerivedSchemaEntry fetchUpdateSchemaEntryFromRouter(int valueSchemaId) {
    String requestPath = TYPE_UPDATE_SCHEMA + "/" + storeName + "/" + valueSchemaId;
    SchemaResponse schemaResponse;
    try {
      schemaResponse = fetchSingleSchemaResponse(requestPath);
    } catch (Exception e) {
      return null;
    }
    if (schemaResponse == null) {
      return null;
    }
    return new DerivedSchemaEntry(
        schemaResponse.getId(),
        schemaResponse.getDerivedSchemaId(),
        schemaResponse.getSchemaStr());
  }

  private Set<Integer> fetchAllValueSchemaIdsFromRouter() {
    String requestPath = TYPE_ALL_VALUE_SCHEMA_IDS + "/" + storeName;
    MultiSchemaIdResponse multiSchemaIdResponse = fetchMultiSchemaIdResponse(requestPath);
    if (multiSchemaIdResponse == null) {
      return Collections.emptySet();
    }
    if (multiSchemaIdResponse.getSuperSetSchemaId() != SchemaData.INVALID_VALUE_SCHEMA_ID) {
      supersetSchemaIdAtomic.set(multiSchemaIdResponse.getSuperSetSchemaId());
    }
    return multiSchemaIdResponse.getSchemaIdSet();
  }

  private List<SchemaEntry> fetchAllValueSchemaEntriesFromRouter() {
    String requestPath = TYPE_VALUE_SCHEMA + "/" + storeName;
    MultiSchemaResponse multiSchemaResponse = fetchMultiSchemaResponse(requestPath);
    if (multiSchemaResponse == null) {
      return Collections.emptyList();
    }
    List<SchemaEntry> valueSchemaEntryList = new ArrayList<>();
    for (MultiSchemaResponse.Schema schema: multiSchemaResponse.getSchemas()) {
      Schema writerSchema = preemptiveSchemaVerification(
          AvroCompatibilityHelper.parse(schema.getSchemaStr()),
          schema.getSchemaStr(),
          schema.getId());
      valueSchemaEntryList.add(new SchemaEntry(schema.getId(), writerSchema));
    }
    supersetSchemaIdAtomic.set(multiSchemaResponse.getSuperSetSchemaId());
    return valueSchemaEntryList;
  }

  private boolean isValidSchemaEntry(SchemaEntry schemaEntry) {
    return schemaEntry.getId() != SchemaData.INVALID_VALUE_SCHEMA_ID;
  }

  private boolean isValidSchemaId(int valueSchemaId) {
    return valueSchemaId != SchemaData.INVALID_VALUE_SCHEMA_ID;
  }

  private MultiSchemaIdResponse fetchMultiSchemaIdResponse(String requestPath) throws VeniceClientException {
    MultiSchemaIdResponse schemaIdResponse;
    byte[] response = executeRouterRequest(requestPath);

    if (response == null) {
      return null;
    }

    try {
      schemaIdResponse = OBJECT_MAPPER.readValue(response, MultiSchemaIdResponse.class);
    } catch (Exception e) {
      throw new VeniceClientException("Got exception while deserializing response", e);
    }
    if (schemaIdResponse.isError()) {
      throw new VeniceClientException(
          "Received an error while fetching schema ID(s) from path: " + requestPath + ", error message: "
              + schemaIdResponse.getError());
    }

    return schemaIdResponse;
  }

  private SchemaResponse fetchSingleSchemaResponse(String requestPath) throws VeniceClientException {
    SchemaResponse schemaResponse;
    byte[] response = executeRouterRequest(requestPath);

    if (response == null) {
      return null;
    }

    try {
      schemaResponse = OBJECT_MAPPER.readValue(response, SchemaResponse.class);
    } catch (Exception e) {
      throw new VeniceClientException("Got exception while deserializing response", e);
    }
    if (schemaResponse.isError()) {
      throw new VeniceClientException(
          "Received an error while fetching schema from path: " + requestPath + ", error message: "
              + schemaResponse.getError());
    }
    if (schemaResponse.getSchemaStr() == null) {
      throw new VeniceClientException("Received bad schema response with null schema string");
    }
    return schemaResponse;
  }

  private MultiSchemaResponse fetchMultiSchemaResponse(String requestPath) throws VeniceClientException {
    MultiSchemaResponse multiSchemaResponse;
    byte[] response = executeRouterRequest(requestPath);

    if (response == null) {
      return null;
    }

    try {
      multiSchemaResponse = OBJECT_MAPPER.readValue(response, MultiSchemaResponse.class);
    } catch (Exception e) {
      throw new VeniceClientException("Got exception while deserializing response", e);
    }
    if (multiSchemaResponse.isError()) {
      throw new VeniceClientException(
          "Received an error while fetching schemas from path: " + requestPath + ", error message: "
              + multiSchemaResponse.getError());
    }
    return multiSchemaResponse;
  }

  private byte[] executeRouterRequest(String requestPath) {
    byte[] response;
    try {
      CompletableFuture<byte[]> responseFuture;
      if (icProvider != null) {
        responseFuture = icProvider.call(this.getClass().getCanonicalName(), () -> storeClient.getRaw(requestPath));
      } else {
        responseFuture = (CompletableFuture<byte[]>) storeClient.getRaw(requestPath);
      }

      response = RetryUtils.executeWithMaxAttempt(
          () -> (responseFuture.get()),
          5,
          Duration.ofMillis(100),
          Collections.singletonList(ExecutionException.class));
    } catch (Exception e) {
      throw new VeniceClientException(
          "Failed to execute request from path " + requestPath + ", storeClient: " + storeClient,
          e);
    }

    if (response == null) {
      LOGGER.warn("Requested data doesn't exist for request path: {}", requestPath);
      return null;
    }

    return response;
  }

  private SchemaEntry fetchKeySchema() throws VeniceClientException {
    String requestPath = TYPE_KEY_SCHEMA + "/" + storeName;
    return fetchSingleSchema(requestPath, false);
  }

  private void refreshAllSchemas() throws VeniceClientException {
    updateAllValueSchemaEntriesAndLatestValueSchemaEntry(true);
    if (!valueSchemaIdToUpdateSchemaEntryMap.isEmpty()) {
      updateAllUpdateSchemas();
    }
  }

  /**
   * Check for any resolver errors with the writer and reader schema if reader schema exists (for specific records).
   * Attempt to fix the resolver error by replacing/adding the writer schema's namespace with the reader's. If the fix
   * worked then the fixed schema is returned else the original schema is returned.
   * @param writerSchema is the original writer schema.
   * @param writerSchemaStr is the string version of the original writer schema.
   * @param schemaId is the corresponding id for the writer schema.
   * @return either a fixed writer schema or the original writer schema.
   */
  private Schema preemptiveSchemaVerification(Schema writerSchema, String writerSchemaStr, int schemaId) {
    if (!readerSchema.isPresent()) {
      return writerSchema;
    }
    Schema alternativeWriterSchema = writerSchema;
    Schema readerSchemaCopy = readerSchema.get();

    try {
      if (AvroSchemaUtils.schemaResolveHasErrors(writerSchema, readerSchemaCopy)) {
        LOGGER.info(
            "Schema error detected during preemptive schema check for store {} "
                + "with writer schema id {} Reader schema: {} Writer schema: {}",
            storeName,
            schemaId,
            readerSchemaCopy,
            writerSchemaStr);
        alternativeWriterSchema =
            AvroSchemaUtils.generateSchemaWithNamespace(writerSchemaStr, readerSchemaCopy.getNamespace());
        if (AvroSchemaUtils.schemaResolveHasErrors(alternativeWriterSchema, readerSchemaCopy)) {
          LOGGER.info("Schema error cannot be resolved with writer schema namespace fix");
          alternativeWriterSchema = writerSchema;
        } else {
          LOGGER.info(
              "Schema error can be resolved with writer schema namespace fix."
                  + " Replacing writer schema for store {} and schema id {}",
              storeName,
              schemaId);
        }
      }
    } catch (Exception e) {
      LOGGER.info("Unknown exception during preemptive schema verification", e);
      alternativeWriterSchema = writerSchema;
    }
    return alternativeWriterSchema;
  }

  private void cacheValueAndCanonicalSchemas(Schema valueSchema, int valueSchemaId) {
    String canonicalSchemaStr = AvroCompatibilityHelper.toParsingForm(valueSchema);
    Schema canonicalSchema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(canonicalSchemaStr);

    cacheValueAndCanonicalSchemas(valueSchema, canonicalSchema, valueSchemaId);
  }

  private void cacheValueAndCanonicalSchemas(Schema valueSchema, Schema canonicalSchema, int valueSchemaId) {
    valueSchemaToCanonicalSchemaId.put(valueSchema, valueSchemaId);

    Integer cachedCanonicalSchemaId = canonicalValueSchemaMapR.getIfPresent(canonicalSchema);
    // Cache schemas if they're previously unseen or have a higher schema ID than the current cached one. This will
    // ensure that the later schemas are preferred over old schemas
    if (cachedCanonicalSchemaId == null || cachedCanonicalSchemaId < valueSchemaId) {
      canonicalValueSchemaMapR.put(canonicalSchema, valueSchemaId);
    }
  }
}
