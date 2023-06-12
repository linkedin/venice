package com.linkedin.venice.client.schema;

import static com.linkedin.venice.client.store.ClientConfig.DEFAULT_SCHEMA_REFRESH_PERIOD;
import static com.linkedin.venice.schema.AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.InternalAvroStoreClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.concurrent.ConcurrencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
  public static final String TYPE_GET_UPDATE_SCHEMA = "update_schema";
  private static final Logger LOGGER = LogManager.getLogger(RouterBackedSchemaReader.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  private final Optional<Schema> readerSchema;
  private volatile Schema keySchema;
  private final Map<Integer, Schema> valueSchemaMap = new VeniceConcurrentHashMap<>();
  private final Map<Schema, Integer> valueSchemaMapR = new VeniceConcurrentHashMap<>();
  private final AtomicReference<SchemaEntry> latestValueSchemaEntry = new AtomicReference<>();
  private final AtomicReference<Boolean> loadUpdateSchemas = new AtomicReference<>(false);

  private final Map<Integer, DerivedSchemaEntry> valueSchemaIdToUpdateSchemaMap = new VeniceConcurrentHashMap<>();

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
  private final AtomicReference<Integer> maxValueSchemaId = new AtomicReference<>(SchemaData.INVALID_VALUE_SCHEMA_ID);

  RouterBackedSchemaReader(Supplier<AbstractAvroStoreClient> clientSupplier) throws VeniceClientException {
    this(clientSupplier, Optional.empty(), Optional.empty());
  }

  public RouterBackedSchemaReader(
      Supplier<AbstractAvroStoreClient> clientSupplier,
      Optional<Schema> readerSchema,
      Optional<Predicate<Schema>> preferredSchemaFilter) {
    this(clientSupplier, readerSchema, preferredSchemaFilter, (ICProvider) null);
  }

  public RouterBackedSchemaReader(
      Supplier<AbstractAvroStoreClient> clientSupplier,
      Optional<Schema> readerSchema,
      Optional<Predicate<Schema>> preferredSchemaFilter,
      ICProvider icProvider) {
    this(clientSupplier, readerSchema, preferredSchemaFilter, DEFAULT_SCHEMA_REFRESH_PERIOD, icProvider);
  }

  public RouterBackedSchemaReader(
      Supplier<AbstractAvroStoreClient> clientSupplier,
      Optional<Schema> readerSchema,
      Optional<Predicate<Schema>> preferredSchemaFilter,
      Duration valueSchemaRefreshPeriod) {
    this(clientSupplier, readerSchema, preferredSchemaFilter, valueSchemaRefreshPeriod, null);
  }

  public RouterBackedSchemaReader(
      Supplier<AbstractAvroStoreClient> clientSupplier,
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
      this.schemaRefreshFuture = refreshSchemaExecutor.scheduleAtFixedRate(
          () -> this.ensureSchemasAreRefreshed(loadUpdateSchemas.get(), true),
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
    Schema valueSchema = valueSchemaMap.get(id);
    if (valueSchema != null) {
      return valueSchema;
    }

    ensureSchemasAreRefreshed(false, true);
    valueSchema = valueSchemaMap.get(id);
    if (valueSchema == null) {
      LOGGER.warn("Got null value schema from Venice for store: {} and id: {}", storeName, id);
    }
    return valueSchema;
  }

  private static final Function<SchemaEntry, Schema> SCHEMA_EXTRACTOR = schemaEntry -> schemaEntry.getSchema();

  @Override
  public Schema getLatestValueSchema() throws VeniceClientException {
    ensureSchemasAreRefreshed(false, false);
    SchemaEntry latest = latestValueSchemaEntry.get();
    return latest == null ? null : SCHEMA_EXTRACTOR.apply(latest);
  }

  private static final Function<SchemaEntry, Integer> SCHEMA_ID_EXTRACTOR = schemaEntry -> schemaEntry.getId();

  @Override
  public Integer getLatestValueSchemaId() throws VeniceClientException {
    ensureSchemasAreRefreshed(false, false);
    SchemaEntry latest = latestValueSchemaEntry.get();
    return latest == null ? null : SCHEMA_ID_EXTRACTOR.apply(latest);
  }

  @Override
  public int getValueSchemaId(Schema schema) {
    Integer valueSchemaId = valueSchemaMapR.get(schema);
    if (valueSchemaId != null) {
      return valueSchemaId;
    }

    ensureSchemasAreRefreshed(false, true);
    if (!valueSchemaMapR.containsKey(schema)) {
      throw new VeniceClientException("Could not find schema: " + schema + ". for store " + storeName);
    }
    return valueSchemaMapR.get(schema);
  }

  @Override
  public Schema getUpdateSchema(int valueSchemaId) {
    DerivedSchemaEntry updateSchema = valueSchemaIdToUpdateSchemaMap.get(valueSchemaId);
    if (updateSchema != null) {
      return updateSchema.getSchema();
    }

    ensureSchemasAreRefreshed(true, true);
    updateSchema = valueSchemaIdToUpdateSchemaMap.get(valueSchemaId);
    if (updateSchema == null) {
      LOGGER.warn("Got null update schema from Venice for store: {} and value schema id: {}", storeName, valueSchemaId);
      return null;
    }
    return updateSchema.getSchema();
  }

  @Override
  public DerivedSchemaEntry getLatestUpdateSchema() {
    ensureSchemasAreRefreshed(true, false);
    SchemaEntry latestValueSchema = latestValueSchemaEntry.get();
    if (latestValueSchema == null) {
      return null;
    }

    return valueSchemaIdToUpdateSchemaMap.get(latestValueSchema.getId());
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

  /**
   * This function fetches schemas if:
   * 1. Schemas haven't been fetched previously
   * 2. This is the first time that derived schemas are needed
   * @param needsDerivedSchemas If the caller of the function needs derived schemas
   * @param forceSchemaRefresh If the caller of the function detects a missing schema and needs to immediately fetch
   *                           updated schemas
   */
  private void ensureSchemasAreRefreshed(boolean needsDerivedSchemas, boolean forceSchemaRefresh) {
    ConcurrencyUtils.executeUnderConditionalLock(() -> {
      loadUpdateSchemas.compareAndSet(false, needsDerivedSchemas);
      this.refreshAllSchemas();
    },
        () -> forceSchemaRefresh || latestValueSchemaEntry.get() == null
            || (needsDerivedSchemas && !loadUpdateSchemas.get()),
        this);
  }

  private SchemaEntry fetchSingleSchema(String requestPath, boolean isValueSchema) throws VeniceClientException {
    SchemaResponse schemaResponse = fetchSingleSchemaResponse(requestPath);

    if (schemaResponse == null) {
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
          3,
          Duration.ofNanos(1),
          Collections.singletonList(ExecutionException.class));
    } catch (Exception e) {
      throw new VeniceClientException("Failed to execute request from path " + requestPath, e);
    }

    if (response == null) {
      LOGGER.warn("Requested data doesn't exist for request path: {}", requestPath);
      return null;
    }

    return response;
  }

  private String getExceptionDetails(String requestPath) {
    return "Store: " + storeName + ", path: " + requestPath + ", storeClient: " + storeClient;
  }

  private SchemaEntry fetchKeySchema() throws VeniceClientException {
    String requestPath = TYPE_KEY_SCHEMA + "/" + storeName;
    return fetchSingleSchema(requestPath, false);
  }

  private void refreshAllSchemas() throws VeniceClientException {
    refreshAllValueSchemas();
    if (loadUpdateSchemas.get()) {
      refreshAllUpdateSchemas();
    }
  }

  private void refreshAllValueSchemas() throws VeniceClientException {
    String requestPath = TYPE_VALUE_SCHEMA + "/" + storeName;
    try {
      MultiSchemaResponse schemaResponse = fetchMultiSchemaResponse(requestPath);

      if (schemaResponse == null) {
        return;
      }

      int supersetSchemaId = schemaResponse.getSuperSetSchemaId();

      int maxSchemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
      int maxPreferredSchemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
      boolean supersetSchemaIsPreferredSchema = false;
      for (MultiSchemaResponse.Schema schema: schemaResponse.getSchemas()) {
        int schemaId = schema.getId();
        String schemaStr = schema.getSchemaStr();
        Schema writerSchema = preemptiveSchemaVerification(
            // Use the "LOOSE" mode here since we might have registered schemas that do not pass the STRICT validation
            // and that is allowed for now
            parseSchemaFromJSONLooseValidation(schemaStr),
            schemaStr,
            schemaId);
        valueSchemaMap.put(schemaId, writerSchema);
        valueSchemaMapR.put(writerSchema, schemaId);
        if (maxSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID || maxSchemaId < schemaId) {
          maxSchemaId = schemaId;
        }
        if (preferredSchemaFilter.test(writerSchema)) {
          if (schemaId == supersetSchemaId) {
            supersetSchemaIsPreferredSchema = true;
          }

          if (maxPreferredSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID || maxPreferredSchemaId < schemaId) {
            maxPreferredSchemaId = schemaId;
          }
        }
      }

      synchronized (this) {
        if (maxSchemaId > maxValueSchemaId.get()) {
          maxValueSchemaId.set(maxSchemaId);
        }

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

        if (latestSchemaId != SchemaData.INVALID_VALUE_SCHEMA_ID && (latestSchemaId == supersetSchemaId
            || latestValueSchemaEntry.get() == null || latestSchemaId > latestValueSchemaEntry.get().getId())) {
          latestValueSchemaEntry.set(new SchemaEntry(latestSchemaId, valueSchemaMap.get(latestSchemaId)));
        }
      }
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        throw e;
      }

      throw new VeniceClientException(
          "Got exception while trying to fetch all value schemas. " + getExceptionDetails(requestPath),
          e);
    }
  }

  private void refreshAllUpdateSchemas() throws VeniceClientException {
    String requestPath = TYPE_GET_UPDATE_SCHEMA + "/" + storeName;
    try {
      MultiSchemaResponse schemaResponse = fetchMultiSchemaResponse(requestPath);

      if (schemaResponse == null) {
        return;
      }

      for (MultiSchemaResponse.Schema schema: schemaResponse.getSchemas()) {
        int valueSchemaId = schema.getId();
        int updateSchemaId = schema.getDerivedSchemaId();
        String schemaStr = schema.getSchemaStr();
        valueSchemaIdToUpdateSchemaMap
            .computeIfAbsent(valueSchemaId, id -> new DerivedSchemaEntry(valueSchemaId, updateSchemaId, schemaStr));
      }
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        throw e;
      }

      throw new VeniceClientException(
          "Got exception while trying to fetch all update schemas. " + getExceptionDetails(requestPath),
          e);
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
}
