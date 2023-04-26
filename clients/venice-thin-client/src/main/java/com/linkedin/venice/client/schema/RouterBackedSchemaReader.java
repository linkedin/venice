package com.linkedin.venice.client.schema;

import static com.linkedin.venice.client.store.ClientConfig.DEFAULT_SCHEMA_REFRESH_PERIOD_SEC;
import static com.linkedin.venice.schema.AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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

  private static final Logger LOGGER = LogManager.getLogger(RouterBackedSchemaReader.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private final Optional<Schema> readerSchema;
  private volatile Schema keySchema;
  private final Map<Integer, Schema> valueSchemaMap = new VeniceConcurrentHashMap<>();
  private final Map<Schema, Integer> valueSchemaMapR = new VeniceConcurrentHashMap<>();
  private final AtomicReference<SchemaEntry> latestValueSchemaEntry = new AtomicReference<>();
  private final String storeName;
  private final AbstractAvroStoreClient storeClient;
  private final Predicate<Schema> preferredSchemaFilter;
  private final Duration valueSchemaRefreshPeriod;
  private final ICProvider icProvider;
  private int maxValueSchemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
  private Instant lastValueSchemaRefreshTime = Instant.EPOCH;

  RouterBackedSchemaReader(Supplier<AbstractAvroStoreClient> clientSupplier) throws VeniceClientException {
    this(clientSupplier, Optional.empty(), Optional.empty());
  }

  public RouterBackedSchemaReader(
      Supplier<AbstractAvroStoreClient> clientSupplier,
      Optional<Schema> readerSchema,
      Optional<Predicate<Schema>> preferredSchemaFilter) {
    this(clientSupplier, readerSchema, preferredSchemaFilter, null);
  }

  public RouterBackedSchemaReader(
      Supplier<AbstractAvroStoreClient> clientSupplier,
      Optional<Schema> readerSchema,
      Optional<Predicate<Schema>> preferredSchemaFilter,
      ICProvider icProvider) {
    this(clientSupplier, readerSchema, preferredSchemaFilter, DEFAULT_SCHEMA_REFRESH_PERIOD_SEC, icProvider);
  }

  public RouterBackedSchemaReader(
      Supplier<AbstractAvroStoreClient> clientSupplier,
      Optional<Schema> readerSchema,
      Optional<Predicate<Schema>> preferredSchemaFilter,
      int valueSchemaRefreshPeriodInSec) {
    this(clientSupplier, readerSchema, preferredSchemaFilter, valueSchemaRefreshPeriodInSec, null);
  }

  public RouterBackedSchemaReader(
      Supplier<AbstractAvroStoreClient> clientSupplier,
      Optional<Schema> readerSchema,
      Optional<Predicate<Schema>> preferredSchemaFilter,
      int valueSchemaRefreshPeriodInSec,
      ICProvider icProvider) {
    this.storeClient = clientSupplier.get();
    this.storeName = this.storeClient.getStoreName();
    this.readerSchema = readerSchema;
    this.preferredSchemaFilter = preferredSchemaFilter.orElse(schema -> true);
    readerSchema.ifPresent(AvroSchemaUtils::validateAvroSchemaStr);
    this.valueSchemaRefreshPeriod = Duration.of(valueSchemaRefreshPeriodInSec, ChronoUnit.SECONDS);
    this.icProvider = icProvider;
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

    try {
      // try to refresh all value schema and latestValueSchemaEntry field.
      synchronized (this) {
        valueSchema = valueSchemaMap.get(id);
        if (valueSchema == null) {
          refreshAllValueSchema();
        }
      }
    } catch (VeniceException e) {
      throw new RuntimeException(e);
    }

    valueSchema = valueSchemaMap.get(id);
    if (valueSchema == null) {
      LOGGER.warn("Got null value schema from Venice for store: {} and id: {}", storeName, id);
    }
    return valueSchema;
  }

  private static final Function<SchemaEntry, Schema> SCHEMA_EXTRACTOR = schemaEntry -> schemaEntry.getSchema();

  @Override
  public Schema getLatestValueSchema() throws VeniceClientException {
    return ensureLatestValueSchemaIsFetched(SCHEMA_EXTRACTOR);
  }

  private static final Function<SchemaEntry, Integer> SCHEMA_ID_EXTRACTOR = schemaEntry -> schemaEntry.getId();

  @Override
  public Integer getLatestValueSchemaId() throws VeniceClientException {
    return ensureLatestValueSchemaIsFetched(SCHEMA_ID_EXTRACTOR);
  }

  @Override
  public int getValueSchemaId(Schema schema) {
    if (valueSchemaMapR.containsKey(schema)) {
      return valueSchemaMapR.get(schema);
    } else {
      synchronized (this) {
        if (!valueSchemaMapR.containsKey(schema)) {
          refreshAllValueSchema();
        }
      }
    }
    if (!valueSchemaMapR.containsKey(schema)) {
      throw new VeniceClientException("Could not find schema: " + schema + ". for store " + storeName);
    }
    return valueSchemaMapR.get(schema);
  }

  @Override
  public int getMaxValueSchemaId() {
    if (Duration.between(lastValueSchemaRefreshTime, Instant.now()).compareTo(valueSchemaRefreshPeriod) > 0) {
      synchronized (this) {
        refreshAllValueSchema();
      }
    }
    return maxValueSchemaId;
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(storeClient, LOGGER::error);
  }

  private <T> T ensureLatestValueSchemaIsFetched(Function<SchemaEntry, T> schemaEntryConsumer) {
    if (latestValueSchemaEntry.get() == null) {
      synchronized (this) {
        // Check it again
        if (latestValueSchemaEntry.get() == null) {
          refreshAllValueSchema();
        }
      }
    }
    SchemaEntry latest = latestValueSchemaEntry.get();
    return latest == null ? null : schemaEntryConsumer.apply(latest);
  }

  private SchemaEntry fetchSingleSchema(String requestPath, boolean isValueSchema) throws VeniceClientException {
    SchemaEntry schemaEntry = null;
    try {
      CompletableFuture<byte[]> responseFuture;
      if (icProvider != null) {
        responseFuture = icProvider.call(this.getClass().getCanonicalName(), () -> storeClient.getRaw(requestPath));
      } else {
        responseFuture = (CompletableFuture<byte[]>) storeClient.getRaw(requestPath);
      }

      byte[] response = RetryUtils.executeWithMaxAttempt(
          () -> (responseFuture.get()),
          3,
          Duration.ofNanos(1),
          Arrays.asList(ExecutionException.class));
      if (response == null) {
        LOGGER.warn("Requested schema doesn't exist for request path: {}", requestPath);
        return null;
      }

      SchemaResponse schemaResponse = OBJECT_MAPPER.readValue(response, SchemaResponse.class);
      if (!schemaResponse.isError()) {
        Schema writerSchema = isValueSchema
            ? preemptiveSchemaVerification(
                Schema.parse(schemaResponse.getSchemaStr()),
                schemaResponse.getSchemaStr(),
                schemaResponse.getId())
            : Schema.parse(schemaResponse.getSchemaStr());
        schemaEntry = new SchemaEntry(schemaResponse.getId(), writerSchema);
      } else {
        throw new VeniceClientException(
            "Received an error while fetching schema. " + getExceptionDetails(requestPath) + ", error message: "
                + schemaResponse.getError());
      }
    } catch (VeniceClientException e) {
      throw e;
    } catch (Exception e) {
      throw new VeniceClientException(
          "Got exception while trying to fetch single schema. " + getExceptionDetails(requestPath),
          e);
    }

    return schemaEntry;
  }

  private String getExceptionDetails(String requestPath) {
    return "Store: " + storeName + ", path: " + requestPath + ", storeClient: " + storeClient;
  }

  private SchemaEntry fetchKeySchema() throws VeniceClientException {
    String requestPath = TYPE_KEY_SCHEMA + "/" + storeName;
    return fetchSingleSchema(requestPath, false);
  }

  private void refreshAllValueSchema() throws VeniceClientException {
    String requestPath = TYPE_VALUE_SCHEMA + "/" + storeName;
    try {
      byte[] response = RetryUtils.executeWithMaxAttempt(
          () -> ((CompletableFuture<byte[]>) storeClient.getRaw(requestPath)).get(),
          3,
          Duration.ofNanos(1),
          Arrays.asList(ExecutionException.class));
      if (response == null) {
        LOGGER.info("Got null for request path: {}", requestPath);
        return;
      }

      MultiSchemaResponse schemaResponse = OBJECT_MAPPER.readValue(response, MultiSchemaResponse.class);
      if (schemaResponse.isError()) {
        throw new VeniceClientException(
            "Received an error while fetching schema. " + getExceptionDetails(requestPath) + ", error message: "
                + schemaResponse.getError());
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

      if (maxSchemaId > maxValueSchemaId) {
        maxValueSchemaId = maxSchemaId;
      }

      synchronized (this) {
        if (maxPreferredSchemaId != SchemaData.INVALID_VALUE_SCHEMA_ID && !supersetSchemaIsPreferredSchema) {
          if (latestValueSchemaEntry.get() == null || latestValueSchemaEntry.get().getId() < maxPreferredSchemaId) {
            latestValueSchemaEntry.set(new SchemaEntry(maxPreferredSchemaId, valueSchemaMap.get(maxPreferredSchemaId)));
          }
        } else if (supersetSchemaId != SchemaData.INVALID_VALUE_SCHEMA_ID) {
          latestValueSchemaEntry.set(new SchemaEntry(supersetSchemaId, valueSchemaMap.get(supersetSchemaId)));
        } else if (maxSchemaId != SchemaData.INVALID_VALUE_SCHEMA_ID) {
          if (latestValueSchemaEntry.get() == null || latestValueSchemaEntry.get().getId() < maxSchemaId) {
            latestValueSchemaEntry.set(new SchemaEntry(maxSchemaId, valueSchemaMap.get(maxSchemaId)));
          }
        }
      }

      lastValueSchemaRefreshTime = Instant.now();
    } catch (Exception e) {
      throw new VeniceClientException(
          "Got exception while trying to fetch single schema. " + getExceptionDetails(requestPath),
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
  Schema preemptiveSchemaVerification(Schema writerSchema, String writerSchemaStr, int schemaId) {
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
