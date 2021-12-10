package com.linkedin.venice.client.schema;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.MetadataReader;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import static com.linkedin.venice.schema.AvroSchemaParseUtils.*;


/**
 * This class is used to fetch key/value schema for a given store.
 */
public class SchemaReader extends MetadataReader implements SchemaRetriever {
  public static final String TYPE_KEY_SCHEMA = "key_schema";
  public static final String TYPE_VALUE_SCHEMA = "value_schema";
  private static final ObjectMapper mapper = new ObjectMapper();

  // Ignore the unknown field while parsing the json response.
  static {
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  private final static Logger logger = Logger.getLogger(SchemaReader.class);
  private final Optional<Schema> readerSchema;
  private Schema keySchema;
  private Map<Integer, Schema> valueSchemaMap = new VeniceConcurrentHashMap<>();
  private Map<Schema, Integer> valueSchemaMapR = new VeniceConcurrentHashMap<>();
  private AtomicReference<SchemaEntry> latestValueSchemaEntry = new AtomicReference<>();
  private final String storeName;

  public SchemaReader(AbstractAvroStoreClient client) throws VeniceClientException {
    this(client, Optional.empty());
  }

  public SchemaReader(AbstractAvroStoreClient client, Optional<Schema> readerSchema) {
    super(client);
    this.storeName = client.getStoreName();
    this.readerSchema = readerSchema;
    if (readerSchema.isPresent()) {
      AvroSchemaUtils.validateAvroSchemaStr(readerSchema.get());
    }
  }

  @Override
  public Schema getKeySchema() {
    if (null != keySchema) {
      return keySchema;
    }
    synchronized (this) {
      if (null != keySchema) {
        return keySchema;
      }
      SchemaEntry keySchemaEntry = fetchKeySchema();
      if (null == keySchemaEntry) {
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
      logger.info("Got null value schema from Venice for store: " + storeName + " and id: " + id);
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

  private <T> T ensureLatestValueSchemaIsFetched(Function<SchemaEntry, T> schemaEntryConsumer) {
    if (null == latestValueSchemaEntry.get()) {
      synchronized (this) {
        // Check it again
        if (null == latestValueSchemaEntry.get()) {
          refreshAllValueSchema();
        }
      }
    }
    SchemaEntry latest = latestValueSchemaEntry.get();
    return null == latest ? null : schemaEntryConsumer.apply(latest);
  }

  private SchemaEntry fetchSingleSchema(String requestPath, boolean isValueSchema) throws VeniceClientException {
    SchemaEntry schemaEntry = null;
    try {
      byte[] response = storeClientGetRawWithRetry(requestPath);
      if (null == response) {
        logger.info("Requested schema doesn't exist for request path: " + requestPath);
        return null;
      }
      SchemaResponse schemaResponse = mapper.readValue(response, SchemaResponse.class);
      if (!schemaResponse.isError()) {
        Schema writerSchema = isValueSchema ? preemptiveSchemaVerification(Schema.parse(schemaResponse.getSchemaStr()), schemaResponse.getSchemaStr(),
            schemaResponse.getId())
            : Schema.parse(schemaResponse.getSchemaStr());
        schemaEntry = new SchemaEntry(schemaResponse.getId(), writerSchema);
      } else {
        throw new VeniceClientException(
            "Received an error while fetching schema. " + getExceptionDetails(requestPath) + ", error message: " + schemaResponse.getError());
      }
    } catch (VeniceClientException e) {
      throw e;
    } catch (Exception e) {
      throw new VeniceClientException("Got exception while trying to fetch single schema. " + getExceptionDetails(requestPath), e);
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
      byte[] response = storeClientGetRawWithRetry(requestPath);
      if (null == response) {
        logger.info("Got null for request path: " + requestPath);
        return;
      }
      MultiSchemaResponse schemaResponse = mapper.readValue(response, MultiSchemaResponse.class);
      if (!schemaResponse.isError()) {
        // Update local cache
        int latestSchemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
        MultiSchemaResponse.Schema[] entries = schemaResponse.getSchemas();
        for (MultiSchemaResponse.Schema schema : entries) {
          if (SchemaData.INVALID_VALUE_SCHEMA_ID == latestSchemaId || latestSchemaId < schema.getId()) {
            latestSchemaId = schema.getId();
          }
          final String schemaStr = schema.getSchemaStr();
          Schema writerSchema = preemptiveSchemaVerification(
                  // Use the "LOOSE" mode here since we might have registered schemas that do not pass the STRICT validation
                  // and that is allowed for now
                  parseSchemaFromJSONLooseValidation(schemaStr),
                  schemaStr,
                  schema.getId()
          );
          valueSchemaMap.put(schema.getId(), writerSchema);
          valueSchemaMapR.put(writerSchema, schema.getId());
        }
        if (schemaResponse.getSuperSetSchemaId() != -1) {
          latestSchemaId = schemaResponse.getSuperSetSchemaId();
          latestValueSchemaEntry.set(new SchemaEntry(latestSchemaId, valueSchemaMap.get(latestSchemaId)));
        } else {
          if (SchemaData.INVALID_VALUE_SCHEMA_ID != latestSchemaId && (null == latestValueSchemaEntry.get()
              || latestValueSchemaEntry.get().getId() < latestSchemaId)) {
            latestValueSchemaEntry.set(new SchemaEntry(latestSchemaId, valueSchemaMap.get(latestSchemaId)));
          }
        }
      } else {
        throw new VeniceClientException(
            "Received an error while fetching schema. " + getExceptionDetails(requestPath) + ", error message: " + schemaResponse.getError());
      }
    } catch (Exception e) {
      throw new VeniceClientException("Got exception while trying to fetch single schema. " + getExceptionDetails(requestPath), e);
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
        logger.info("Schema error detected during preemptive schema check for store " + storeName + " with writer schema id "
            + schemaId + " Reader schema: " + readerSchemaCopy.toString() + " Writer schema: " + writerSchemaStr);
        alternativeWriterSchema =
            AvroSchemaUtils.generateSchemaWithNamespace(writerSchemaStr, readerSchemaCopy.getNamespace());
        if (AvroSchemaUtils.schemaResolveHasErrors(alternativeWriterSchema, readerSchemaCopy)) {
          logger.info("Schema error cannot be resolved with writer schema namespace fix");
          alternativeWriterSchema = writerSchema;
        } else {
          logger.info(
              "Schema error can be resolved with writer schema namespace fix. Replacing writer schema for store " + storeName + " and schema id " + schemaId);
        }
      }
    } catch (Exception e) {
      logger.info("Unknown exception during preemptive schema verification", e);
      alternativeWriterSchema = writerSchema;
    }
    return alternativeWriterSchema;
  }
}
