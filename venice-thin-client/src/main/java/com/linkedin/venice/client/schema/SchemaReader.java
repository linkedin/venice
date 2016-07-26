package com.linkedin.venice.client.schema;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is used to fetch key/value schema for a given store.
 */
public class SchemaReader {
  public static final String TYPE_KEY_SCHEMA = "key_schema";
  public static final String TYPE_VALUE_SCHEMA = "value_schema";
  private static final ObjectMapper mapper = new ObjectMapper();

  private final Logger logger = Logger.getLogger(SchemaReader.class);
  private Schema keySchema;
  private Map<Integer, Schema> valueSchemaMap = new ConcurrentHashMap<>();
  private AtomicReference<SchemaEntry> latestValueSchemaEntry = new AtomicReference<>();

  private final AbstractAvroStoreClient storeClient;
  private final String storeName;

  public SchemaReader(AbstractAvroStoreClient client) throws VeniceClientException {
    this.storeClient = client;
    this.storeName = client.getStoreName();

    // Initialize key schema
    SchemaEntry keySchemaEntry = fetchKeySchema();
    if (null == keySchemaEntry) {
      throw new VeniceClientException("Key Schema of store: " + this.storeName + " doesn't exist");
    }
    keySchema = keySchemaEntry.getSchema();
  }

  public Schema getKeySchema() {
    return keySchema;
  }

  public Schema getValueSchema(int id) {
    valueSchemaMap.computeIfAbsent(id, k -> {
      SchemaEntry valueSchemaEntry = null;
      try {
        valueSchemaEntry = fetchValueSchema(k);
      } catch (VeniceClientException e) {
        throw new RuntimeException(e);
      }
      if (null == valueSchemaEntry) {
        logger.info("Got null value schema from Venice for store: " + storeName + " and id: " + id);
        return null;
      }
      synchronized(this) {
        if (null == latestValueSchemaEntry.get() || valueSchemaEntry.getId() > latestValueSchemaEntry.get().getId()) {
          latestValueSchemaEntry.set(valueSchemaEntry);
        }
      }
      return valueSchemaEntry.getSchema();
    });

    return valueSchemaMap.get(id);
  }

  public Schema getLatestValueSchema() throws VeniceClientException {
    if (null == latestValueSchemaEntry.get()) {
      synchronized(this) {
        // Check it again
        if (null == latestValueSchemaEntry.get()) {
          refreshAllValueSchema();
        }
      }
    }
    if (null == latestValueSchemaEntry.get()) {
      return null;
    }
    return latestValueSchemaEntry.get().getSchema();
  }

  private void refreshAllValueSchema() throws VeniceClientException {
    String requestPath = TYPE_VALUE_SCHEMA + "/" + storeName;
    try {
      Future<byte[]> future = storeClient.getRaw(requestPath);
      byte[] response = future.get();
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
          valueSchemaMap.put(schema.getId(), Schema.parse(schema.getSchemaStr()));
        }
        if (SchemaData.INVALID_VALUE_SCHEMA_ID != latestSchemaId &&
            (null == latestValueSchemaEntry.get() || latestValueSchemaEntry.get().getId() < latestSchemaId)) {
          latestValueSchemaEntry.set(new SchemaEntry(latestSchemaId, valueSchemaMap.get(latestSchemaId)));
        }
      } else {
        throw new VeniceClientException("Got error while fetching schema of store: " + storeName
            + " for path: " + requestPath + " : " + schemaResponse.getError());
      }
    } catch (Exception e) {
      throw new VeniceClientException(e);
    }
  }

  private SchemaEntry fetchSingleSchema(String requestPath) throws VeniceClientException {
    SchemaEntry schemaEntry = null;
    try {
      Future<byte[]> future = storeClient.getRaw(requestPath);
      byte[] response = future.get();
      if (null == response) {
        logger.info("Requested schema doesn't exist for request path: " + requestPath);
        return null;
      }
      SchemaResponse schemaResponse = mapper.readValue(response, SchemaResponse.class);
      if (!schemaResponse.isError()) {
        schemaEntry = new SchemaEntry(schemaResponse.getId(), schemaResponse.getSchemaStr());
      } else {
        throw new VeniceClientException("Got error while fetching schema of store: " + storeName
            + " for path: " + requestPath + " : " + schemaResponse.getError());
      }
    } catch (Exception e) {
      throw new VeniceClientException(e);
    }

    return schemaEntry;
  }

  private SchemaEntry fetchKeySchema() throws VeniceClientException {
    String requestPath = TYPE_KEY_SCHEMA + "/" + storeName;
    return fetchSingleSchema(requestPath);
  }

  private SchemaEntry fetchValueSchema(int id) throws VeniceClientException {
    String requestPath = TYPE_VALUE_SCHEMA + "/" + storeName + "/" + id;
    return fetchSingleSchema(requestPath);
  }

}
