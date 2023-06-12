package com.linkedin.venice.client.schema;

import static com.linkedin.venice.schema.AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.InvalidVeniceSchemaException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.RetryUtils;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.commons.lang.Validate;


/**
 * Router based implementation of {@link StoreSchemaFetcher}. It supports schema look up for key schema, the latest value
 * schema and latest update schema of a provided value schema version.
 * Notice that this class does not contain any caching of schema so each API invocation will send a new request to
 * Venice Router to fetch the desired store schema.
 */
public class RouterBasedStoreSchemaFetcher implements StoreSchemaFetcher {
  public static final String TYPE_KEY_SCHEMA = "key_schema";
  public static final String TYPE_VALUE_SCHEMA = "value_schema";
  public static final String TYPE_GET_UPDATE_SCHEMA = "update_schema";
  public static final String TYPE_LATEST_VALUE_SCHEMA = "latest_value_schema";
  private final AbstractAvroStoreClient storeClient;
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  // Ignore the unknown field while parsing the json response.
  static {
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public RouterBasedStoreSchemaFetcher(AbstractAvroStoreClient client) {
    this.storeClient = client;
  }

  @Override
  public Schema getKeySchema() {
    String keySchemaRequestPath = TYPE_KEY_SCHEMA + "/" + storeClient.getStoreName();
    String keySchemaStr = fetchSingleSchemaString(keySchemaRequestPath);
    try {
      return parseSchemaFromJSONLooseValidation(keySchemaStr);
    } catch (Exception e) {
      throw new VeniceException("Got exception while parsing key schema", e);
    }
  }

  @Override
  public Schema getLatestValueSchema() {
    // Fetch the latest value schema for the specified value schema.
    String latestSchemaRequestPath = TYPE_LATEST_VALUE_SCHEMA + "/" + storeClient.getStoreName();
    String latestSchemaStr;
    try {
      latestSchemaStr = fetchSingleSchemaString(latestSchemaRequestPath);
    } catch (Exception e) {
      // If the routers don't support /latest_value_schema yet, an Exception will be thrown from executeRequest
      // In such a case, fetch the latest value schema from the /value_schema endpoint.
      return getLatestValueSchemaFromAllValueSchemas();
    }

    Schema latestSchema;
    try {
      latestSchema = parseSchemaFromJSONLooseValidation(latestSchemaStr);
    } catch (Exception e) {
      throw new VeniceException("Got exception while parsing superset schema", e);
    }
    return latestSchema;
  }

  private Schema getLatestValueSchemaFromAllValueSchemas() {
    MultiSchemaResponse multiSchemaResponse = fetchAllValueSchemas();
    int targetSchemaId = multiSchemaResponse.getSuperSetSchemaId();
    int largestSchemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
    String schemaStr = null;
    for (MultiSchemaResponse.Schema schema: multiSchemaResponse.getSchemas()) {
      if (targetSchemaId != SchemaData.INVALID_VALUE_SCHEMA_ID && schema.getId() == targetSchemaId) {
        schemaStr = schema.getSchemaStr();
        break;
      }

      if (SchemaData.INVALID_VALUE_SCHEMA_ID == largestSchemaId || largestSchemaId < schema.getId()) {
        largestSchemaId = schema.getId();
        schemaStr = schema.getSchemaStr();
      }
    }
    try {
      return parseSchemaFromJSONLooseValidation(schemaStr);
    } catch (Exception e) {
      throw new VeniceException("Got exception while parsing latest value schema", e);
    }
  }

  @Override
  public Set<Schema> getAllValueSchemas() {
    MultiSchemaResponse multiSchemaResponse = fetchAllValueSchemas();
    Set<Schema> schemaSet = new HashSet<>();
    try {
      for (MultiSchemaResponse.Schema schema: multiSchemaResponse.getSchemas()) {
        schemaSet.add(parseSchemaFromJSONLooseValidation(schema.getSchemaStr()));
      }
    } catch (Exception e) {
      throw new VeniceException("Got exception while parsing value schema", e);
    }
    return schemaSet;
  }

  @Override
  public Schema getUpdateSchema(Schema valueSchema) throws VeniceException {
    int valueSchemaId = getValueSchemaId(valueSchema);
    // Fetch the latest update schema for the specified value schema.
    String updateSchemaRequestPath = TYPE_GET_UPDATE_SCHEMA + "/" + storeClient.getStoreName() + "/" + valueSchemaId;
    String updateSchemaStr = fetchSingleSchemaString(updateSchemaRequestPath);
    Schema updateSchema;
    try {
      updateSchema = parseSchemaFromJSONLooseValidation(updateSchemaStr);
    } catch (Exception e) {
      throw new VeniceException("Got exception while parsing update schema", e);
    }
    if (!updateSchema.getType().equals(Schema.Type.RECORD)) {
      throw new InvalidVeniceSchemaException("Update schema can only be record schema.");
    }
    return updateSchema;
  }

  @Override
  public String getStoreName() {
    return storeClient.getStoreName();
  }

  @Override
  public void close() throws IOException {
    storeClient.close();
  }

  private int getValueSchemaId(Schema valueSchema) {
    Validate.notNull(valueSchema, "Input value schema is null.");
    if (!valueSchema.getType().equals(Schema.Type.RECORD)) {
      throw new InvalidVeniceSchemaException(
          "Input value schema is not a record type schema. Update schema can only be derived from record schema.");
    }
    // Locate value schema ID for input value schema.
    MultiSchemaResponse multiSchemaResponse = fetchAllValueSchemas();
    Schema retrievedValueSchema;
    int valueSchemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
    for (MultiSchemaResponse.Schema schema: multiSchemaResponse.getSchemas()) {
      try {
        // Use loose validation for old value schema that does not set default value as the first branch of union field.
        retrievedValueSchema = parseSchemaFromJSONLooseValidation(schema.getSchemaStr());
      } catch (Exception e) {
        throw new VeniceException("Got exception while parsing latest value schema", e);
      }
      if (retrievedValueSchema.equals(valueSchema)) {
        valueSchemaId = schema.getId();
      }
    }

    if (valueSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
      throw new InvalidVeniceSchemaException(
          "Input value schema not found in Venice backend for store: " + storeClient.getStoreName());
    }

    return valueSchemaId;
  }

  private MultiSchemaResponse fetchAllValueSchemas() {
    String requestPath = TYPE_VALUE_SCHEMA + "/" + storeClient.getStoreName();
    MultiSchemaResponse multiSchemaResponse;
    byte[] response = executeRequest(requestPath);
    try {
      multiSchemaResponse = OBJECT_MAPPER.readValue(response, MultiSchemaResponse.class);
    } catch (Exception e) {
      throw new VeniceException("Got exception while deserializing response", e);
    }
    if (multiSchemaResponse.isError()) {
      throw new VeniceException(
          "Received an error while fetching value schemas from path: " + requestPath + ", error message: "
              + multiSchemaResponse.getError());
    }
    if (multiSchemaResponse.getSchemas() == null) {
      throw new VeniceException("Received bad schema response with null schema string");
    }
    return multiSchemaResponse;
  }

  private String fetchSingleSchemaString(String requestPath) throws VeniceClientException {
    SchemaResponse schemaResponse;
    byte[] response = executeRequest(requestPath);
    try {
      schemaResponse = OBJECT_MAPPER.readValue(response, SchemaResponse.class);
    } catch (Exception e) {
      throw new VeniceException("Got exception while deserializing response", e);
    }
    if (schemaResponse.isError()) {
      throw new VeniceException(
          "Received an error while fetching schema from path: " + requestPath + ", error message: "
              + schemaResponse.getError());
    }
    if (schemaResponse.getSchemaStr() == null) {
      throw new VeniceException("Received bad schema response with null schema string");
    }
    return schemaResponse.getSchemaStr();
  }

  private byte[] executeRequest(String requestPath) {
    byte[] response;
    try {
      response = RetryUtils.executeWithMaxAttempt(
          () -> ((CompletableFuture<byte[]>) storeClient.getRaw(requestPath)).get(),
          3,
          Duration.ofSeconds(5),
          Collections.singletonList(ExecutionException.class));
    } catch (Exception e) {
      throw new VeniceException("Failed to fetch schema from path " + requestPath, e);
    }

    if (response == null) {
      throw new VeniceException("Requested schema(s) doesn't exist for request path: " + requestPath);
    }
    return response;
  }
}
