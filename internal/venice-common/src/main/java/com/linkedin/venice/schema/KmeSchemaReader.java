package com.linkedin.venice.schema;

import static com.linkedin.venice.schema.SchemaData.INVALID_VALUE_SCHEMA_ID;
import static com.linkedin.venice.system.store.ControllerClientBackedSystemSchemaInitializer.DEFAULT_KEY_SCHEMA_STR;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;


/**
 * A {@link SchemaReader} for Kafka Message Envelope (KME) protocol-version schemas. Merges
 * KME schemas loaded from jar resources with any newer schemas supplied at runtime (e.g.,
 * fetched from a Venice controller) so a deserializer can resolve a protocol version it does
 * not statically know.
 *
 * <p>Use this whenever a code path reads Venice control messages or value envelopes and has a
 * route to fresher KME schemas than the jar carries — either via {@code newer.kme.schemas.*}
 * job-conf entries broadcast by the VPJ driver, or via a {@link ControllerClient} pointing at
 * the {@code venice_system_store_kafka_message_envelope} system store. Hand the resulting
 * reader to {@link com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer#setSchemaReader}
 * (e.g., through {@link com.linkedin.venice.pubsub.api.PubSubMessageDeserializer#createWithSchemaReader}).
 * It defends the consumer against records that arrive without the {@code vtp} bootstrap header.
 *
 * <p>Update schemas are not supported — KME has no update schemas.
 *
 * <p>Formerly {@code KmeSchemaReaderForKafkaInputFormat} under {@code com.linkedin.venice.hadoop.input.kafka};
 * moved to venice-common in {@link com.linkedin.venice.schema} so non-VPJ callers (admin tool,
 * controller, server fallback, CDC) can construct one without taking a build dependency on
 * venice-push-job.
 */
public class KmeSchemaReader implements SchemaReader {
  private final SchemaData schemas;
  private final Schema keySchema;

  /**
   * Build a reader from a map of newer KME schemas (id → schema JSON). The supplied schemas
   * are merged on top of the schemas baked into this jar's resources, so {@code getValueSchema}
   * can resolve any protocol version that either side knows about.
   *
   * @param newerKmeSchemas id → schema-JSON for schemas newer than what this jar ships; may be empty
   * @throws IllegalArgumentException if {@code newerKmeSchemas} is null
   */
  public KmeSchemaReader(Map<Integer, String> newerKmeSchemas) {
    Map<Integer, Schema> allSchemasFromResources =
        Utils.getAllSchemasFromResources(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE);
    this.schemas = new SchemaData(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName(), null);
    for (Map.Entry<Integer, String> entry: newerKmeSchemas.entrySet()) {
      schemas.addValueSchema(new SchemaEntry(entry.getKey(), entry.getValue()));
    }

    for (Map.Entry<Integer, Schema> entry: allSchemasFromResources.entrySet()) {
      schemas.addValueSchema(new SchemaEntry(entry.getKey(), entry.getValue().toString()));
    }

    this.keySchema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(DEFAULT_KEY_SCHEMA_STR);
  }

  /**
   * Fetch all KME schemas from the {@code venice_system_store_kafka_message_envelope} system
   * store via the supplied {@code controllerClient}, and build a {@link KmeSchemaReader} that
   * merges them with the schemas baked into the local jar. Used by admin tool, controller-side
   * topic metadata fetcher, and other code paths that have a {@link ControllerClient} in scope.
   *
   * @throws VeniceException if the controller call fails (so the caller can choose to swallow
   *         the failure and fall back to a jar-only deserializer, or surface it).
   */
  public static KmeSchemaReader fromControllerClient(ControllerClient controllerClient) {
    String kmeSystemStoreName = AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName();
    MultiSchemaResponse response = controllerClient.getAllValueSchema(kmeSystemStoreName);
    if (response.isError()) {
      throw new VeniceException(
          "Failed to fetch KME schemas from controller for " + kmeSystemStoreName + ": " + response.getError());
    }
    Map<Integer, String> newerKmeSchemas = new HashMap<>();
    for (MultiSchemaResponse.Schema schema: response.getSchemas()) {
      newerKmeSchemas.put(schema.getId(), schema.getSchemaStr());
    }
    return new KmeSchemaReader(newerKmeSchemas);
  }

  /**
   * Returns the key schema used for Venice operations.
   *
   * @return The default key schema for Venice system operations
   */
  @Override
  public Schema getKeySchema() {
    return keySchema;
  }

  /**
   * Retrieves the value schema for the specified schema ID.
   *
   * @param id The schema ID to look up
   * @return The Avro schema corresponding to the given ID
   * @throws IllegalArgumentException if the schema ID is not found
   */
  @Override
  public Schema getValueSchema(int id) {
    return schemas.getValueSchema(id).getSchema();
  }

  /**
   * Finds the schema ID for the given Avro schema.
   *
   * @param schema The Avro schema to find the ID for
   * @return The schema ID corresponding to the given schema, or -1 if not found
   */
  @Override
  public int getValueSchemaId(Schema schema) {
    SchemaEntry schemaEntry = new SchemaEntry(INVALID_VALUE_SCHEMA_ID, schema);
    return schemas.getSchemaID(schemaEntry);
  }

  /**
   * Returns the latest (highest ID) value schema available.
   *
   * @return The most recent value schema
   */
  @Override
  public Schema getLatestValueSchema() {
    return schemas.getValueSchema(getLatestValueSchemaId()).getSchema();
  }

  /**
   * Returns the ID of the latest (highest ID) value schema.
   *
   * @return The schema ID of the most recent value schema
   */
  @Override
  public Integer getLatestValueSchemaId() {
    return schemas.getMaxValueSchemaId();
  }

  /**
   * Update schemas are not supported for KME schema reading in Kafka input format.
   *
   * @param valueSchemaId The value schema ID (ignored)
   * @return Never returns - always throws UnsupportedOperationException
   * @throws VeniceUnsupportedOperationException Always thrown as update schemas are not supported
   */
  @Override
  public Schema getUpdateSchema(int valueSchemaId) {
    throw new VeniceUnsupportedOperationException("getUpdateSchema");
  }

  /**
   * Latest update schemas are not supported for KME schema reading in Kafka input format.
   *
   * @return Never returns - always throws UnsupportedOperationException
   * @throws VeniceUnsupportedOperationException Always thrown as update schemas are not supported
   */
  @Override
  public DerivedSchemaEntry getLatestUpdateSchema() {
    throw new VeniceUnsupportedOperationException("getLatestUpdateSchema");
  }

  /**
   * Closes the schema reader. This implementation is a no-op as there are no resources to clean up.
   */
  @Override
  public void close() {
  }
}
