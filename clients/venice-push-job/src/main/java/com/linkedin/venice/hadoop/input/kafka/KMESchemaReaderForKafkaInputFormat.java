package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.schema.SchemaData.INVALID_VALUE_SCHEMA_ID;
import static com.linkedin.venice.system.store.ControllerClientBackedSystemSchemaInitializer.DEFAULT_KEY_SCHEMA_STR;

import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;


/**
 * A specialized {@link SchemaReader} implementation designed for Kafka input format processing
 * in Hadoop MapReduce jobs. This class provides schema reading capabilities specifically for
 * Kafka Message Envelope (KME) schemas used during data ingestion from Kafka topics.
 *
 * <p>This implementation combines newer KME schemas provided at runtime with schemas loaded
 * from resources to create a comprehensive schema registry for deserializing Kafka messages.
 * It is primarily used by {@link KafkaInputUtils} to configure Kafka value serializers with
 * the appropriate schema reader for processing Venice data stored in Kafka topics.
 *
 * <p>Key features:
 * <ul>
 *   <li>Merges runtime-provided KME schemas with resource-based schemas</li>
 *   <li>Provides thread-safe access to schema data using {@link AtomicReference}</li>
 *   <li>Uses default key schema for Venice system operations</li>
 *   <li>Supports schema evolution by handling multiple schema versions</li>
 * </ul>
 */
public class KMESchemaReaderForKafkaInputFormat implements SchemaReader {
  private final SchemaData schemas;
  private final Schema keySchema;

  /**
   * Constructs a new KMESchemaReaderForKafkaInputFormat with the provided newer KME schemas.
   *
   * <p>This constructor initializes the schema reader by:
   * <ol>
   *   <li>Loading all existing KME schemas from resources</li>
   *   <li>Adding the provided newer schemas to the schema data</li>
   *   <li>Merging resource-based schemas with the newer schemas</li>
   *   <li>Setting up the default key schema for Venice operations</li>
   * </ol>
   *
   * <p>The newer schemas take precedence over resource-based schemas when there are
   * schema ID conflicts, allowing for schema evolution and updates.
   *
   * @param newerKmeSchemas A map of schema ID to schema string containing newer KME schemas
   *                        that should be available for deserialization. Can be empty but not null.
   * @throws IllegalArgumentException if newerKmeSchemas is null
   * @throws RuntimeException if there are issues parsing the default key schema
   */
  public KMESchemaReaderForKafkaInputFormat(Map<Integer, String> newerKmeSchemas) {
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
