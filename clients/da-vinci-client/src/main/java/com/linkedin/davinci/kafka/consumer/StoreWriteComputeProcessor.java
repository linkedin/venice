package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.merge.MergeRecordHelper;
import com.linkedin.venice.schema.writecompute.WriteComputeProcessor;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaValidator;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.avro.MapOrderingPreservingSerDeFactory;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.Validate;


/**
 * This class handles Write Compute operations related to a specific store.
 * TODO: Optimize the process to reduce the footprint.
 */
public class StoreWriteComputeProcessor {
  private final String storeName;
  private final ReadOnlySchemaRepository schemaRepo;
  private final WriteComputeProcessor writeComputeProcessor;
  private final Map<SchemaIds, ValueAndWriteComputeSchemas> schemaIdsToSchemasMap;
  private final Map<Schema, AvroSerializer<GenericRecord>> valueSchemaSerializerMap;

  public StoreWriteComputeProcessor(
      @Nonnull String storeName,
      @Nonnull ReadOnlySchemaRepository schemaRepo,
      MergeRecordHelper mergeRecordHelper) {
    Validate.notEmpty(storeName);
    Validate.notNull(schemaRepo);
    this.storeName = storeName;
    this.schemaRepo = schemaRepo;
    this.writeComputeProcessor = new WriteComputeProcessor(mergeRecordHelper);
    this.schemaIdsToSchemasMap = new VeniceConcurrentHashMap<>();
    this.valueSchemaSerializerMap = new VeniceConcurrentHashMap<>();
  }

  /**
   * Apply Update operation on the current value record.
   *
   * @param currValue value record that is currently stored on this Venice server. It is null when there
   *                  is currently no value stored on this Venice server.
   * @param writeComputeBytes serialized write-compute operation.
   * @param writerValueSchemaId ID of the writer value schema.
   * @param readerValueSchemaId ID of the reader value schema.
   * @param writerUpdateProtocolVersion Update protocol version used to serialize Update payload bytes.
   * @param readerUpdateProtocolVersion Update protocol version used to deserialize Update payload bytes.
   *
   * @return Bytes of partially updated original value.
   */
  public byte[] applyWriteCompute(
      GenericRecord currValue,
      int writerValueSchemaId,
      int readerValueSchemaId,
      ByteBuffer writeComputeBytes,
      int writerUpdateProtocolVersion,
      int readerUpdateProtocolVersion) {
    GenericRecord writeComputeRecord = deserializeWriteComputeRecord(
        writeComputeBytes,
        writerValueSchemaId,
        readerValueSchemaId,
        writerUpdateProtocolVersion,
        readerUpdateProtocolVersion);
    Schema valueSchema = getValueSchema(readerValueSchemaId);
    GenericRecord updatedValue = writeComputeProcessor.updateRecord(valueSchema, currValue, writeComputeRecord);

    // If write compute is enabled and the record is deleted, the updatedValue will be null.
    if (updatedValue == null) {
      return null;
    }
    return getValueSerializer(valueSchema).serialize(updatedValue);
  }

  private ValueAndWriteComputeSchemas getValueAndWriteComputeSchemas(int valueSchemaId, int writeComputeSchemaId) {
    final SchemaIds schemaIds = new SchemaIds(valueSchemaId, writeComputeSchemaId);

    return schemaIdsToSchemasMap.computeIfAbsent(schemaIds, ids -> {
      final Schema valueSchema = getValueSchema(valueSchemaId);
      final Schema writeComputeSchema = getWriteComputeSchema(valueSchemaId, writeComputeSchemaId);
      WriteComputeSchemaValidator.validate(valueSchema, writeComputeSchema);
      return new ValueAndWriteComputeSchemas(valueSchema, writeComputeSchema);
    });
  }

  private GenericRecord deserializeWriteComputeRecord(
      ByteBuffer writeComputeBytes,
      int writerValueSchemaId,
      int readerValueSchemaId,
      int writerUpdateProtocolVersion,
      int readerUpdateProtocolVersion) {
    Schema writerSchema =
        getValueAndWriteComputeSchemas(writerValueSchemaId, writerUpdateProtocolVersion).getWriteComputeSchema();
    Schema readerSchema =
        getValueAndWriteComputeSchemas(readerValueSchemaId, readerUpdateProtocolVersion).getWriteComputeSchema();

    // Map in write compute needs to have consistent ordering. On the sender side, users may not care about ordering
    // in their maps. However, on the receiver side, we still want to make sure that the same serialized map bytes
    // always get deserialized into maps with the same entry ordering.
    AvroGenericDeserializer<GenericRecord> deserializer =
        MapOrderingPreservingSerDeFactory.getDeserializer(writerSchema, readerSchema);
    return deserializer.deserialize(writeComputeBytes);
  }

  private AvroSerializer<GenericRecord> getValueSerializer(Schema schema) {
    return valueSchemaSerializerMap.computeIfAbsent(schema, id -> new AvroSerializer<>(schema));
  }

  private Schema getValueSchema(int valueSchemaId) {
    Schema valueSchema = schemaRepo.getValueSchema(storeName, valueSchemaId).getSchema();
    if (valueSchema == null) {
      throw new VeniceException(
          String.format("Cannot find value schema for store: %s, value schema id: %d", storeName, valueSchemaId));
    }
    return valueSchema;
  }

  private Schema getWriteComputeSchema(int valueSchemaId, int writeComputeSchemaId) {
    Schema writeComputeSchema = schemaRepo.getDerivedSchema(storeName, valueSchemaId, writeComputeSchemaId).getSchema();
    if (writeComputeSchema == null) {
      throw new VeniceException(
          String.format(
              "Cannot find write-compute schema for store: %s, value schema id: %d," + " write-compute schema id: %d",
              storeName,
              valueSchemaId,
              writeComputeSchemaId));
    }
    return writeComputeSchema;
  }

  /**
   * A POJO to encapsulate different kinds of schema IDs
   */
  private static class SchemaIds {
    private final int valueSchemaId;
    private final int writeComputeSchemaId;

    SchemaIds(int valueSchemaId, int writeComputeSchemaId) {
      this.valueSchemaId = valueSchemaId;
      this.writeComputeSchemaId = writeComputeSchemaId;
    }

    @Override
    public int hashCode() {
      return Pair.calculateHashCode(valueSchemaId, writeComputeSchemaId);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      return this.valueSchemaId == ((SchemaIds) other).valueSchemaId
          && this.writeComputeSchemaId == ((SchemaIds) other).writeComputeSchemaId;
    }
  }

  /**
   * A POJO to encapsulate value schema and write compute schema
   */
  private static class ValueAndWriteComputeSchemas {
    private final Schema valueSchema;
    private final Schema writeComputeSchema;

    ValueAndWriteComputeSchemas(@Nonnull Schema valueSchema, @Nonnull Schema writeComputeSchema) {
      Validate.notNull(valueSchema);
      Validate.notNull(writeComputeSchema);
      this.valueSchema = valueSchema;
      this.writeComputeSchema = writeComputeSchema;
    }

    Schema getValueSchema() {
      return valueSchema;
    }

    Schema getWriteComputeSchema() {
      return writeComputeSchema;
    }
  }
}
