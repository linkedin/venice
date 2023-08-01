package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.merge.MergeRecordHelper;
import com.linkedin.venice.schema.writecompute.WriteComputeProcessor;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaValidator;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.avro.MapOrderingPreservingSerDeFactory;
import com.linkedin.venice.utils.SparseConcurrentList;
import com.linkedin.venice.utils.collections.BiIntKeyCache;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.Validate;


/**
 * This class handles Write Compute operations related to a specific store.
 */
public class StoreWriteComputeProcessor {
  private final String storeName;
  private final ReadOnlySchemaRepository schemaRepo;
  private final WriteComputeProcessor writeComputeProcessor;

  /** List of serializers keyed by their value schema ID. */
  private final SparseConcurrentList<RecordSerializer<GenericRecord>> serializerIndexedBySchemaId;

  /** List of write compute schemas keyed by the unique ID generated by {@link #uniqueIdGenerator}. */
  private final SparseConcurrentList<Schema> writeComputeSchemasIndexedByUniqueId;

  /** This is a purely synthetic ID, generated by the process, and not related to any ID registered in any repository */
  private final AtomicInteger uniqueIdGenerator;

  /**
   * A read-through cache keyed by the composition of {valueSchemaId, writeComputeSchemaId}, returning a
   * {@link SchemaAndUniqueId} object. The unique ID serves as keys in {@link #writeComputeDeserializerCache}.
   */
  private final BiIntKeyCache<SchemaAndUniqueId> schemaAndUniqueIdCache;

  /**
   * A read-through cache keyed by a pair of unique IDs corresponding to the writer and reader WC schemas,
   * returning a deserializer capable of decoding from writer to reader.
   */
  private final BiIntKeyCache<RecordDeserializer<GenericRecord>> writeComputeDeserializerCache;

  private final boolean fastAvroEnabled;

  public StoreWriteComputeProcessor(
      @Nonnull String storeName,
      @Nonnull ReadOnlySchemaRepository schemaRepo,
      MergeRecordHelper mergeRecordHelper,
      boolean fastAvroEnabled) {
    Validate.notEmpty(storeName);
    Validate.notNull(schemaRepo);
    this.storeName = storeName;
    this.schemaRepo = schemaRepo;
    this.writeComputeProcessor = new WriteComputeProcessor(mergeRecordHelper);
    this.serializerIndexedBySchemaId = new SparseConcurrentList<>();
    this.writeComputeSchemasIndexedByUniqueId = new SparseConcurrentList<>();
    this.uniqueIdGenerator = new AtomicInteger();
    this.fastAvroEnabled = fastAvroEnabled;
    this.schemaAndUniqueIdCache = new BiIntKeyCache<>((valueSchemaId, writeComputeSchemaId) -> {
      final Schema valueSchema = getValueSchema(valueSchemaId);
      final Schema writeComputeSchema = getWriteComputeSchema(valueSchemaId, writeComputeSchemaId);
      WriteComputeSchemaValidator.validate(valueSchema, writeComputeSchema);
      final int uniqueId = this.uniqueIdGenerator.getAndIncrement();
      this.writeComputeSchemasIndexedByUniqueId.set(uniqueId, writeComputeSchema);
      return new SchemaAndUniqueId(uniqueId, valueSchema);
    });
    this.writeComputeDeserializerCache = new BiIntKeyCache<>((writerSchemaUniqueId, readerSchemaUniqueId) -> {
      Schema writerSchema = this.writeComputeSchemasIndexedByUniqueId.get(writerSchemaUniqueId);
      Schema readerSchema = this.writeComputeSchemasIndexedByUniqueId.get(readerSchemaUniqueId);
      return getValueDeserializer(writerSchema, readerSchema);
    });
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
    int writerSchemaUniqueId = getSchemaAndUniqueId(writerValueSchemaId, writerUpdateProtocolVersion).getUniqueId();
    SchemaAndUniqueId readerSchemaContainer = getSchemaAndUniqueId(readerValueSchemaId, readerUpdateProtocolVersion);
    RecordDeserializer<GenericRecord> deserializer =
        this.writeComputeDeserializerCache.get(writerSchemaUniqueId, readerSchemaContainer.getUniqueId());
    GenericRecord writeComputeRecord = deserializer.deserialize(writeComputeBytes);

    GenericRecord updatedValue =
        writeComputeProcessor.updateRecord(readerSchemaContainer.getValueSchema(), currValue, writeComputeRecord);

    // If write compute is enabled and the record is deleted, the updatedValue will be null.
    if (updatedValue == null) {
      return null;
    }
    return getValueSerializer(readerValueSchemaId).serialize(updatedValue);
  }

  private SchemaAndUniqueId getSchemaAndUniqueId(int valueSchemaId, int writeComputeSchemaId) {
    return schemaAndUniqueIdCache.get(valueSchemaId, writeComputeSchemaId);
  }

  RecordDeserializer<GenericRecord> getValueDeserializer(Schema writerSchema, Schema readerSchema) {
    if (fastAvroEnabled) {
      return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(writerSchema, readerSchema);
    }

    /**
     * It is not necessary to preserve the map-order since this class is only used by {@link LeaderFollowerStoreIngestionTask}.
     * We are not supposed to run DCR validation workflow against non-AA store.
     */
    // Map in write compute needs to have consistent ordering. On the sender side, users may not care about ordering
    // in their maps. However, on the receiver side, we still want to make sure that the same serialized map bytes
    // always get deserialized into maps with the same entry ordering.
    return MapOrderingPreservingSerDeFactory.getDeserializer(writerSchema, readerSchema);
  }

  private RecordSerializer<GenericRecord> getValueSerializer(int valueSchemaId) {
    return serializerIndexedBySchemaId.computeIfAbsent(valueSchemaId, this::generateValueSerializer);
  }

  RecordSerializer<GenericRecord> generateValueSerializer(int valueSchemaId) {
    Schema valueSchema = getValueSchema(valueSchemaId);
    /**
     * It is not necessary to preserve the map-order since this class is only used by {@link LeaderFollowerStoreIngestionTask}.
     * We are not supposed to run DCR validation workflow against non-AA store.
     */
    if (fastAvroEnabled) {
      return FastSerializerDeserializerFactory.getFastAvroGenericSerializer(valueSchema);
    }
    return new AvroSerializer<>(valueSchema);
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
   * A POJO to encapsulate value schema and a unique ID associated with a pair of value schema and write compute schema
   */
  private static class SchemaAndUniqueId {
    private final int uniqueId;
    private final Schema valueSchema;

    SchemaAndUniqueId(int uniqueId, @Nonnull Schema valueSchema) {
      this.uniqueId = uniqueId;
      this.valueSchema = valueSchema;
    }

    int getUniqueId() {
      return uniqueId;
    }

    Schema getValueSchema() {
      return valueSchema;
    }
  }
}
