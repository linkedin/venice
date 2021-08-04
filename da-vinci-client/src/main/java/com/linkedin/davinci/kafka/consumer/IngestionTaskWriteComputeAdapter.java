package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.storage.chunking.GenericRecordChunkingAdapter;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.WriteComputeAdapter;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Utils class that handles operations related to write compute in {@link LeaderFollowerStoreIngestionTask}.
 * TODO: We make a couple of additional copies here. Optimize the process to reduce the footprint.
 */
public class IngestionTaskWriteComputeAdapter {
  private String storeName;
  private ReadOnlySchemaRepository schemaRepo;

  private VeniceConcurrentHashMap<Pair<Integer, Integer>, WriteComputeAdapter> idToWriteComputeAdapterMap =
      new VeniceConcurrentHashMap<>();
  private VeniceConcurrentHashMap<Pair<Integer, Integer>, AvroGenericDeserializer<GenericRecord>> idToDerivedSchemaDeserializerMap =
      new VeniceConcurrentHashMap<>();
  private VeniceConcurrentHashMap<Schema, AvroSerializer> valueSchemaSerializerMap = new VeniceConcurrentHashMap<>();

  public IngestionTaskWriteComputeAdapter(String storeName, ReadOnlySchemaRepository schemaRepo) {
    this.storeName = storeName;
    this.schemaRepo = schemaRepo;
  }

  /**
   * Apply write-compute operation on top of original value. A few of different schemas are used here
   * and should be handled carefully.
   *
   * @param originalValue original value read from DB. It can be null if the value is not existent. original
   *                      value is read via {@link GenericRecordChunkingAdapter#INSTANCE#get()}}
   *                      and deserialized by the latest value schema. In some cases, this can be different
   *                      from "valueSchemaId"/"derivedSchemaId".
   * @param writeComputeBytes serialized write-compute operation.
   * @param valueSchemaId value schema id that this write-compute operation is associated with.
   *                      It's read from Kafka record.
   * @param derivedSchemaId derived schema id that this write-compute operation is associated with.
   *                        It's read from Kafka record
   */
  public byte[] getUpdatedValueBytes(GenericRecord originalValue, ByteBuffer writeComputeBytes, int valueSchemaId,
      int derivedSchemaId) {
    //If original value is not null, extracts schema and use it to serialize the updated record back to disk.
    // We want to make serialization schema consistent
    Schema originalValueSchema;
    if (originalValue != null) {
      originalValueSchema = originalValue.getSchema();
    } else {
      originalValueSchema = getValueSchema(valueSchemaId);
    }

    GenericRecord writeComputeRecord = deserializeWriteComputeRecord(writeComputeBytes, valueSchemaId, derivedSchemaId);
    GenericRecord updatedValue = getWriteComputeAdapter(valueSchemaId, derivedSchemaId)
        .updateRecord(originalValue, writeComputeRecord);

    //If write compute is enabled and the record is deleted, the updatedValue will be null.
    if (updatedValue == null) {
      return null;
    }

    return getValueSerializer(originalValueSchema).serialize(updatedValue);
  }

  public GenericRecord deserializeWriteComputeRecord(ByteBuffer writeComputeBytes, int valueSchemaId,
      int derivedSchemaId) {
    return getWriteComputeUpdateDeserializer(valueSchemaId, derivedSchemaId)
        .deserialize(writeComputeBytes);
  }

  private WriteComputeAdapter getWriteComputeAdapter(int valueSchemaId, int derivedSchemaId) {
    return idToWriteComputeAdapterMap.computeIfAbsent(new Pair<>(valueSchemaId, derivedSchemaId),
        idPair -> WriteComputeAdapter.getWriteComputeAdapter(getValueSchema(valueSchemaId),
            getDerivedSchema(valueSchemaId, derivedSchemaId)));
  }

  private AvroGenericDeserializer<GenericRecord> getWriteComputeUpdateDeserializer(int valueSchemaId, int derivedSchemaId) {
    return idToDerivedSchemaDeserializerMap.computeIfAbsent(new Pair<>(valueSchemaId, derivedSchemaId),
        idPair -> {
          Schema derivedSchema = getDerivedSchema(valueSchemaId, derivedSchemaId);
          return new AvroGenericDeserializer(derivedSchema, derivedSchema);
        });
  }

  private AvroSerializer getValueSerializer(Schema schema) {
    return valueSchemaSerializerMap.computeIfAbsent(schema,
        id -> new AvroSerializer(schema));
  }

  private Schema getValueSchema(int valueSchemaId) {
    Schema valueSchema = schemaRepo.getValueSchema(storeName, valueSchemaId).getSchema();
    if (valueSchema == null) {
      throw new VeniceException(String.format("Cannot find value schema for store: %s, value schema id: %d",
          storeName, valueSchemaId));
    }

    return valueSchema;
  }

  private Schema getDerivedSchema(int valueSchemaId, int derivedSchemaId) {
    Schema derivedSchema = schemaRepo.getDerivedSchema(storeName, valueSchemaId, derivedSchemaId).getSchema();
    if (derivedSchema ==  null) {
      throw new VeniceException(String.format("Cannot find derived schema for store: %s, value schema id: %d,"
          + " derived schema id: %d", storeName, valueSchemaId, derivedSchemaId));
    }

    return derivedSchema;
  }
}
