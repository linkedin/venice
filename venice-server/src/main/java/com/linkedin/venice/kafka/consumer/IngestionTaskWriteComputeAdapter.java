package com.linkedin.venice.kafka.consumer;

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
 * Utils class that handles operations related to write compute in {@link StoreIngestionTask}.
 * TODO: We make a couple of additional copies here. Optimize the process to reduce the footprint.
 */
public class IngestionTaskWriteComputeAdapter {
  private String storeName;
  private ReadOnlySchemaRepository schemaRepo;

  private VeniceConcurrentHashMap<Pair<Integer, Integer>, WriteComputeAdapter> idToWriteComputeAdapterMap =
      new VeniceConcurrentHashMap<>();
  private VeniceConcurrentHashMap<Pair<Integer, Integer>, AvroGenericDeserializer<GenericRecord>> idToDerivedSchemaDeserializerMap =
      new VeniceConcurrentHashMap<>();
  private VeniceConcurrentHashMap<Integer, AvroSerializer> valueSchemaSerializerMap = new VeniceConcurrentHashMap<>();

  public IngestionTaskWriteComputeAdapter(String storeName, ReadOnlySchemaRepository schemaRepo) {
    this.storeName = storeName;
    this.schemaRepo = schemaRepo;
  }

  public byte[] getUpdatedValueBytes(GenericRecord originalValue, ByteBuffer writeComputeBytes, int valueSchemaId, int derivedSchemaId,
      int partitionId) {
    GenericRecord writeComputeRecord = getWriteComputeUpdateDeserializer(valueSchemaId, derivedSchemaId)
        .deserialize(writeComputeBytes);
    GenericRecord updatedValue = getWriteComputeAdapter(valueSchemaId, derivedSchemaId)
        .updateRecord(originalValue, writeComputeRecord);

    return getValueSerializer(valueSchemaId).serialize(updatedValue);
  }

  private WriteComputeAdapter getWriteComputeAdapter(int valueSchemaId, int derivedSchemaId) {
    return idToWriteComputeAdapterMap.computeIfAbsent(new  Pair<>(valueSchemaId, derivedSchemaId),
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

  private AvroSerializer getValueSerializer(int valueSchemaId) {
    return valueSchemaSerializerMap.computeIfAbsent(valueSchemaId,
        id -> new AvroSerializer(getValueSchema(valueSchemaId)));
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
