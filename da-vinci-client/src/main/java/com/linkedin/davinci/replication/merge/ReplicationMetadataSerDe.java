package com.linkedin.davinci.replication.merge;

import com.linkedin.davinci.replication.ReplicationMetadataWithValueSchemaId;
import com.linkedin.davinci.serialization.avro.MapOrderingPreservingSerDeFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaEntry;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.OptimizedBinaryDecoder;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;
import org.apache.commons.lang3.Validate;


/**
 * This class is responsible for serialization and deserialization related tasks. Specifically 3 things:
 *  1. Deserialize RMD from bytes.
 *  2. Serialize RMD record to bytes.
 *  3. Get RMD schema given its value schema ID.
 */
public class ReplicationMetadataSerDe {
  private final String storeName;
  private final ReadOnlySchemaRepository schemaRepository;
  private final int rmdVersionId;
  private final Map<Integer, Schema> valueSchemaIdToRmdSchemaMap;
  private final Map<Schema, RecordDeserializer<GenericRecord>> schemaToDeserializerMap;

  public ReplicationMetadataSerDe(ReadOnlySchemaRepository schemaRepository, String storeName, int rmdVersionId) {
    this.schemaRepository = schemaRepository;
    this.storeName = storeName;
    this.rmdVersionId = rmdVersionId;
    this.valueSchemaIdToRmdSchemaMap = new VeniceConcurrentHashMap<>();
    this.schemaToDeserializerMap = new VeniceConcurrentHashMap<>();
  }

  /**
   * @param valueSchemaIdPrependedBytes The raw bytes with value schema ID prepended.
   * @return A {@link ReplicationMetadataWithValueSchemaId} object composed by extracting the value schema ID from the
   *    * header of the replication metadata.
   */
  public ReplicationMetadataWithValueSchemaId deserializeValueSchemaIdPrependedRmdBytes(byte[] valueSchemaIdPrependedBytes) {
    Validate.notNull(valueSchemaIdPrependedBytes);
    ByteBuffer rmdWithValueSchemaID = ByteBuffer.wrap(valueSchemaIdPrependedBytes);
    final int valueSchemaId = rmdWithValueSchemaID.getInt();
    OptimizedBinaryDecoder binaryDecoder =
        OptimizedBinaryDecoderFactory.defaultFactory().createOptimizedBinaryDecoder(
            rmdWithValueSchemaID.array(), // bytes of replication metadata with NO value schema ID.
            rmdWithValueSchemaID.position(),
            rmdWithValueSchemaID.remaining()
        );
    GenericRecord rmdRecord = getRmdDeserializer(valueSchemaId).deserialize(binaryDecoder);

    return new ReplicationMetadataWithValueSchemaId(
        valueSchemaId,
        rmdRecord
    );
  }

  public ByteBuffer serializeReplicationMetadata(final int valueSchemaId, GenericRecord replicationMetadataRecord) {
    byte[] replicationMetadataBytes = getRmdSerializer(valueSchemaId)
        .serialize(replicationMetadataRecord, AvroSerializer.REUSE.get());
    return ByteBuffer.wrap(replicationMetadataBytes);
  }

  public Schema getReplicationMetadataSchema(final int valueSchemaId) {
    return valueSchemaIdToRmdSchemaMap.computeIfAbsent(valueSchemaId,  id -> {
      ReplicationMetadataSchemaEntry
          replicationMetadataSchemaEntry = schemaRepository.getReplicationMetadataSchema(storeName, valueSchemaId, rmdVersionId);
      if (replicationMetadataSchemaEntry == null) {
        throw new VeniceException("Unable to fetch replication metadata schema from schema repository");
      }
      return replicationMetadataSchemaEntry.getSchema();
    });
  }

  private RecordDeserializer<GenericRecord> getRmdDeserializer(final int valueSchemaId) {
    Schema replicationMetadataSchema = getReplicationMetadataSchema(valueSchemaId);
    return schemaToDeserializerMap.computeIfAbsent(replicationMetadataSchema,
        schema -> MapOrderingPreservingSerDeFactory.getDeserializer(replicationMetadataSchema, replicationMetadataSchema));
  }

  private RecordSerializer<GenericRecord> getRmdSerializer(int valueSchemaId) {
    Schema replicationMetadataSchema = getReplicationMetadataSchema(valueSchemaId);
    return MapOrderingPreservingSerDeFactory.getSerializer(replicationMetadataSchema);
  }
}
