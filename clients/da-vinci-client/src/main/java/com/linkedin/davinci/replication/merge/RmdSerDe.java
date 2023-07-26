package com.linkedin.davinci.replication.merge;

import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.avro.MapOrderingPreservingSerDeFactory;
import com.linkedin.venice.utils.SparseConcurrentList;
import com.linkedin.venice.utils.collections.BiIntKeyCache;
import java.nio.ByteBuffer;
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
@Threadsafe
public class RmdSerDe {
  private final StringAnnotatedStoreSchemaCache annotatedStoreSchemaCache;
  private final int rmdVersionId;
  private final SparseConcurrentList<Schema> rmdSchemaIndexedByValueSchemaId;
  private final SparseConcurrentList<RecordSerializer<GenericRecord>> rmdSerializerIndexedByValueSchemaId;
  private final BiIntKeyCache<RecordDeserializer<GenericRecord>> deserializerCache;

  public RmdSerDe(StringAnnotatedStoreSchemaCache annotatedStoreSchemaCache, int rmdVersionId) {
    this.annotatedStoreSchemaCache = annotatedStoreSchemaCache;
    this.rmdVersionId = rmdVersionId;
    this.rmdSchemaIndexedByValueSchemaId = new SparseConcurrentList<>();
    this.rmdSerializerIndexedByValueSchemaId = new SparseConcurrentList<>();
    this.deserializerCache = new BiIntKeyCache<>((writerSchemaId, readerSchemaId) -> {
      Schema rmdWriterSchema = getRmdSchema(writerSchemaId);
      Schema rmdReaderSchema = getRmdSchema(readerSchemaId);
      return MapOrderingPreservingSerDeFactory.getDeserializer(rmdWriterSchema, rmdReaderSchema);
    });
  }

  /**
   * This method takes in the RMD bytes with prepended value schema ID and a {@link RmdWithValueSchemaId} container object.
   * It will deserialize the RMD bytes into RMD record and fill the passed-in container.
   */
  public void deserializeValueSchemaIdPrependedRmdBytes(
      byte[] valueSchemaIdPrependedBytes,
      RmdWithValueSchemaId rmdWithValueSchemaId) {
    Validate.notNull(valueSchemaIdPrependedBytes);
    ByteBuffer rmdWithValueSchemaID = ByteBuffer.wrap(valueSchemaIdPrependedBytes);
    final int valueSchemaId = rmdWithValueSchemaID.getInt();
    OptimizedBinaryDecoder binaryDecoder = OptimizedBinaryDecoderFactory.defaultFactory()
        .createOptimizedBinaryDecoder(
            rmdWithValueSchemaID.array(), // bytes of replication metadata with NO value schema ID.
            rmdWithValueSchemaID.position(),
            rmdWithValueSchemaID.remaining());
    GenericRecord rmdRecord = getRmdDeserializer(valueSchemaId, valueSchemaId).deserialize(binaryDecoder);
    rmdWithValueSchemaId.setValueSchemaID(valueSchemaId);
    rmdWithValueSchemaId.setRmdProtocolVersionID(rmdVersionId);
    rmdWithValueSchemaId.setRmdRecord(rmdRecord);
  }

  /**
   * Given a value schema ID {@param valueSchemaID} and RMD bytes {@param rmdBytes}, find the RMD schema that corresponds
   * to the given value schema ID and use that RMD schema to deserialize RMD bytes in a RMD record.
   *
   */
  public GenericRecord deserializeRmdBytes(final int writerSchemaID, final int readerSchemaID, ByteBuffer rmdBytes) {
    return getRmdDeserializer(writerSchemaID, readerSchemaID).deserialize(rmdBytes);
  }

  public ByteBuffer serializeRmdRecord(final int valueSchemaId, GenericRecord rmdRecord) {
    RecordSerializer<GenericRecord> rmdSerializer =
        this.rmdSerializerIndexedByValueSchemaId.computeIfAbsent(valueSchemaId, this::generateRmdSerializer);
    byte[] rmdBytes = rmdSerializer.serialize(rmdRecord);
    return ByteBuffer.wrap(rmdBytes);
  }

  public Schema getRmdSchema(final int valueSchemaId) {
    return this.rmdSchemaIndexedByValueSchemaId.computeIfAbsent(valueSchemaId, this::generateRmdSchema);
  }

  private Schema generateRmdSchema(final int valueSchemaId) {
    RmdSchemaEntry rmdSchemaEntry = this.annotatedStoreSchemaCache.getRmdSchema(valueSchemaId, this.rmdVersionId);
    if (rmdSchemaEntry == null) {
      throw new VeniceException("Unable to fetch replication metadata schema from schema repository");
    }
    return rmdSchemaEntry.getSchema();
  }

  private RecordDeserializer<GenericRecord> getRmdDeserializer(final int writerSchemaID, final int readerSchemaID) {
    return this.deserializerCache.get(writerSchemaID, readerSchemaID);
  }

  private RecordSerializer<GenericRecord> generateRmdSerializer(int valueSchemaId) {
    Schema replicationMetadataSchema = getRmdSchema(valueSchemaId);
    return MapOrderingPreservingSerDeFactory.getSerializer(replicationMetadataSchema);
  }
}
