package com.linkedin.davinci.replication.merge;

import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.avro.MapOrderingPreservingSerDeFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
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
  private final Map<Integer, Schema> valueSchemaIdToRmdSchemaMap;
  private final Map<WriterReaderSchemaIDs, RecordDeserializer<GenericRecord>> schemaIdToDeserializerMap;

  public RmdSerDe(StringAnnotatedStoreSchemaCache annotatedStoreSchemaCache, int rmdVersionId) {
    this.annotatedStoreSchemaCache = annotatedStoreSchemaCache;
    this.rmdVersionId = rmdVersionId;
    this.valueSchemaIdToRmdSchemaMap = new VeniceConcurrentHashMap<>();
    this.schemaIdToDeserializerMap = new VeniceConcurrentHashMap<>();
  }

  /**
   * @param valueSchemaIdPrependedBytes The raw bytes with value schema ID prepended.
   * @return A {@link RmdWithValueSchemaId} object composed by extracting the value schema ID from the
   *    * header of the replication metadata.
   */
  public RmdWithValueSchemaId deserializeValueSchemaIdPrependedRmdBytes(byte[] valueSchemaIdPrependedBytes) {
    Validate.notNull(valueSchemaIdPrependedBytes);
    ByteBuffer rmdWithValueSchemaID = ByteBuffer.wrap(valueSchemaIdPrependedBytes);
    final int valueSchemaId = rmdWithValueSchemaID.getInt();
    OptimizedBinaryDecoder binaryDecoder = OptimizedBinaryDecoderFactory.defaultFactory()
        .createOptimizedBinaryDecoder(
            rmdWithValueSchemaID.array(), // bytes of replication metadata with NO value schema ID.
            rmdWithValueSchemaID.position(),
            rmdWithValueSchemaID.remaining());
    GenericRecord rmdRecord = getRmdDeserializer(valueSchemaId, valueSchemaId).deserialize(binaryDecoder);
    return new RmdWithValueSchemaId(valueSchemaId, rmdVersionId, rmdRecord);
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
    byte[] rmdBytes = getRmdSerializer(valueSchemaId).serialize(rmdRecord);
    return ByteBuffer.wrap(rmdBytes);
  }

  public Schema getRmdSchema(final int valueSchemaId) {
    return valueSchemaIdToRmdSchemaMap.computeIfAbsent(valueSchemaId, id -> {
      RmdSchemaEntry rmdSchemaEntry = annotatedStoreSchemaCache.getRmdSchema(valueSchemaId, rmdVersionId);
      if (rmdSchemaEntry == null) {
        throw new VeniceException("Unable to fetch replication metadata schema from schema repository");
      }
      return rmdSchemaEntry.getSchema();
    });
  }

  private RecordDeserializer<GenericRecord> getRmdDeserializer(final int writerSchemaID, final int readerSchemaID) {
    final WriterReaderSchemaIDs writerReaderSchemaIDs = new WriterReaderSchemaIDs(writerSchemaID, readerSchemaID);
    return schemaIdToDeserializerMap.computeIfAbsent(writerReaderSchemaIDs, schemaIDs -> {
      Schema rmdWriterSchema = getRmdSchema(schemaIDs.getWriterSchemaID());
      Schema rmdReaderSchema = getRmdSchema(schemaIDs.getReaderSchemaID());
      return MapOrderingPreservingSerDeFactory.getDeserializer(rmdWriterSchema, rmdReaderSchema);
    });
  }

  private RecordSerializer<GenericRecord> getRmdSerializer(int valueSchemaId) {
    Schema replicationMetadataSchema = getRmdSchema(valueSchemaId);
    return MapOrderingPreservingSerDeFactory.getSerializer(replicationMetadataSchema);
  }

  /**
   * A POJO containing a write schema ID and a reader schema ID.
   */
  private static class WriterReaderSchemaIDs {
    private final int writerSchemaID;
    private final int readerSchemaID;

    WriterReaderSchemaIDs(int writerSchemaID, int readerSchemaID) {
      this.writerSchemaID = writerSchemaID;
      this.readerSchemaID = readerSchemaID;
    }

    int getWriterSchemaID() {
      return writerSchemaID;
    }

    int getReaderSchemaID() {
      return readerSchemaID;
    }

    @Override
    public int hashCode() {
      return Objects.hash(writerSchemaID, readerSchemaID);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof WriterReaderSchemaIDs)) {
        return false;
      }
      WriterReaderSchemaIDs other = (WriterReaderSchemaIDs) o;
      return this.writerSchemaID == other.writerSchemaID && this.readerSchemaID == other.readerSchemaID;
    }

    @Override
    public String toString() {
      return String.format("writer_schema_ID = %d, reader_schema_ID = %d", writerSchemaID, readerSchemaID);
    }
  }
}
