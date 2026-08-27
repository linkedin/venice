package com.linkedin.venice.spark.chunk;

import static com.linkedin.venice.spark.SparkConstants.CHUNKED_KEY_SUFFIX_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA_WITH_SCHEMA_ID;
import static com.linkedin.venice.spark.SparkConstants.MESSAGE_TYPE_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.OFFSET_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_VERSION_ID_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.SCHEMA_ID_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;

import com.linkedin.venice.common.ChunkAssembler;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.spark.input.kafka.ttl.SparkChunkedPayloadTTLFilter;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;


/**
 * Spark adapter for ChunkAssembler that handles chunked values and RMDs.
 * Converts Spark Rows to the format expected by ChunkAssembler, assembles chunks,
 * and returns the result as a Spark Row.
 */
public class SparkChunkAssembler implements Serializable, AutoCloseable {
  private static final long serialVersionUID = 1L;

  private static final RecordSerializer<KafkaInputMapperValue> SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KafkaInputMapperValue.SCHEMA$);

  private final boolean isRmdChunkingEnabled;
  private final boolean isTTLFilteringEnabled;
  private final VeniceProperties filterProperties;

  private transient ChunkAssembler chunkAssembler;
  private transient SparkChunkedPayloadTTLFilter ttlFilter;

  public SparkChunkAssembler(boolean isRmdChunkingEnabled) {
    this(isRmdChunkingEnabled, false, null);
  }

  public SparkChunkAssembler(
      boolean isRmdChunkingEnabled,
      boolean isTTLFilteringEnabled,
      VeniceProperties filterProperties) {
    this.isRmdChunkingEnabled = isRmdChunkingEnabled;
    this.isTTLFilteringEnabled = isTTLFilteringEnabled;
    this.filterProperties = filterProperties;
  }

  /**
   * Get or lazily initialize the ChunkAssembler.
   * Transient field needs to be recreated after deserialization.
   */
  private ChunkAssembler getChunkAssembler() {
    if (chunkAssembler == null) {
      chunkAssembler = new ChunkAssembler(isRmdChunkingEnabled);
    }
    return chunkAssembler;
  }

  /**
   * Get or lazily initialize the TTL filter.
   * Transient field needs to be recreated after deserialization.
   */
  private SparkChunkedPayloadTTLFilter getTTLFilter() throws IOException {
    if (ttlFilter == null) {
      ttlFilter = new SparkChunkedPayloadTTLFilter(filterProperties);
    }
    return ttlFilter;
  }

  /**
   * Assemble chunks for a single key.
   * If TTL filtering is enabled, also filters the assembled record.
   *
   * @param keyBytes The key bytes
   * @param rows Iterator of rows for this key (MUST be sorted by offset DESC - highest offset first)
   * @return Assembled row with DEFAULT_SCHEMA_WITH_SCHEMA_ID schema, or null if DELETE, orphan chunks
   *         (chunks with no corresponding manifest), or filtered by TTL
   * @throws RuntimeException (e.g. VeniceException, IllegalArgumentException, IllegalStateException) if the
   *         assembled record is malformed - for example missing value/RMD chunks, a byte-count mismatch
   *         against the manifest, an offset-ordering violation, or an unrecognized schema id/message type.
   *         This intentionally mirrors {@code VeniceKafkaInputReducer#extractChunkedMessage}, which lets the
   *         same exceptions from {@code ChunkAssembler#assembleAndGetValue} propagate and fail the MR task
   *         rather than silently dropping the record.
   */
  public Row assembleChunks(byte[] keyBytes, Iterator<Row> rows) {
    // Handle empty iterator early
    if (!rows.hasNext()) {
      return null;
    }

    Iterator<byte[]> valueIterator = new RowToSerializedValueIterator(rows);

    // Do not catch-and-swallow exceptions here: every exception ChunkAssembler#assembleAndGetValue throws
    // (missing chunks, byte-count mismatch, ordering violation, unrecognized schema id) represents a genuinely
    // corrupt/incomplete record, not a legitimate "no data" outcome. Legitimate null results (DELETE, orphan
    // chunks with no manifest) are already returned as `null` by assembleAndGetValue below, without throwing.
    ChunkAssembler.ValueBytesAndSchemaId assembled = getChunkAssembler().assembleAndGetValue(keyBytes, valueIterator);

    if (assembled == null) {
      // Latest record is DELETE with no RMD, or no manifest was ever found (orphan chunks)
      return null;
    }

    byte[] rmdBytes = null;
    ByteBuffer rmdPayload = assembled.getReplicationMetadataPayload();
    if (rmdPayload != null && rmdPayload.hasRemaining()) {
      rmdBytes = ByteUtils.extractByteArray(rmdPayload);
    }

    // Convert result back to Spark Row
    Row assembledRow = new GenericRowWithSchema(
        new Object[] { keyBytes, assembled.getBytes(), rmdBytes, assembled.getSchemaID(),
            assembled.getReplicationMetadataVersionId() },
        DEFAULT_SCHEMA_WITH_SCHEMA_ID);

    // Apply post-assembly TTL filtering if enabled (similar to MR reducer)
    if (isTTLFilteringEnabled) {
      try {
        SparkChunkedPayloadTTLFilter.FilterResult result = getTTLFilter().filterAndUpdate(assembledRow);
        if (result.shouldFilter()) {
          // Record is completely stale, filter it out
          return null;
        }
        // Check if the wrapper was modified (PARTIALLY_UPDATED case)
        SparkChunkedPayloadTTLFilter.AssembledRowWrapper wrapper = result.getWrapper();
        byte[] modifiedValue = wrapper.getValue();
        byte[] modifiedRmd = wrapper.getRmd();

        // Create new row with potentially modified payloads
        return new GenericRowWithSchema(
            new Object[] { keyBytes, modifiedValue, modifiedRmd, wrapper.getSchemaID(),
                wrapper.getReplicationMetadataVersionId() },
            DEFAULT_SCHEMA_WITH_SCHEMA_ID);
      } catch (IOException e) {
        throw new RuntimeException("TTL filtering failed for assembled record", e);
      }
    }

    return assembledRow;
  }

  /**
   * Releases resources (e.g. the post-assembly TTL filter's schema reader) held by this assembler.
   * Safe to call even if the assembler was never used (transient fields still null).
   */
  @Override
  public void close() {
    if (ttlFilter != null) {
      ttlFilter.close();
    }
  }

  /**
   * Inner class to adapt Spark Row iterator to byte[] iterator.
   * ChunkAssembler.assembleAndGetValue() expects an Iterator of byte[] where each byte[]
   * is a serialized KafkaInputMapperValue.
   */
  private static class RowToSerializedValueIterator implements Iterator<byte[]> {
    private final Iterator<Row> rowIterator;

    RowToSerializedValueIterator(Iterator<Row> rowIterator) {
      this.rowIterator = rowIterator;
    }

    @Override
    public boolean hasNext() {
      return rowIterator.hasNext();
    }

    @Override
    public byte[] next() {
      Row row = rowIterator.next();

      // Create KafkaInputMapperValue from Spark Row
      KafkaInputMapperValue mapperValue = new KafkaInputMapperValue();
      mapperValue.schemaId = row.getAs(SCHEMA_ID_COLUMN_NAME);
      mapperValue.offset = row.getAs(OFFSET_COLUMN_NAME);

      int messageType = row.getAs(MESSAGE_TYPE_COLUMN_NAME);
      if (messageType == MessageType.PUT.getValue()) {
        mapperValue.valueType = MapperValueType.PUT;
      } else if (messageType == MessageType.DELETE.getValue()) {
        mapperValue.valueType = MapperValueType.DELETE;
      } else {
        throw new IllegalStateException(
            "Unexpected message type: " + messageType + ". Only PUT(0) and DELETE(1) are supported. "
                + "CONTROL_MESSAGE(2), UPDATE(3), and GLOBAL_RT_DIV(4) should have been filtered by "
                + "SparkPubSubInputPartitionReader. This indicates a bug in the upstream filtering.");
      }

      // Value field
      byte[] valueBytes = row.getAs(VALUE_COLUMN_NAME);
      mapperValue.value = valueBytes != null ? ByteBuffer.wrap(valueBytes) : ByteBuffer.allocate(0);

      // RMD fields
      mapperValue.replicationMetadataVersionId = row.getAs(RMD_VERSION_ID_COLUMN_NAME);
      byte[] rmdBytes = row.getAs(RMD_COLUMN_NAME);
      mapperValue.replicationMetadataPayload = rmdBytes != null ? ByteBuffer.wrap(rmdBytes) : ByteBuffer.allocate(0);

      // Chunked key suffix: used by ChunkAssembler to match chunks with their manifest
      // This is the extracted suffix from the composite key (user key + suffix)
      byte[] chunkedKeySuffixBytes = row.getAs(CHUNKED_KEY_SUFFIX_COLUMN_NAME);
      mapperValue.chunkedKeySuffix =
          chunkedKeySuffixBytes != null ? ByteBuffer.wrap(chunkedKeySuffixBytes) : ByteBuffer.allocate(0);

      // Serialize to byte[] (format expected by ChunkAssembler)
      return SERIALIZER.serialize(mapperValue);
    }
  }
}
