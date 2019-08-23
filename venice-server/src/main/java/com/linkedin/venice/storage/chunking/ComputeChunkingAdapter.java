package com.linkedin.venice.storage.chunking;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.response.ReadResponse;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.serializer.ComputableSerializerDeserializerFactory;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.record.ValueRecord;
import com.linkedin.venice.utils.LatencyUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;


/**
 * Read compute and write compute chunking adapter
 */
public class ComputeChunkingAdapter implements ChunkingAdapter<ChunkedValueInputStream, GenericRecord> {
  private static final ComputeChunkingAdapter COMPUTE_CHUNKING_ADAPTER = new ComputeChunkingAdapter();

  /** Singleton */
  private ComputeChunkingAdapter() {}

  @Override
  public GenericRecord constructValue(
      int schemaId,
      byte[] fullBytes,
      GenericRecord reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponse response,
      CompressionStrategy compressionStrategy,
      boolean fastAvroEnabled,
      ReadOnlySchemaRepository schemaRepo,
      String storeName) {
    InputStream inputStream = new ByteArrayInputStream(
        fullBytes,
        ValueRecord.SCHEMA_HEADER_LENGTH,
        fullBytes.length - ValueRecord.SCHEMA_HEADER_LENGTH);
    return deserialize(
        schemaId,
        inputStream,
        reusedValue,
        reusedDecoder,
        response,
        compressionStrategy,
        fastAvroEnabled,
        schemaRepo,
        storeName);
  }

  @Override
  public void addChunkIntoContainer(ChunkedValueInputStream chunkedValueInputStream, int chunkIndex, byte[] valueChunk) {
    chunkedValueInputStream.setChunk(chunkIndex, valueChunk);
  }

  @Override
  public ChunkedValueInputStream constructChunksContainer(ChunkedValueManifest chunkedValueManifest) {
    return new ChunkedValueInputStream(chunkedValueManifest.keysWithChunkIdSuffix.size());
  }

  @Override
  public GenericRecord constructValue(
      int schemaId,
      ChunkedValueInputStream chunkedValueInputStream,
      GenericRecord reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponse response,
      CompressionStrategy compressionStrategy,
      boolean fastAvroEnabled,
      ReadOnlySchemaRepository schemaRepo,
      String storeName) {
    return deserialize(
        schemaId,
        chunkedValueInputStream,
        reusedValue,
        reusedDecoder,
        response,
        compressionStrategy,
        fastAvroEnabled,
        schemaRepo,
        storeName);
  }

  private GenericRecord deserialize(
      int schemaId,
      InputStream inputStream,
      GenericRecord reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponse response,
      CompressionStrategy compressionStrategy,
      boolean fastAvroEnabled,
      ReadOnlySchemaRepository schemaRepo,
      String storeName) {
    long deserializeStartTimeInNS = System.nanoTime();
    VeniceCompressor compressor = CompressorFactory.getCompressor(compressionStrategy);
    try (InputStream decompressedInputStream = compressor.decompress(inputStream)) {
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(decompressedInputStream, reusedDecoder);
      Schema writerSchema = schemaRepo.getValueSchema(storeName, schemaId).getSchema();
      Schema latestValueSchema = schemaRepo.getLatestValueSchema(storeName).getSchema();
      RecordDeserializer<GenericRecord> deserializer;
      if (fastAvroEnabled) {
        deserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(writerSchema, latestValueSchema);
      } else {
        deserializer = ComputableSerializerDeserializerFactory.getComputableAvroGenericDeserializer(writerSchema, latestValueSchema);
      }
      GenericRecord record = deserializer.deserialize(reusedValue, decoder);

      if (null != response) {
        response.addReadComputeDeserializationLatency(LatencyUtils.getLatencyInMS(deserializeStartTimeInNS));
      } // else, if there is no associated response, then it's not a read compute query

      return record;
    } catch (IOException e) {
      throw new VeniceException("Failed to decompress, compressionStrategy: " + compressionStrategy.name()
          + ", storeName: " + storeName, e);
    }
  }

  public static GenericRecord get(
      AbstractStorageEngine store,
      int partition,
      ByteBuffer key,
      boolean isChunked,
      GenericRecord reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponse response,
      CompressionStrategy compressionStrategy,
      boolean fastAvroEnabled,
      ReadOnlySchemaRepository schemaRepo,
      String storeName) {
    if (isChunked) {
      key = ByteBuffer.wrap(ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key));
    }
    return ChunkingUtils.getFromStorage(COMPUTE_CHUNKING_ADAPTER, store, partition, key, response, reusedValue,
        reusedDecoder, compressionStrategy, fastAvroEnabled, schemaRepo, storeName);
  }
}
