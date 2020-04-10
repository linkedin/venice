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
public abstract class AbstractAvroChunkingAdapter<T> implements ChunkingAdapter<ChunkedValueInputStream, T> {
  protected abstract RecordDeserializer<T> getDeserializer(String storeName, int schemaId, ReadOnlySchemaRepository schemaRepo, boolean fastAvroEnabled);

  @Override
  public T constructValue(
      int schemaId,
      byte[] fullBytes,
      T reusedValue,
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
  public T constructValue(
      int schemaId,
      ChunkedValueInputStream chunkedValueInputStream,
      T reusedValue,
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

  private T deserialize(
      int schemaId,
      InputStream inputStream,
      T reusedValue,
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
      RecordDeserializer<T> deserializer = getDeserializer(storeName, schemaId, schemaRepo, fastAvroEnabled);
      T record = deserializer.deserialize(reusedValue, decoder);

      if (null != response) {
        response.addReadComputeDeserializationLatency(LatencyUtils.getLatencyInMS(deserializeStartTimeInNS));
      } // else, if there is no associated response, then it's not a read compute query

      return record;
    } catch (IOException e) {
      throw new VeniceException("Failed to decompress, compressionStrategy: " + compressionStrategy.name()
          + ", storeName: " + storeName, e);
    }
  }

  public T get(
      AbstractStorageEngine store,
      int partition,
      ByteBuffer key,
      boolean isChunked,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponse response,
      CompressionStrategy compressionStrategy,
      boolean fastAvroEnabled,
      ReadOnlySchemaRepository schemaRepo,
      String storeName) {
    if (isChunked) {
      key = ByteBuffer.wrap(ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key));
    }
    return ChunkingUtils.getFromStorage(this, store, partition, key, response, reusedValue,
        reusedDecoder, compressionStrategy, fastAvroEnabled, schemaRepo, storeName);
  }

  public T get(
      AbstractStorageEngine store,
      int partition,
      byte[] key,
      boolean isChunked,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponse response,
      CompressionStrategy compressionStrategy,
      boolean fastAvroEnabled,
      ReadOnlySchemaRepository schemaRepo,
      String storeName) {
    if (isChunked) {
      key = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key);
    }
    return ChunkingUtils.getFromStorage(this, store, partition, key, response, reusedValue,
        reusedDecoder, compressionStrategy, fastAvroEnabled, schemaRepo, storeName);
  }
}
