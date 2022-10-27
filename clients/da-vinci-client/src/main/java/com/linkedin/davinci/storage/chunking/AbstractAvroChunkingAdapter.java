package com.linkedin.davinci.storage.chunking;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.LatencyUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;


/**
 * Read compute and write compute chunking adapter
 */
public abstract class AbstractAvroChunkingAdapter<T> implements ChunkingAdapter<ChunkedValueInputStream, T> {
  private static final int UNUSED_INPUT_BYTES_LENGTH = -1;

  protected RecordDeserializer<T> getDeserializer(
      String storeName,
      int writerSchemaId,
      ReadOnlySchemaRepository schemaRepo,
      boolean fastAvroEnabled) {
    return getDeserializer(
        storeName,
        writerSchemaId,
        schemaRepo.getSupersetOrLatestValueSchema(storeName).getId(),
        schemaRepo,
        fastAvroEnabled);
  }

  protected abstract RecordDeserializer<T> getDeserializer(
      String storeName,
      int writerSchemaId,
      int readerSchemaId,
      ReadOnlySchemaRepository schemaRepo,
      boolean fastAvroEnabled);

  @Override
  public T constructValue(
      int writerSchemaId,
      int readerSchemaId,
      byte[] fullBytes,
      int bytesLength,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponse response,
      CompressionStrategy compressionStrategy,
      boolean fastAvroEnabled,
      ReadOnlySchemaRepository schemaRepo,
      String storeName,
      VeniceCompressor compressor) {
    return getByteArrayDecoder(compressionStrategy, response).decode(
        reusedDecoder,
        fullBytes,
        bytesLength,
        reusedValue,
        getDeserializer(storeName, writerSchemaId, readerSchemaId, schemaRepo, fastAvroEnabled),
        response,
        compressor);
  }

  @Override
  public T constructValue(
      int writerSchemaId,
      int readerSchemaId,
      byte[] valueOnlyBytes,
      int offset,
      int bytesLength,
      boolean fastAvroEnabled,
      ReadOnlySchemaRepository schemaRepo,
      String storeName,
      VeniceCompressor veniceCompressor) {
    try {
      return byteArrayDecompressingDecoderValueOnly.decode(
          null,
          valueOnlyBytes,
          offset,
          bytesLength,
          null,
          veniceCompressor,
          getDeserializer(storeName, writerSchemaId, readerSchemaId, schemaRepo, fastAvroEnabled),
          null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addChunkIntoContainer(
      ChunkedValueInputStream chunkedValueInputStream,
      int chunkIndex,
      byte[] valueChunk) {
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
      String storeName,
      VeniceCompressor compressor) {
    return getInputStreamDecoder(response).decode(
        reusedDecoder,
        chunkedValueInputStream,
        UNUSED_INPUT_BYTES_LENGTH,
        reusedValue,
        getDeserializer(storeName, schemaId, schemaRepo, fastAvroEnabled),
        response,
        compressor);
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
      String storeName,
      VeniceCompressor compressor,
      boolean skipCache) {
    return get(
        store,
        schemaRepo.getSupersetOrLatestValueSchema(storeName).getId(),
        partition,
        key,
        isChunked,
        reusedValue,
        reusedDecoder,
        response,
        compressionStrategy,
        fastAvroEnabled,
        schemaRepo,
        storeName,
        compressor,
        skipCache);
  }

  public T get(
      AbstractStorageEngine store,
      int readerSchemaID,
      int partition,
      ByteBuffer key,
      boolean isChunked,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponse response,
      CompressionStrategy compressionStrategy,
      boolean fastAvroEnabled,
      ReadOnlySchemaRepository schemaRepo,
      String storeName,
      VeniceCompressor compressor,
      boolean skipCache) {
    if (isChunked) {
      key = ByteBuffer.wrap(ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key));
    }
    return ChunkingUtils.getFromStorage(
        this,
        store,
        readerSchemaID,
        partition,
        key,
        response,
        reusedValue,
        reusedDecoder,
        compressionStrategy,
        fastAvroEnabled,
        schemaRepo,
        storeName,
        compressor,
        skipCache);
  }

  public T get(
      String storeName,
      AbstractStorageEngine store,
      int partition,
      byte[] key,
      ByteBuffer reusedRawValue,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      boolean isChunked,
      CompressionStrategy compressionStrategy,
      boolean fastAvroEnabled,
      ReadOnlySchemaRepository schemaRepo,
      ReadResponse response,
      VeniceCompressor compressor) {
    if (isChunked) {
      key = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key);
    }
    return ChunkingUtils.getFromStorage(
        this,
        store,
        partition,
        key,
        reusedRawValue,
        reusedValue,
        reusedDecoder,
        response,
        compressionStrategy,
        fastAvroEnabled,
        schemaRepo,
        storeName,
        compressor);
  }

  public T get(
      String storeName,
      AbstractStorageEngine store,
      int userPartition,
      VenicePartitioner partitioner,
      PartitionerConfig partitionerConfig,
      byte[] key,
      ByteBuffer reusedRawValue,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      boolean isChunked,
      CompressionStrategy compressionStrategy,
      boolean fastAvroEnabled,
      ReadOnlySchemaRepository schemaRepo,
      ReadResponse response,
      VeniceCompressor compressor) {
    int subPartition = userPartition;
    int amplificationFactor = partitionerConfig == null ? 1 : partitionerConfig.getAmplificationFactor();
    if (amplificationFactor > 1) {
      int subPartitionOffset = partitioner.getPartitionId(key, amplificationFactor);
      subPartition = userPartition * amplificationFactor + subPartitionOffset;
    }
    return get(
        storeName,
        store,
        subPartition,
        key,
        reusedRawValue,
        reusedValue,
        reusedDecoder,
        isChunked,
        compressionStrategy,
        fastAvroEnabled,
        schemaRepo,
        response,
        compressor);
  }

  public void getByPartialKey(
      String storeName,
      AbstractStorageEngine store,
      int userPartition,
      PartitionerConfig partitionerConfig,
      byte[] keyPrefixBytes,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      RecordDeserializer<GenericRecord> keyRecordDeserializer,
      boolean isChunked,
      CompressionStrategy compressionStrategy,
      boolean fastAvroEnabled,
      ReadOnlySchemaRepository schemaRepo,
      ReadResponse response,
      VeniceCompressor compressor,
      StreamingCallback<GenericRecord, GenericRecord> computingCallback) {

    if (isChunked) {
      throw new VeniceException("Filtering by key prefix is not supported when chunking is enabled.");
    }

    int subPartition;
    int amplificationFactor = partitionerConfig == null ? 1 : partitionerConfig.getAmplificationFactor();

    int subPartitionCount = (userPartition + 1) * amplificationFactor;
    for (subPartition = userPartition * amplificationFactor; subPartition < subPartitionCount; subPartition++) {
      ChunkingUtils.getFromStorageByPartialKey(
          this,
          store,
          subPartition,
          keyPrefixBytes,
          reusedValue,
          keyRecordDeserializer,
          reusedDecoder,
          response,
          compressionStrategy,
          fastAvroEnabled,
          schemaRepo,
          storeName,
          compressor,
          computingCallback);
    }
  }

  private final DecompressingDecoderWrapperValueOnly<byte[], T> byteArrayDecompressingDecoderValueOnly = (
      reusedDecoder,
      bytes,
      offset,
      inputBytesLength,
      reusedValue,
      veniceCompressor,
      deserializer,
      readResponse) -> deserializer
          .deserialize(reusedValue, veniceCompressor.decompress(bytes, offset, inputBytesLength), reusedDecoder);

  private final DecoderWrapper<byte[], T> byteArrayDecoder =
      (reusedDecoder, bytes, inputBytesLength, reusedValue, deserializer, readResponse, compressor) -> deserializer
          .deserialize(
              reusedValue,
              ByteBuffer
                  .wrap(bytes, ValueRecord.SCHEMA_HEADER_LENGTH, inputBytesLength - ValueRecord.SCHEMA_HEADER_LENGTH),
              reusedDecoder);

  private final DecoderWrapper<InputStream, T> decompressingInputStreamDecoder =
      (reusedDecoder, inputStream, inputBytesLength, reusedValue, deserializer, readResponse, compressor) -> {
        try (InputStream decompressedInputStream = compressor.decompress(inputStream)) {
          return deserializer.deserialize(reusedValue, decompressedInputStream, reusedDecoder);
        } catch (IOException e) {
          throw new VeniceException(
              "Failed to decompress, compressionStrategy: " + compressor.getCompressionStrategy().name(),
              e);
        }
      };

  private final DecoderWrapper<byte[], T> decompressingByteArrayDecoder =
      (reusedDecoder, bytes, inputBytesLength, reusedValue, deserializer, readResponse, compressor) -> {
        try (InputStream inputStream = new ByteArrayInputStream(
            bytes,
            ValueRecord.SCHEMA_HEADER_LENGTH,
            inputBytesLength - ValueRecord.SCHEMA_HEADER_LENGTH)) {

          return decompressingInputStreamDecoder.decode(
              reusedDecoder,
              inputStream,
              inputBytesLength,
              reusedValue,
              deserializer,
              readResponse,
              compressor);
        } catch (IOException e) {
          throw new VeniceException(
              "Failed to decompress, compressionStrategy: " + compressor.getCompressionStrategy().name(),
              e);
        }
      };

  private final DecoderWrapper<byte[], T> instrumentedByteArrayDecoder =
      new InstrumentedDecoderWrapper<>(byteArrayDecoder);

  private final DecoderWrapper<byte[], T> instrumentedDecompressingByteArrayDecoder =
      new InstrumentedDecoderWrapper(decompressingByteArrayDecoder);

  private final DecoderWrapper<InputStream, T> instrumentedDecompressingInputStreamDecoder =
      new InstrumentedDecoderWrapper(decompressingInputStreamDecoder);

  private DecoderWrapper<byte[], T> getByteArrayDecoder(
      CompressionStrategy compressionStrategy,
      ReadResponse response) {
    if (compressionStrategy == CompressionStrategy.NO_OP) {
      return (response == null) ? byteArrayDecoder : instrumentedByteArrayDecoder;
    } else {
      return (response == null) ? decompressingByteArrayDecoder : instrumentedDecompressingByteArrayDecoder;
    }
  }

  private DecoderWrapper<InputStream, T> getInputStreamDecoder(ReadResponse response) {
    return (response == null) ? decompressingInputStreamDecoder : instrumentedDecompressingInputStreamDecoder;
  }

  private interface DecoderWrapper<INPUT, OUTPUT> {
    OUTPUT decode(
        BinaryDecoder reusedDecoder,
        INPUT input,
        int inputBytesLength,
        OUTPUT reusedValue,
        RecordDeserializer<OUTPUT> deserializer,
        ReadResponse response,
        VeniceCompressor compressor);
  }

  private interface DecompressingDecoderWrapperValueOnly<INPUT, OUTPUT> {
    OUTPUT decode(
        BinaryDecoder reusedDecoder,
        INPUT input,
        int offset,
        int inputBytesLength,
        OUTPUT reusedValue,
        VeniceCompressor compressor,
        RecordDeserializer<OUTPUT> deserializer,
        ReadResponse response) throws IOException;
  }

  private class InstrumentedDecoderWrapper<INPUT, OUTPUT> implements DecoderWrapper<INPUT, OUTPUT> {
    final private DecoderWrapper<INPUT, OUTPUT> delegate;

    InstrumentedDecoderWrapper(DecoderWrapper<INPUT, OUTPUT> delegate) {
      this.delegate = delegate;
    }

    public OUTPUT decode(
        BinaryDecoder reusedDecoder,
        INPUT input,
        int inputBytesLength,
        OUTPUT reusedValue,
        RecordDeserializer<OUTPUT> deserializer,
        ReadResponse response,
        VeniceCompressor compressor) {
      long deserializeStartTimeInNS = System.nanoTime();
      OUTPUT output =
          delegate.decode(reusedDecoder, input, inputBytesLength, reusedValue, deserializer, response, compressor);
      response.addReadComputeDeserializationLatency(LatencyUtils.getLatencyInMS(deserializeStartTimeInNS));
      return output;
    }
  }
}
