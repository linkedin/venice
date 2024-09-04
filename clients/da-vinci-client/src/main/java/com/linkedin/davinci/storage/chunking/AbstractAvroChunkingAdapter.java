package com.linkedin.davinci.storage.chunking;

import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.LatencyUtils;
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

  @Override
  public T constructValue(
      byte[] fullBytes,
      int bytesLength,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponseStats response,
      int writerSchemaId,
      int readerSchemaId,
      StoreDeserializerCache<T> storeDeserializerCache,
      VeniceCompressor compressor) {
    return getByteArrayDecoder(compressor.getCompressionStrategy(), response).decode(
        reusedDecoder,
        fullBytes,
        bytesLength,
        reusedValue,
        storeDeserializerCache.getDeserializer(writerSchemaId, readerSchemaId),
        response,
        compressor);
  }

  @Override
  public T constructValue(
      byte[] valueOnlyBytes,
      int offset,
      int bytesLength,
      RecordDeserializer<T> recordDeserializer,
      VeniceCompressor veniceCompressor) {
    try {
      return byteArrayDecompressingDecoderValueOnly
          .decode(null, valueOnlyBytes, offset, bytesLength, null, veniceCompressor, recordDeserializer, null);
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
      ChunkedValueInputStream chunkedValueInputStream,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponseStats response,
      int writerSchemaId,
      int readerSchemaId,
      StoreDeserializerCache<T> storeDeserializerCache,
      VeniceCompressor compressor) {
    return getInputStreamDecoder(response).decode(
        reusedDecoder,
        chunkedValueInputStream,
        UNUSED_INPUT_BYTES_LENGTH,
        reusedValue,
        storeDeserializerCache.getDeserializer(writerSchemaId, readerSchemaId),
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
      ReadResponseStats response,
      int readerSchemaId,
      StoreDeserializerCache<T> storeDeserializerCache,
      VeniceCompressor compressor,
      ChunkedValueManifestContainer manifestContainer) {
    if (isChunked) {
      key = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key);
    }
    return ChunkingUtils.getFromStorage(
        this,
        store::get,
        store.getStoreVersionName(),
        partition,
        key,
        response,
        reusedValue,
        reusedDecoder,
        readerSchemaId,
        storeDeserializerCache,
        compressor,
        manifestContainer);
  }

  public ByteBufferValueRecord<T> getWithSchemaId(
      AbstractStorageEngine store,
      int partition,
      ByteBuffer key,
      boolean isChunked,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      StoreDeserializerCache<T> storeDeserializerCache,
      VeniceCompressor compressor,
      ChunkedValueManifestContainer manifestContainer) {
    if (isChunked) {
      key = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key);
    }
    return ChunkingUtils.getValueAndSchemaIdFromStorage(
        this,
        store,
        partition,
        key,
        reusedValue,
        reusedDecoder,
        storeDeserializerCache,
        compressor,
        manifestContainer);
  }

  public T get(
      AbstractStorageEngine store,
      int partition,
      byte[] key,
      ByteBuffer reusedRawValue,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      boolean isChunked,
      ReadResponseStats response,
      int readerSchemaId,
      StoreDeserializerCache<T> storeDeserializerCache,
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
        readerSchemaId,
        storeDeserializerCache,
        compressor);
  }

  public void getByPartialKey(
      AbstractStorageEngine store,
      int userPartition,
      byte[] keyPrefixBytes,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      RecordDeserializer<GenericRecord> keyRecordDeserializer,
      boolean isChunked,
      ReadResponseStats response,
      int readerSchemaId,
      StoreDeserializerCache<T> storeDeserializerCache,
      VeniceCompressor compressor,
      StreamingCallback<GenericRecord, GenericRecord> computingCallback) {

    if (isChunked) {
      throw new VeniceException("Filtering by key prefix is not supported when chunking is enabled.");
    }

    ChunkingUtils.getFromStorageByPartialKey(
        this,
        store,
        userPartition,
        keyPrefixBytes,
        reusedValue,
        keyRecordDeserializer,
        reusedDecoder,
        response,
        readerSchemaId,
        storeDeserializerCache,
        compressor,
        computingCallback);
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
        try {
          // Fetch the schema id.
          int schemaId = ByteUtils.readInt(bytes, 0);
          return deserializer.deserialize(
              reusedValue,
              compressor.decompressAndPrependSchemaHeader(
                  bytes,
                  ValueRecord.SCHEMA_HEADER_LENGTH,
                  inputBytesLength - ValueRecord.SCHEMA_HEADER_LENGTH,
                  schemaId),
              reusedDecoder);
        } catch (IOException e) {
          throw new VeniceException(
              "Failed to decompress, compressionStrategy: " + compressor.getCompressionStrategy().name(),
              e);
        }
      };

  private final DecoderWrapper<byte[], T> instrumentedByteArrayDecoder =
      new InstrumentedDecoderWrapper<>(byteArrayDecoder);

  private final DecoderWrapper<byte[], T> instrumentedDecompressingByteArrayDecoder =
      new InstrumentedDecoderWrapper<>(decompressingByteArrayDecoder);

  private final DecoderWrapper<InputStream, T> instrumentedDecompressingInputStreamDecoder =
      new InstrumentedDecoderWrapper<>(decompressingInputStreamDecoder);

  private DecoderWrapper<byte[], T> getByteArrayDecoder(
      CompressionStrategy compressionStrategy,
      ReadResponseStats response) {
    if (compressionStrategy == CompressionStrategy.NO_OP) {
      return (response == null) ? byteArrayDecoder : instrumentedByteArrayDecoder;
    } else {
      return (response == null) ? decompressingByteArrayDecoder : instrumentedDecompressingByteArrayDecoder;
    }
  }

  private DecoderWrapper<InputStream, T> getInputStreamDecoder(ReadResponseStats response) {
    return (response == null) ? decompressingInputStreamDecoder : instrumentedDecompressingInputStreamDecoder;
  }

  private interface DecoderWrapper<INPUT, OUTPUT> {
    OUTPUT decode(
        BinaryDecoder reusedDecoder,
        INPUT input,
        int inputBytesLength,
        OUTPUT reusedValue,
        RecordDeserializer<OUTPUT> deserializer,
        ReadResponseStats response,
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
        ReadResponseStats response) throws IOException;
  }

  private static class InstrumentedDecoderWrapper<INPUT, OUTPUT> implements DecoderWrapper<INPUT, OUTPUT> {
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
        ReadResponseStats response,
        VeniceCompressor compressor) {
      long deserializeStartTimeInNS = System.nanoTime();
      OUTPUT output =
          delegate.decode(reusedDecoder, input, inputBytesLength, reusedValue, deserializer, response, compressor);
      response.addReadComputeDeserializationLatency(LatencyUtils.getElapsedTimeFromNSToMS(deserializeStartTimeInNS));
      return output;
    }
  }
}
