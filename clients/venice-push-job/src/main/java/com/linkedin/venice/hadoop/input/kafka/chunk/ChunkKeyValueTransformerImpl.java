package com.linkedin.venice.hadoop.input.kafka.chunk;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.ChunkedKeySuffixSerializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;
import org.apache.commons.lang.Validate;


public class ChunkKeyValueTransformerImpl implements ChunkKeyValueTransformer {
  private static final int NON_CHUNKED_KEY_SIZE = calculateNonChunkedKeySize();

  private static int calculateNonChunkedKeySize() {
    return new ChunkedKeySuffixSerializer().serialize(
        KeyWithChunkingSuffixSerializer.IGNORED_TOPIC_NAME,
        KeyWithChunkingSuffixSerializer.NON_CHUNK_KEY_SUFFIX).length;
  }

  private final RecordDeserializer<?> keyDeserializer;

  public ChunkKeyValueTransformerImpl(@Nonnull Schema keySchema) {
    Validate.notNull(keySchema);
    this.keyDeserializer = SerializerDeserializerFactory.getAvroGenericDeserializer(keySchema);
  }

  @Override
  public RawKeyBytesAndChunkedKeySuffix splitChunkedKey(byte[] compositeKey, KeyType keyType) {
    switch (keyType) {
      case WITH_FULL_VALUE:
      case WITH_CHUNK_MANIFEST:
        return splitKeyWithNonChunkedKeySuffix(compositeKey);
      case WITH_VALUE_CHUNK:
        return splitKeyWithChunkedKeySuffix(compositeKey);
      default:
        throw new VeniceException("Unhandled key type: " + keyType);
    }
  }

  private RawKeyBytesAndChunkedKeySuffix splitKeyWithNonChunkedKeySuffix(byte[] compositeKey) {
    return splitCompositeKeyWithSuffixSize(compositeKey, NON_CHUNKED_KEY_SIZE);
  }

  private RawKeyBytesAndChunkedKeySuffix splitKeyWithChunkedKeySuffix(byte[] compositeKey) {
    BinaryDecoder decoder = OptimizedBinaryDecoderFactory.defaultFactory()
        .createOptimizedBinaryDecoder(compositeKey, 0, compositeKey.length);
    keyDeserializer.deserialize(decoder);
    int chunkedKeySuffixByteCount;
    try {
      // Remaining number of bytes must be the size in the decoder's input stream buffer. So the remaining available
      // bytes should be the size of the chunked key suffix.
      chunkedKeySuffixByteCount = decoder.inputStream().available();
    } catch (IOException e) {
      throw new VeniceException(e);
    }
    return splitCompositeKeyWithSuffixSize(compositeKey, chunkedKeySuffixByteCount);
  }

  private RawKeyBytesAndChunkedKeySuffix splitCompositeKeyWithSuffixSize(
      byte[] compositeKey,
      int chunkedKeySuffixSize) {
    return new RawKeyBytesAndChunkedKeySuffix(
        ByteBuffer.wrap(compositeKey, 0, compositeKey.length - chunkedKeySuffixSize),
        ByteBuffer.wrap(compositeKey, compositeKey.length - chunkedKeySuffixSize, chunkedKeySuffixSize));
  }
}
