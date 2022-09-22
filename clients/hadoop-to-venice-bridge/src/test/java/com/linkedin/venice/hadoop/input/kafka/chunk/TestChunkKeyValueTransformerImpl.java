package com.linkedin.venice.hadoop.input.kafka.chunk;

import static com.linkedin.venice.hadoop.input.kafka.chunk.TestChunkingUtils.createChunkBytes;
import static com.linkedin.venice.hadoop.input.kafka.chunk.TestChunkingUtils.createChunkedKeySuffix;

import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.ChunkedKeySuffixSerializer;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestChunkKeyValueTransformerImpl {
  private static final ChunkedKeySuffixSerializer CHUNKED_KEY_SUFFIX_SERIALIZER = new ChunkedKeySuffixSerializer();

  @Test
  public void testSplitCompositeKeyWithNonChunkValue() {
    byte[] firstPartBytes = createChunkBytes(5, 1213);
    byte[] secondPartBytes = serialize(KeyWithChunkingSuffixSerializer.NON_CHUNK_KEY_SUFFIX);
    byte[] compositeKey = combineParts(firstPartBytes, secondPartBytes);
    ChunkKeyValueTransformer chunkKeyValueTransformer = new ChunkKeyValueTransformerImpl(ChunkedKeySuffix.SCHEMA$);

    for (ChunkKeyValueTransformer.KeyType keyType: Arrays.asList(
        ChunkKeyValueTransformer.KeyType.WITH_FULL_VALUE,
        ChunkKeyValueTransformer.KeyType.WITH_CHUNK_MANIFEST)) {
      RawKeyBytesAndChunkedKeySuffix bytesAndChunkedKeySuffix =
          chunkKeyValueTransformer.splitChunkedKey(compositeKey, keyType);

      byte[] resultsBytes =
          combineParts(bytesAndChunkedKeySuffix.getRawKeyBytes(), bytesAndChunkedKeySuffix.getChunkedKeySuffixBytes());
      Assert.assertEquals(resultsBytes, compositeKey); // Same as the original composite key bytes
    }
  }

  @Test
  public void testSplitCompositeKeyWithChunkValue() {
    // Each composite key has 2 chunked key suffix serialized bytes concatenated back to back. We should be able to
    // split the composite key and when we concatenate the divided 2 parts together, we should get the original
    // composite key bytes.
    ChunkKeyValueTransformer chunkKeyValueTransformer = new ChunkKeyValueTransformerImpl(ChunkedKeySuffix.SCHEMA$);
    List<ChunkedKeySuffix> firstParts = new ArrayList<>();
    firstParts.add(KeyWithChunkingSuffixSerializer.NON_CHUNK_KEY_SUFFIX);
    firstParts.add(createChunkedKeySuffix(12, 123, 12));
    firstParts.add(createChunkedKeySuffix(1212, 12331, 121213));
    firstParts.add(createChunkedKeySuffix(0, 0, 0));
    firstParts.add(createChunkedKeySuffix(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE));
    firstParts.add(createChunkedKeySuffix(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE));
    List<ChunkedKeySuffix> secondParts = new ArrayList<>(firstParts);

    for (int i = 0; i < firstParts.size(); i++) {
      for (int j = 0; j < secondParts.size(); j++) {
        byte[] compositeKey = combineParts(serialize(firstParts.get(i)), serialize(secondParts.get(j)));
        RawKeyBytesAndChunkedKeySuffix bytesAndChunkedKeySuffix =
            chunkKeyValueTransformer.splitChunkedKey(compositeKey, ChunkKeyValueTransformer.KeyType.WITH_VALUE_CHUNK);
        byte[] resultsBytes = combineParts(
            bytesAndChunkedKeySuffix.getRawKeyBytes(),
            bytesAndChunkedKeySuffix.getChunkedKeySuffixBytes());
        Assert.assertEquals(resultsBytes, compositeKey); // Same as the original composite key bytes
      }
    }
  }

  private byte[] serialize(ChunkedKeySuffix chunkedKeySuffix) {
    return CHUNKED_KEY_SUFFIX_SERIALIZER.serialize("", chunkedKeySuffix);
  }

  private byte[] combineParts(byte[] firstPartBytes, byte[] secondPartBytes) {
    return combineParts(ByteBuffer.wrap(firstPartBytes), ByteBuffer.wrap(secondPartBytes));
  }

  private byte[] combineParts(ByteBuffer firstPartBytes, ByteBuffer secondPartBytes) {
    byte[] combinedBytes = new byte[firstPartBytes.remaining() + secondPartBytes.remaining()];
    System.arraycopy(firstPartBytes.array(), firstPartBytes.position(), combinedBytes, 0, firstPartBytes.remaining());
    System.arraycopy(
        secondPartBytes.array(),
        secondPartBytes.position(),
        combinedBytes,
        firstPartBytes.remaining(),
        secondPartBytes.remaining());
    return combinedBytes;
  }
}
