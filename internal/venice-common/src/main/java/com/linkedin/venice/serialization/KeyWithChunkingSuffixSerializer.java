package com.linkedin.venice.serialization;

import com.linkedin.venice.serialization.avro.ChunkedKeySuffixSerializer;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import java.nio.ByteBuffer;


public class KeyWithChunkingSuffixSerializer {
  public static final String IGNORED_TOPIC_NAME = "ignored";
  public static final ChunkedKeySuffix NON_CHUNK_KEY_SUFFIX = createNoChunkKeySuffix();
  private final ChunkedKeySuffixSerializer chunkedKeySuffixSerializer = new ChunkedKeySuffixSerializer();
  private final byte[] serializedNonChunkKeySuffix;

  private static ChunkedKeySuffix createNoChunkKeySuffix() {
    ChunkedKeySuffix nonChunkKeySuffix = new ChunkedKeySuffix();
    nonChunkKeySuffix.isChunk = false;
    nonChunkKeySuffix.chunkId = null;
    return nonChunkKeySuffix;
  }

  public KeyWithChunkingSuffixSerializer() {
    this.serializedNonChunkKeySuffix = chunkedKeySuffixSerializer.serialize(IGNORED_TOPIC_NAME, NON_CHUNK_KEY_SUFFIX);
  }

  /**
   * @return an exactly-sized {@link ByteBuffer} containing the key appended by the suffix
   */
  public ByteBuffer serializeChunkedKey(byte[] key, ChunkedKeySuffix chunkedKeySuffix) {
    byte[] encodedChunkedKeySuffix = chunkedKeySuffixSerializer.serialize(IGNORED_TOPIC_NAME, chunkedKeySuffix);
    return serialize(key, encodedChunkedKeySuffix);
  }

  /**
   * @return an exactly-sized {@link ByteBuffer} containing the key appended by the standard suffix for non-chunked keys
   */
  public ByteBuffer serializeNonChunkedKeyAsByteBuffer(byte[] key) {
    return serialize(key, serializedNonChunkKeySuffix);
  }

  public byte[] serializeNonChunkedKey(byte[] key) {
    return serializeNonChunkedKeyAsByteBuffer(key).array();
  }

  private ByteBuffer serialize(byte[] key, byte[] encodedChunkedKeySuffix) {
    ByteBuffer target = ByteBuffer.allocate(key.length + encodedChunkedKeySuffix.length);
    target.put(key);
    target.put(encodedChunkedKeySuffix);
    target.position(0);
    return target;
  }

  public ByteBuffer serializeNonChunkedKey(ByteBuffer key) {
    return serialize(key, serializedNonChunkKeySuffix);
  }

  private ByteBuffer serialize(ByteBuffer key, byte[] encodedChunkedKeySuffix) {
    /**
     * Here will always allocate a new {@link ByteBuffer} to accommodate} the combination of key and chunked
     * key suffix since we don't know whether reusing the original {@link ByteBuffer} will cause any side effect
     * or not.
     *
     * Also this function won't change the position of the passed key {@link ByteBuffer}.
     */
    key.mark();
    ByteBuffer target = ByteBuffer.allocate(key.remaining() + encodedChunkedKeySuffix.length);
    target.put(key);
    key.reset();
    target.put(encodedChunkedKeySuffix);
    target.position(0);
    return target;
  }
}
