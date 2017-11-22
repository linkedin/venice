package com.linkedin.venice.serialization;

import com.linkedin.venice.serialization.avro.ChunkedKeySuffixSerializer;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import java.nio.ByteBuffer;


public class KeyWithChunkingSuffixSerializer {
  private static final String IGNORED_TOPIC_NAME = "ignored";
  private final ChunkedKeySuffixSerializer chunkedKeySuffixSerializer = new ChunkedKeySuffixSerializer();
  private final byte[] serializedNonChunkKeySuffix;

  public KeyWithChunkingSuffixSerializer() {
    ChunkedKeySuffix nonChunkKeySuffix = new ChunkedKeySuffix();
    nonChunkKeySuffix.isChunk = false;
    nonChunkKeySuffix.chunkId = null;
    this.serializedNonChunkKeySuffix = chunkedKeySuffixSerializer.serialize(IGNORED_TOPIC_NAME, nonChunkKeySuffix);
  }

  public byte[] serializeChunkedKey(byte[] key, ChunkedKeySuffix chunkedKeySuffix) {
    byte[] encodedChunkedKeySuffix = chunkedKeySuffixSerializer.serialize(IGNORED_TOPIC_NAME, chunkedKeySuffix);
    return serialize(key, encodedChunkedKeySuffix);
  }

  public byte[] serializeNonChunkedKey(byte[] key) {
    return serialize(key, serializedNonChunkKeySuffix);
  }

  private byte[] serialize(byte[] key, byte[] encodedChunkedKeySuffix) {
    ByteBuffer target = ByteBuffer.allocate(key.length + encodedChunkedKeySuffix.length);
    target.put(key);
    target.put(encodedChunkedKeySuffix);
    return target.array();
  }

  public byte[] serializeChunkedKey(ByteBuffer key, ChunkedKeySuffix chunkedKeySuffix) {
    byte[] encodedChunkedKeySuffix = chunkedKeySuffixSerializer.serialize(IGNORED_TOPIC_NAME, chunkedKeySuffix);
    return serialize(key, encodedChunkedKeySuffix);
  }

  public byte[] serializeNonChunkedKey(ByteBuffer key) {
    return serialize(key, serializedNonChunkKeySuffix);
  }

  private byte[] serialize(ByteBuffer key, byte[] encodedChunkedKeySuffix) {
    if (key.capacity() >= key.limit() + encodedChunkedKeySuffix.length) {
      // If the passed in ByteBuffer has enough capacity, then we expand it, rather than allocating a new one.
      key.limit(key.limit() + encodedChunkedKeySuffix.length);
      key.put(encodedChunkedKeySuffix, key.limit() - encodedChunkedKeySuffix.length, encodedChunkedKeySuffix.length);
      return key.array();
    } else {
      ByteBuffer target = ByteBuffer.allocate(key.limit() + encodedChunkedKeySuffix.length);
      target.put(key);
      target.put(encodedChunkedKeySuffix);
      return target.array();
    }
  }
}
