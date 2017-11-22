package com.linkedin.venice.serialization;

import com.linkedin.venice.serialization.avro.ChunkedKeySuffixSerializer;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import java.nio.ByteBuffer;


public class KeyWithChunkingSuffixSerializer {
  private static final String IGNORED_TOPIC_NAME = "ignored";
  private final ChunkedKeySuffixSerializer chunkedKeySuffixSerializer = new ChunkedKeySuffixSerializer();

  public byte[] serialize(byte[] key, ChunkedKeySuffix chunkedKeySuffix) {
    byte[] encodedChunkedKeySuffix = chunkedKeySuffixSerializer.serialize(IGNORED_TOPIC_NAME, chunkedKeySuffix);
    byte[] fullKey = new byte[key.length + encodedChunkedKeySuffix.length];
    ByteBuffer target = ByteBuffer.allocate(key.length + encodedChunkedKeySuffix.length);
    target.put(key);
    target.put(encodedChunkedKeySuffix);
    return target.array();
  }
}
