package com.linkedin.venice.hadoop.input.kafka.chunk;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.storage.protocol.ChunkId;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;


public class TestChunkingUtils {
  private TestChunkingUtils() {
    // Util class
  }

  static byte[] createChunkBytes(int startValue, final int chunkLength) {
    byte[] chunkBytes = new byte[chunkLength];
    for (int i = 0; i < chunkBytes.length; i++) {
      chunkBytes[i] = (byte) startValue;
      startValue++;
    }
    return chunkBytes;
  }

  static ChunkedKeySuffix createChunkedKeySuffix(int segmentNumber, int messageSequenceNumber, int chunkIndex) {
    ChunkId chunkId = new ChunkId();
    chunkId.segmentNumber = segmentNumber;
    chunkId.messageSequenceNumber = messageSequenceNumber;
    chunkId.chunkIndex = chunkIndex;
    chunkId.producerGUID = new GUID();
    ChunkedKeySuffix chunkedKeySuffix = new ChunkedKeySuffix();
    chunkedKeySuffix.chunkId = chunkId;
    return chunkedKeySuffix;
  }
}
