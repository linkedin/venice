package com.linkedin.venice.chunking;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.storage.protocol.ChunkId;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;


public class TestChunkingUtils {
  private TestChunkingUtils() {
    // Util class
  }

  public static byte[] createChunkBytes(int startValue, final int chunkLength) {
    byte[] chunkBytes = new byte[chunkLength];
    for (int i = 0; i < chunkBytes.length; i++) {
      chunkBytes[i] = (byte) startValue;
      startValue++;
    }
    return chunkBytes;
  }

  public static ChunkedKeySuffix createChunkedKeySuffix(
      int firstChunkSegmentNumber,
      int firstChunkSequenceNumber,
      int chunkIndex) {
    ChunkId chunkId = new ChunkId();
    chunkId.segmentNumber = firstChunkSegmentNumber;
    chunkId.messageSequenceNumber = firstChunkSequenceNumber;
    chunkId.chunkIndex = chunkIndex;
    chunkId.producerGUID = new GUID();
    ChunkedKeySuffix chunkedKeySuffix = new ChunkedKeySuffix();
    chunkedKeySuffix.chunkId = chunkId;
    return chunkedKeySuffix;
  }
}
