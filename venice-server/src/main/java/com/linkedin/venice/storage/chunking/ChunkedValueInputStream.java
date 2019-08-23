package com.linkedin.venice.storage.chunking;

import com.linkedin.venice.store.record.ValueRecord;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;


public class ChunkedValueInputStream extends InputStream {
  /** Each chunk is prefixed by a 4 bytes schema ID, which is not part of the payload and thus needs to be skipped */
  private static final int STARTING_OFFSET_IN_EACH_CHUNK = ValueRecord.SCHEMA_HEADER_LENGTH;

  private final byte[][] chunks;
  private int offsetInCurrentChunk = STARTING_OFFSET_IN_EACH_CHUNK;
  private int currentChunk = 0;

  public ChunkedValueInputStream(int numberOfChunks) {
    this.chunks = new byte[numberOfChunks][];
  }

  public void setChunk(int chunkIndex, byte[] chunkContent) {
    if (chunkIndex >= chunks.length) {
      throw new IllegalArgumentException("chunkIndex out of bound, chunkIndex: " + chunkIndex + ", chunks.length: " + chunks.length);
    }
    if (null != chunks[chunkIndex]) {
      throw new IllegalArgumentException("chunk already set at index: " + chunkIndex);
    }
    if (chunkContent.length <= STARTING_OFFSET_IN_EACH_CHUNK) {
      throw new IllegalArgumentException("chunkContent.length must be greater than " + STARTING_OFFSET_IN_EACH_CHUNK);
    }

    chunks[chunkIndex] = chunkContent;
  }

  @Override
  public int read() throws IOException {
    if (currentChunk == chunks.length) {
      return -1;
    }
    if (chunks[currentChunk].length == offsetInCurrentChunk) {
      currentChunk++;
      offsetInCurrentChunk = STARTING_OFFSET_IN_EACH_CHUNK;
      return read();
    }
    if (null == chunks[currentChunk]) {
      throw new IllegalStateException("All chunks should be set prior to calling read(), yet currentChunk (" + currentChunk + ") is null.");
    }

    return chunks[currentChunk][offsetInCurrentChunk++];
  }
}
