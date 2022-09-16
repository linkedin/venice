package com.linkedin.davinci.storage.chunking;

import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.annotation.NotThreadsafe;
import java.io.IOException;
import java.io.InputStream;


/**
 * This {@link InputStream} implementation allows us to pass a value made up of many chunks into the Avro decoder
 * without stitching it up into a single big byte array. This is in the hope of avoiding humongous allocations
 * and being more GC-efficient.
 *
 * NOT intended for multi-threaded usage.
 */
@NotThreadsafe
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
      throw new IllegalArgumentException(
          "chunkIndex out of bound, chunkIndex: " + chunkIndex + ", chunks.length: " + chunks.length);
    }
    if (chunks[chunkIndex] != null) {
      throw new IllegalArgumentException("chunk already set at index: " + chunkIndex);
    }
    if (chunkContent.length <= STARTING_OFFSET_IN_EACH_CHUNK) {
      throw new IllegalArgumentException("chunkContent.length must be greater than " + STARTING_OFFSET_IN_EACH_CHUNK);
    }

    chunks[chunkIndex] = chunkContent;
  }

  /**
   * Required part of the {@link InputStream} contract, although we hope that it will generally not
   * be called, as the alternative {@link #read(byte[], int, int)} function is more efficient. Still,
   * we provide this mandatory function in case any code needs to do byte-by-byte consumption.
   */
  @Override
  public int read() throws IOException {
    if (isFullyRead()) {
      return -1;
    }
    if (isCurrentChunkFullyRead()) {
      moveToNextChunk();
      return read();
    }

    /**
     * The {@code & 0xFF} is very important. The contract of {@link InputStream#read()} is to return data
     * between 0..255, but the byte primitive type in Java is signed, from -128..127. The {@code & 0xFF}
     * is equivalent to incrementing negative values by 256, which complies with the API's contract.
     *
     * Without this, there was a bug where if the serialized data contained -1, it wedged the internal
     * state of the {@link org.apache.avro.io.BinaryDecoder}, since -1 is a sentinel value for the EOF.
     */
    return chunks[currentChunk][offsetInCurrentChunk++] & 0xFF;
  }

  /**
   * This function is an optional, but recommended, part of the {@link InputStream} contract. It is more
   * optimal than {@link InputStream#read(byte[], int, int)} which internally iterates over our own
   * {@link #read()} implementation, one byte at a time.
   */
  @Override
  public int read(byte b[], int off, int len) throws IOException {
    /** Boilerplate safeguards copied from {@link InputStream#read(byte[], int, int)} */
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int bytesRead = 0;
    while (bytesRead < len) {
      if (isFullyRead()) {
        if (bytesRead == 0) {
          return -1;
        } else {
          return bytesRead;
        }
      }
      if (isCurrentChunkFullyRead()) {
        moveToNextChunk();
        // defensive code: we restart the loop to undergo the previous check again
        continue;
      }

      int copiableLength = Math.min(len - bytesRead, chunks[currentChunk].length - offsetInCurrentChunk);
      System.arraycopy(chunks[currentChunk], offsetInCurrentChunk, b, off + bytesRead, copiableLength);
      offsetInCurrentChunk += copiableLength;
      bytesRead += copiableLength;
    }
    return bytesRead;
  }

  private void moveToNextChunk() {
    currentChunk++;
    offsetInCurrentChunk = STARTING_OFFSET_IN_EACH_CHUNK;
    if (!isFullyRead() && chunks[currentChunk] == null) {
      // Defensive code. Should never happen, unless there is a regression in the code.
      throw new IllegalStateException(
          "All chunks should be set prior to calling read(), yet currentChunk (" + currentChunk + ") is null.");
    }
  }

  private boolean isFullyRead() {
    return currentChunk == chunks.length;
  }

  private boolean isCurrentChunkFullyRead() {
    return chunks[currentChunk].length == offsetInCurrentChunk;
  }
}
