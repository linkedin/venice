package com.linkedin.venice.compression;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ByteUtils;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public abstract class VeniceCompressor implements Closeable {
  protected static final int SCHEMA_HEADER_LENGTH = ByteUtils.SIZE_OF_INT;
  private final CompressionStrategy compressionStrategy;
  private boolean isClosed = false;
  /**
   * To avoid the race condition between 'compress'/'decompress' operation and 'close'.
   */
  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  protected VeniceCompressor(CompressionStrategy compressionStrategy) {
    this.compressionStrategy = compressionStrategy;
  }

  interface CompressionRunnable<R> {
    R run() throws IOException;
  }

  private <R> R executeWithSafeGuard(CompressionRunnable<R> runnable) throws IOException {
    readWriteLock.readLock().lock();
    try {
      if (isClosed) {
        throw new VeniceException("Compressor for " + getCompressionStrategy() + " has been closed");
      }
      return runnable.run();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public byte[] compress(byte[] data) throws IOException {
    return executeWithSafeGuard(() -> compressInternal(data));
  }

  protected abstract byte[] compressInternal(byte[] data) throws IOException;

  public ByteBuffer compress(ByteBuffer src, int startPositionOfOutput) throws IOException {
    return executeWithSafeGuard(() -> compressInternal(src, startPositionOfOutput));
  }

  protected abstract ByteBuffer compressInternal(ByteBuffer src, int startPositionOfOutput) throws IOException;

  public ByteBuffer decompress(ByteBuffer data) throws IOException {
    return executeWithSafeGuard(() -> decompressInternal(data));
  }

  protected abstract ByteBuffer decompressInternal(ByteBuffer data) throws IOException;

  public ByteBuffer decompress(byte[] data, int offset, int length) throws IOException {
    return executeWithSafeGuard(() -> decompressInternal(data, offset, length));
  }

  protected abstract ByteBuffer decompressInternal(byte[] data, int offset, int length) throws IOException;

  /**
   * This method tries to decompress data and maybe prepend the schema header.
   * The returned ByteBuffer will be backed by byte array that starts with schema header, followed by the
   * decompressed data. The ByteBuffer will be positioned at the beginning of the decompressed data and the remaining of
   * the ByteBuffer will be the length of the decompressed data.
   */
  public ByteBuffer decompressAndPrependSchemaHeader(byte[] data, int offset, int length, int schemaHeader)
      throws IOException {
    return executeWithSafeGuard(() -> decompressAndPrependSchemaHeaderInternal(data, offset, length, schemaHeader));
  }

  protected abstract ByteBuffer decompressAndPrependSchemaHeaderInternal(
      byte[] data,
      int offset,
      int length,
      int schemaHeader) throws IOException;

  public CompressionStrategy getCompressionStrategy() {
    return compressionStrategy;
  }

  public InputStream decompress(InputStream inputStream) throws IOException {
    return executeWithSafeGuard(() -> decompressInternal(inputStream));
  }

  protected abstract InputStream decompressInternal(InputStream inputStream) throws IOException;

  public void close() throws IOException {
    readWriteLock.writeLock().lock();
    try {
      isClosed = true;
      closeInternal();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  protected abstract void closeInternal() throws IOException;
}
