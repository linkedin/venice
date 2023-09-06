package com.linkedin.venice.compression;

import com.linkedin.venice.utils.ByteUtils;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;


public abstract class VeniceCompressor implements Closeable {
  protected static final int SCHEMA_HEADER_LENGTH = ByteUtils.SIZE_OF_INT;
  private final CompressionStrategy compressionStrategy;

  protected VeniceCompressor(CompressionStrategy compressionStrategy) {
    this.compressionStrategy = compressionStrategy;
  }

  public abstract byte[] compress(byte[] data) throws IOException;

  public abstract ByteBuffer compress(ByteBuffer src, int startPositionOfOutput) throws IOException;

  public abstract ByteBuffer decompress(ByteBuffer data) throws IOException;

  public abstract ByteBuffer decompress(byte[] data, int offset, int length) throws IOException;

  /**
   * This method tries to decompress data and prepend the schema header. It assumes the input data is already prepended
   * with schema header in the beginning of the data byte array, and the length includes the header length.
   * The returned ByteBuffer will be backed by byte array that starts with schema header, followed by the decompressed
   * data. The ByteBuffer will be positioned at the beginning of the decompressed data and the remaining of the ByteBuffer
   * will be the length of the decompressed data.
   */
  public abstract ByteBuffer decompressAndPrependSchemaHeader(byte[] data, int length) throws IOException;

  public CompressionStrategy getCompressionStrategy() {
    return compressionStrategy;
  }

  public abstract InputStream decompress(InputStream inputStream) throws IOException;

  public void close() throws IOException {
    return;
  }

}
