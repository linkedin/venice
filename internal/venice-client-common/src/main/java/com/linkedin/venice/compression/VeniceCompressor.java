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
   * This method tries to decompress data and maybe prepend the schema header.
   * The returned ByteBuffer will be backed by byte array that starts with schema header headroom, followed by the
   * decompressed data. The ByteBuffer will be positioned at the beginning of the decompressed data and the remaining of
   * the ByteBuffer will be the length of the decompressed data.
   * If prependSchemaHeader is set to true, it will try to fetch the schema header from the original data array, from the
   * position of (offset - SCHEMA_HEADER_LENGTH), and fill in the beginning of the new byte array that backs the returned
   * ByteBuffer.
   * If offset is smaller than SCHEMA_HEADER_LENGTH, this method will throw {@link com.linkedin.venice.exceptions.VeniceException}.
   */
  public abstract ByteBuffer decompressAndMaybePrependSchemaHeader(
      byte[] data,
      int offset,
      int length,
      boolean prependSchemaHeader) throws IOException;

  public CompressionStrategy getCompressionStrategy() {
    return compressionStrategy;
  }

  public abstract InputStream decompress(InputStream inputStream) throws IOException;

  public void close() throws IOException {
  }
}
