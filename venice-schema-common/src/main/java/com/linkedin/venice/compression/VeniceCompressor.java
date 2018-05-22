package com.linkedin.venice.compression;

import java.io.IOException;
import java.nio.ByteBuffer;


public abstract class VeniceCompressor {
  private CompressionStrategy compressionStrategy;

  protected VeniceCompressor(CompressionStrategy compressionStrategy) {
    this.compressionStrategy = compressionStrategy;
  }

  public abstract byte[] compress(byte[] data) throws IOException;

  public abstract ByteBuffer decompress(ByteBuffer data) throws IOException;

  public CompressionStrategy getCompressionStrategy() {
    return compressionStrategy;
  }
}
