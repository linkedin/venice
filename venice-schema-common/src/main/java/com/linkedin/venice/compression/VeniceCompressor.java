package com.linkedin.venice.compression;

import java.io.IOException;


public abstract class VeniceCompressor {
  private CompressionStrategy compressionStrategy;

  protected VeniceCompressor(CompressionStrategy compressionStrategy) {
    this.compressionStrategy = compressionStrategy;
  }

  public abstract byte[] compress(byte[] data) throws IOException;

  public abstract byte[] decompress(byte[] data) throws IOException;

  public CompressionStrategy getCompressionStrategy() {
    return compressionStrategy;
  }
}
