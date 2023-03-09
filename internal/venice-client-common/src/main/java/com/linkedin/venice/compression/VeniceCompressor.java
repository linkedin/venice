package com.linkedin.venice.compression;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;


public abstract class VeniceCompressor implements Closeable {
  private CompressionStrategy compressionStrategy;

  protected VeniceCompressor(CompressionStrategy compressionStrategy) {
    this.compressionStrategy = compressionStrategy;
  }

  public abstract byte[] compress(byte[] data) throws IOException;

  public abstract ByteBuffer compress(ByteBuffer src, int startPositionOfOutput) throws IOException;

  public abstract ByteBuffer decompress(ByteBuffer data) throws IOException;

  public abstract ByteBuffer decompress(byte[] data, int offset, int length) throws IOException;

  public CompressionStrategy getCompressionStrategy() {
    return compressionStrategy;
  }

  public abstract InputStream decompress(InputStream inputStream) throws IOException;

  public void close() throws IOException {
    return;
  }

}
