package com.linkedin.venice.compression;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;


public class NoopCompressor extends VeniceCompressor {
  public NoopCompressor() {
    super(CompressionStrategy.NO_OP);
  }

  @Override
  public byte[] compress(byte[] data) throws IOException {
    return data;
  }

  @Override
  public ByteBuffer compress(ByteBuffer data, int startPositionOfOutput) throws IOException {
    if (startPositionOfOutput != 0) {
      throw new UnsupportedOperationException("Compression with front padding is not supported for NO_OP.");
    }
    return data;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public ByteBuffer decompress(ByteBuffer data) throws IOException {
    return data;
  }

  @Override
  public ByteBuffer decompress(byte[] data, int offset, int length) throws IOException {
    return ByteBuffer.wrap(data, offset, length);
  }

  @Override
  public InputStream decompress(InputStream inputStream) throws IOException {
    return inputStream;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    return o != null && o instanceof NoopCompressor;
  }
}
