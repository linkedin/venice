package com.linkedin.venice.compression;

import java.io.IOException;
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
  public ByteBuffer decompress(ByteBuffer data) throws IOException {
    return data;
  }
}
