package com.linkedin.venice.compression;

import java.io.IOException;


public class NoopCompressor extends VeniceCompressor {
  public NoopCompressor() {
    super(CompressionStrategy.NO_OP);
  }

  public byte[] compress(byte[] data) throws IOException {
    return data;
  }

  public byte[] decompress(byte[] data) throws IOException {
    return data;
  }
}
