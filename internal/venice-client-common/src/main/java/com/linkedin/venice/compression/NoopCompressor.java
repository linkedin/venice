package com.linkedin.venice.compression;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;


public class NoopCompressor extends VeniceCompressor {
  public NoopCompressor() {
    super(CompressionStrategy.NO_OP);
  }

  @Override
  protected byte[] compressInternal(byte[] data) throws IOException {
    return data;
  }

  @Override
  protected ByteBuffer compressInternal(ByteBuffer data, int startPositionOfOutput) throws IOException {
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
  protected ByteBuffer decompressInternal(ByteBuffer data) throws IOException {
    return data;
  }

  @Override
  protected ByteBuffer decompressInternal(byte[] data, int offset, int length) throws IOException {
    return ByteBuffer.wrap(data, offset, length);
  }

  @Override
  protected ByteBuffer decompressAndPrependSchemaHeaderInternal(byte[] data, int offset, int length, int schemaHeader)
      throws IOException {
    if (offset < SCHEMA_HEADER_LENGTH) {
      throw new VeniceException("Start offset does not have enough room for schema header.");
    }
    ByteBuffer bb = ByteBuffer.wrap(data, offset - SCHEMA_HEADER_LENGTH, length + SCHEMA_HEADER_LENGTH);
    bb.putInt(schemaHeader);
    return bb;
  }

  @Override
  protected InputStream decompressInternal(InputStream inputStream) throws IOException {
    return inputStream;
  }

  @Override
  protected void closeInternal() throws IOException {
    // do nothing
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    return o != null && o instanceof NoopCompressor;
  }
}
