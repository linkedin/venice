package com.linkedin.venice.compression;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;


/**
 * Locking is not necessary for {@link NoopCompressor}, so this class overrides all the public APIs to avoid locking.
 */
public class NoopCompressor extends VeniceCompressor {
  public NoopCompressor() {
    super(CompressionStrategy.NO_OP);
  }

  @Override
  public byte[] compress(byte[] data) throws IOException {
    return data;
  }

  @Override
  protected byte[] compressInternal(byte[] data) throws IOException {
    throw new UnsupportedOperationException("compressInternal");
  }

  @Override
  public ByteBuffer compress(ByteBuffer data, int startPositionOfOutput) throws IOException {
    if (startPositionOfOutput != 0) {
      throw new UnsupportedOperationException("Compression with front padding is not supported for NO_OP.");
    }
    return data;
  }

  @Override
  protected ByteBuffer compressInternal(ByteBuffer src, int startPositionOfOutput) throws IOException {
    throw new UnsupportedOperationException("compressInternal");
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
  protected ByteBuffer decompressInternal(ByteBuffer data) throws IOException {
    throw new UnsupportedOperationException("decompressInternal");
  }

  @Override
  public ByteBuffer decompress(byte[] data, int offset, int length) throws IOException {
    return ByteBuffer.wrap(data, offset, length);
  }

  @Override
  protected ByteBuffer decompressInternal(byte[] data, int offset, int length) throws IOException {
    throw new UnsupportedOperationException("decompressInternal");
  }

  @Override
  public ByteBuffer decompressAndPrependSchemaHeader(byte[] data, int offset, int length, int schemaHeader)
      throws IOException {
    if (offset < SCHEMA_HEADER_LENGTH) {
      throw new VeniceException("Start offset does not have enough room for schema header.");
    }
    ByteBuffer bb = ByteBuffer.wrap(data, offset - SCHEMA_HEADER_LENGTH, length + SCHEMA_HEADER_LENGTH);
    bb.putInt(schemaHeader);
    return bb;
  }

  @Override
  protected ByteBuffer decompressAndPrependSchemaHeaderInternal(byte[] data, int offset, int length, int schemaHeader)
      throws IOException {
    throw new UnsupportedOperationException("decompressAndPrependSchemaHeaderInternal");
  }

  @Override
  public InputStream decompress(InputStream inputStream) throws IOException {
    return inputStream;
  }

  @Override
  protected InputStream decompressInternal(InputStream inputStream) throws IOException {
    throw new UnsupportedOperationException("decompressInternal");
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }

  @Override
  protected void closeInternal() throws IOException {
    throw new UnsupportedOperationException("closeInternal");
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    return o != null && o instanceof NoopCompressor;
  }
}
