package com.linkedin.davinci.storage.chunking;

import java.io.ByteArrayInputStream;


/**
 * This class exposes the underlying byte array, the original offset and the original length.
 * This is mainly used in {@link AbstractAvroChunkingAdapter} to retrieve back the underlying
 * byte array.
 */
public class VeniceByteArrayInputStream extends ByteArrayInputStream {
  private final int originalOffset;
  private final int originalLength;

  public VeniceByteArrayInputStream(byte[] buf) {
    this(buf, 0, buf.length);
  }

  public VeniceByteArrayInputStream(byte[] buf, int offset, int length) {
    super(buf, offset, length);
    this.originalOffset = offset;
    this.originalLength = length;
  }

  public byte[] getBuf() {
    return buf;
  }

  public int getOriginalOffset() {
    return originalOffset;
  }

  public int getOriginalLength() {
    return originalLength;
  }
}
