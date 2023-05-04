package com.linkedin.venice.io;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;


public class ZeroCopyByteArrayOutputStream extends ByteArrayOutputStream {
  public ZeroCopyByteArrayOutputStream(int size) {
    super(size);
  }

  @Override
  public synchronized byte[] toByteArray() {
    return buf;
  }

  public synchronized ByteBuffer toByteBuffer() {
    return ByteBuffer.wrap(buf, 0, size());
  }
}
