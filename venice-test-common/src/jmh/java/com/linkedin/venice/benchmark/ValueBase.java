package com.linkedin.venice.benchmark;

import java.nio.ByteBuffer;


public class ValueBase {
  ByteBuffer _byteBuffer;
  int _start;
  int _end;

  public void init(ByteBuffer byteBuffer, int start, int end) {
    if (_byteBuffer != null) {
      throw new IllegalStateException("value.close() has not been called prior to the next get");
    }
    _byteBuffer = byteBuffer;
    _start = start;
    _end = end;
  }

  public ByteBuffer getByteBuffer() {
    return _byteBuffer;
  }

  public int getStart() {
    return _start;
  }

  public int getEnd() {
    return _end;
  }

  public void close() {
    _byteBuffer = null;
    _start = 0;
    _end = 0;
  }
}
