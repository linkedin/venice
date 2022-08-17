package com.linkedin.venice.benchmark;

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;


class VectorBuilder extends VectorBase {
  VectorBuilder(int size, int typeWidth) {
    super(size, typeWidth);
  }

  VectorBuilder(int size, int typeWidth, FlatBufferBuilder.ByteBufferFactory byteBufferFactory) {
    super(size, typeWidth, byteBufferFactory);
  }

  public void init(ByteBuffer byteBuffer, int start, int end) {
    throw new IllegalStateException("DataStoreInterface.StoreValue.init() should not be used by builder");
  }

  public int getStart() {
    return getByteBuffer().position();
  }

  public int getEnd() {
    return getByteBuffer().limit();
  }

  public void close() {
    super.close();
  }
}
