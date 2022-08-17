package com.linkedin.venice.benchmark;

import com.google.flatbuffers.FlatBufferBuilder;
import java.io.Closeable;
import java.nio.ByteBuffer;


public class VectorBase implements Closeable {
  private final int _typeWidth;
  private final int _maxSize;
  private final FlatBufferBuilder.ByteBufferFactory _byteBufferFactory;
  protected ByteBuffer _byteBuffer;
  private int _size;

  protected VectorBase(int size, int typeWidth) {
    this(size, typeWidth, FlatBufferBuilder.HeapByteBufferFactory.INSTANCE);
  }

  protected VectorBase(int size, int typeWidth, FlatBufferBuilder.ByteBufferFactory byteBufferFactory) {
    _typeWidth = typeWidth;
    _maxSize = (int) ((1L << 32) / typeWidth);
    _byteBufferFactory = byteBufferFactory;
    if (size > _maxSize) {
      throw new IllegalArgumentException("can't allocate " + size + "values. Max is " + _maxSize);
    }
    int sizeBytes = size * _typeWidth;
    _size = size;
    _byteBuffer = _byteBufferFactory.newByteBuffer(sizeBytes);
  }

  /**
   * number of elements
   * @param size
   */
  public void size(int size) {
    if (size > _maxSize) {
      throw new IllegalArgumentException("can't allocate " + size + "values. Max is " + _maxSize);
    }
    int sizeBytes = size * _typeWidth;
    if (_byteBuffer.capacity() < sizeBytes) {
      ByteBuffer newBuffer = _byteBufferFactory.newByteBuffer(nextPowerOfTwo(sizeBytes));
      int currentSizeBytes = _size * _typeWidth;
      for (int i = 0; i < currentSizeBytes; i++) {
        newBuffer.put(i, _byteBuffer.get(i));
      }
      _byteBufferFactory.releaseByteBuffer(_byteBuffer);
      _byteBuffer = newBuffer;
    }
    _size = size;
    // set the limit, so that ByteBuffer.put* doesn't fail, when it calls java.nio.Buffer.checkIndex
    _byteBuffer.position(0);
    _byteBuffer.limit(sizeBytes);
  }

  /**
   * returns the number of elements
   */
  public int size() {
    return _size;
  }

  @Deprecated
  public ByteBuffer build() {
    return getByteBuffer();
  }

  public ByteBuffer getByteBuffer() {
    _byteBuffer.position(0);
    _byteBuffer.limit(_size * _typeWidth);
    return _byteBuffer;
  }

  public boolean isEmpty() {
    return _size == 0;
  }

  /**
   * Rounds up the provided value to the nearest power of two.
   *
   * @param val An integer value.
   * @return The closest power of two of that value.
   */
  public static int nextPowerOfTwo(int val) {
    if (val == 0) {
      return 0;
    } else if (val == 1) {
      return 2;
    } else {
      int highestOneBit = Integer.highestOneBit(val);
      if (highestOneBit == val) {
        return val;
      } else {
        return highestOneBit << 1;
      }
    }
  }

  @Override
  public void close() {
    _byteBufferFactory.releaseByteBuffer(_byteBuffer);
  }
}
