package com.linkedin.venice.benchmark;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


public class FloatVectorImpl extends ValueBase {
  public static final byte TYPE_WIDTH = Float.BYTES;
  private int _size;

  public float getFloat(int index) {
    return _byteBuffer.getFloat(_start + index * TYPE_WIDTH);
  }

  public void init(ByteBuffer byteBuffer, int start, int end) {
    super.init(byteBuffer, start, end);
    _size = (_end - _start) / TYPE_WIDTH;
  }

  public void close() {
    super.close();
    _size = 0;
  }

  public int size() {
    return _size;
  }

  public float[] getFLoatArray(float[] floatArrayReuse) {
    if (_byteBuffer == null) {
      return null;
    }
    final float[] floatArray;
    if (floatArrayReuse != null && floatArrayReuse.length == _size) {
      floatArray = floatArrayReuse;
    } else {
      floatArray = new float[_size];
    }
    ((ByteBuffer) _byteBuffer.duplicate().position(_start)).order(ByteOrder.LITTLE_ENDIAN)
        .asFloatBuffer()
        .get(floatArray, 0, _size);

    return floatArray;
  }

  public static class Builder extends VectorBuilder {
    public Builder() {
      this(0);
    }

    public Builder(int size) {
      super(size, TYPE_WIDTH);
    }

    /**
     * Grow vector size by one and write vector element to the end of the vector.
     * @param value
     */
    public void write(float value) {
      int index = size();
      size(index + 1);
      set(index, value);
    }

    /**
     * write vector element at a given index.
     * It is caller responsibility to ensure they don't write past the vector end,
     *  i.e. call {@link #size(int)} to reserve enough capacity
     * @param index
     * @param value
     */
    public void set(int index, float value) {
      _byteBuffer.putFloat(index * TYPE_WIDTH, value);
    }
  }
}
