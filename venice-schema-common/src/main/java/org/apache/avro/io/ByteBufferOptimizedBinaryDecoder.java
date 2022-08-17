package org.apache.avro.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.avro.util.Utf8;


/**
 * This class is a wrapper of {@link BinaryDecoder} with the following optimization:
 * When deserializing byte array field, instead of copying the bytes into a new {@link ByteBuffer},
 * this class will create a ByteBuffer, which is wrapping the original array.
 * This optimization is useful when we know that the original array won't change during the lifecycle.
 *
 * This optimization should work with both 1.4 and 1.7.
 */
public class ByteBufferOptimizedBinaryDecoder extends BinaryDecoder {
  private final BinaryDecoder delegate;
  private final byte[] data;
  private final int offset;
  private final int length;

  public ByteBufferOptimizedBinaryDecoder(byte[] data) {
    this(data, 0, data.length);
  }

  public ByteBufferOptimizedBinaryDecoder(byte[] data, int offset, int length) {
    this.delegate = DecoderFactory.defaultFactory().createBinaryDecoder(data, offset, length, null);
    this.data = data;
    this.offset = offset;
    this.length = length;
  }

  /**
   * The following implementation for {@link #readString()} is copied from
   * {@link BinaryDecoder#readString()} of Avro-1.7 since it is not available in Avro-1.4.
   *
   * Maybe it is fine to just call "delegate.readString()" since this function won't be used
   * in Avro-1.4, but I think it is safer to copy the implementation over.
   */
  private final Utf8 scratchUtf8 = new Utf8();

  public String readString() throws IOException {
    return readString(scratchUtf8).toString();
  }

  @Override
  public void readNull() throws IOException {
    delegate.readNull();
  }

  @Override
  public boolean readBoolean() throws IOException {
    return delegate.readBoolean();
  }

  @Override
  public int readInt() throws IOException {
    return delegate.readInt();
  }

  @Override
  public long readLong() throws IOException {
    return delegate.readLong();
  }

  @Override
  public float readFloat() throws IOException {
    return delegate.readLong();
  }

  @Override
  public double readDouble() throws IOException {
    return delegate.readDouble();
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    return delegate.readString(old);
  }

  @Override
  public void skipString() throws IOException {
    delegate.skipString();
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    int bytesLength = readInt();
    int bytesLeft = inputStream().available();
    int relativeStartPosition = length - bytesLeft;
    int startPosition = offset + relativeStartPosition;
    skipFixed(bytesLength);
    return ByteBuffer.wrap(data, startPosition, bytesLength);
  }

  @Override
  public void skipBytes() throws IOException {
    delegate.skipBytes();
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    delegate.readFixed(bytes, start, length);
  }

  @Override
  public void skipFixed(int length) throws IOException {
    delegate.skipFixed(length);
  }

  @Override
  public int readEnum() throws IOException {
    return delegate.readEnum();
  }

  @Override
  public long readArrayStart() throws IOException {
    return delegate.readArrayStart();
  }

  @Override
  public long arrayNext() throws IOException {
    return delegate.arrayNext();
  }

  @Override
  public long skipArray() throws IOException {
    return delegate.skipArray();
  }

  @Override
  public long readMapStart() throws IOException {
    return delegate.readMapStart();
  }

  @Override
  public long mapNext() throws IOException {
    return delegate.mapNext();
  }

  @Override
  public long skipMap() throws IOException {
    return delegate.skipMap();
  }

  @Override
  public int readIndex() throws IOException {
    return delegate.readIndex();
  }

  @Override
  public boolean isEnd() throws IOException {
    return delegate.isEnd();
  }

  @Override
  public InputStream inputStream() {
    return delegate.inputStream();
  }
}
