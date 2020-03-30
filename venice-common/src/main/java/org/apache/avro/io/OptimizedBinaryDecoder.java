package org.apache.avro.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


/**
 * This class is used to optimize bytes field decoding.
 * It will wrap the original byte array as a byte buffer if the field type is 'bytes'(the default behavior
 * in {@link BinaryDecoder#readBytes(ByteBuffer) is to create a new byte array copy, which is not efficient}.
 *
 * The reason to use package name: org.apache.avro.io since {@link BinaryDecoder#BinaryDecoder(byte[], int, int)} is
 * package-visible.
 */
public class OptimizedBinaryDecoder extends BinaryDecoder {
  private ByteBuffer byteBuffer;
  private int offset;
  private int length;

  OptimizedBinaryDecoder() {
  }

  @Override
  BinaryDecoder configure(InputStream in, int bufferSize) {
    throw new RuntimeException(this.getClass().getSimpleName() + " is not compatible with InputStream!");
  }

  @Override
  BinaryDecoder configure(byte[] data, int offset, int length) {
    super.configure(data, offset, length);
    byteBuffer = ByteBuffer.wrap(data, offset, length);
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    this.offset = offset;
    this.length = length;
    return this;
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    int bytesLength = readInt();
    int bytesLeft = inputStream().available();
    int relativeStartPosition = length - bytesLeft;
    int startPosition = offset + relativeStartPosition;
    skipFixed(bytesLength);
    return ByteBuffer.wrap(byteBuffer.array(), startPosition, bytesLength);
  }

 @Override
  public float readFloat() throws IOException {
    float f = byteBuffer.getFloat(getPos());
    doSkipBytes(Float.BYTES);
    return f;
  }
}
