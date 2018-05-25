package org.apache.avro.io;

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * This class is used to optimize bytes field decoding.
 * It will wrap the original byte array as a byte buffer if the field type is 'bytes'(the default behavior
 * in {@link BinaryDecoder#readBytes(ByteBuffer) is to create a new byte array copy, which is not efficient}.
 *
 * The reason to use package name: org.apache.avro.io since {@link BinaryDecoder#BinaryDecoder(byte[], int, int)} is
 * package-visible.
 */
public class OptimizedKafkaValueBinaryDecoder extends BinaryDecoder {
  private byte[] data;
  private int offset;
  private int length;

  OptimizedKafkaValueBinaryDecoder() {
  }

  @Override
  void init(byte[] data, int offset, int length) {
    super.init(data, offset, length);
    this.data = data;
    this.offset = offset;
    this.length = length;
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
}
