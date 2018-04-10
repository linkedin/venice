package org.apache.avro.io;

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * This class is used to optimize bytes field decoding.
 * It will follow {@link #bufferReuse} flag to decide whether to wrap the original byte array
 * as a byte buffer or create a new byte array (the default behavior in {@link BinaryDecoder#readBytes(ByteBuffer)}.
 *
 * The reason to use package name: org.apache.avro.io since {@link BinaryDecoder#BinaryDecoder(byte[], int, int)} is
 * package-visible.
 */
public class OptimizedKafkaValueBinaryDecoder extends BinaryDecoder {
  private byte[] data;
  private int offset;
  private int length;
  private boolean bufferReuse = false;

  OptimizedKafkaValueBinaryDecoder() {
  }

  @Override
  void init(byte[] data, int offset, int length) {
    super.init(data, offset, length);
    this.data = data;
    this.offset = offset;
    this.length = length;
    disableBufferReuse();
  }

  public void enableBufferReuse() {
    bufferReuse = true;
  }

  public void disableBufferReuse() {
    bufferReuse = false;
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    if (bufferReuse) {
      int bytesLength = readInt();
      int bytesLeft = inputStream().available();
      int relativeStartPosition = length - bytesLeft;
      int startPosition = offset + relativeStartPosition;
      skipFixed(bytesLength);
      return ByteBuffer.wrap(data, startPosition, bytesLength);
    } else {
      return super.readBytes(old);
    }
  }
}
