package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import java.nio.ByteBuffer;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;


/**
 * Utility functions for munging on bytes
 *
 * N.B.: Most functions taken from Voldemort's ByteUtils class.
 */
public class ByteUtils {
  public static final int BYTES_PER_KB = 1024;
  public static final int BYTES_PER_MB = BYTES_PER_KB * 1024;
  public static final long BYTES_PER_GB = BYTES_PER_MB * 1024;
  public static final int SIZE_OF_LONG = Long.SIZE / Byte.SIZE;
  public static final int SIZE_OF_INT = Integer.SIZE / Byte.SIZE;
  public static final int SIZE_OF_SHORT = Short.SIZE / Byte.SIZE;

  public static final int SIZE_OF_BOOLEAN = 1;

  /**
   * Translate the given byte array into a hexadecimal string
   *
   * @param bytes The bytes to translate
   * @return The string
   */
  public static String toHexString(byte[] bytes) {
    return Hex.encodeHexString(bytes);
  }

  /**
   * Translate the given byte array with specific start position and length into a hexadecimal string
   */
  public static String toHexString(byte[] bytes, int start, int len) {
    byte[] newBytes = new byte[len];
    System.arraycopy(bytes, start, newBytes, 0, len);
    return Hex.encodeHexString(newBytes);
  }

  /**
   * Translate the given hexidecimal string into a byte array
   *
   * @param hexString The hex string to translate
   * @return The bytes
   * @throws DecoderException
   */
  public static byte[] fromHexString(String hexString) {
    try {
      return Hex.decodeHex(hexString.toCharArray());
    } catch (DecoderException e) {
      throw new VeniceException("Failed to convert from Hex to byte[]", e);
    }
  }

  /**
   * Write a long to the byte array starting at the given offset
   *
   * @param bytes  The byte array
   * @param value  The long to write
   * @param offset The offset to begin writing at
   */
  public static void writeLong(byte[] bytes, long value, int offset) {
    bytes[offset] = (byte) (0xFF & (value >> 56));
    bytes[offset + 1] = (byte) (0xFF & (value >> 48));
    bytes[offset + 2] = (byte) (0xFF & (value >> 40));
    bytes[offset + 3] = (byte) (0xFF & (value >> 32));
    bytes[offset + 4] = (byte) (0xFF & (value >> 24));
    bytes[offset + 5] = (byte) (0xFF & (value >> 16));
    bytes[offset + 6] = (byte) (0xFF & (value >> 8));
    bytes[offset + 7] = (byte) (0xFF & value);
  }

  /**
   * Read a long from the byte array starting at the given offset
   *
   * @param bytes  The byte array to read from
   * @param offset The offset to start reading at
   * @return The long read
   */
  public static long readLong(byte[] bytes, int offset) {
    return (((long) (bytes[offset + 0] & 0xff) << 56) | ((long) (bytes[offset + 1] & 0xff) << 48)
        | ((long) (bytes[offset + 2] & 0xff) << 40) | ((long) (bytes[offset + 3] & 0xff) << 32)
        | ((long) (bytes[offset + 4] & 0xff) << 24) | ((long) (bytes[offset + 5] & 0xff) << 16)
        | ((long) (bytes[offset + 6] & 0xff) << 8) | ((long) bytes[offset + 7] & 0xff));
  }

  /**
   * Write an int to the byte array starting at the given offset
   *
   * @param bytes  The byte array
   * @param value  The int to write
   * @param offset The offset to begin writing at
   */
  public static void writeInt(byte[] bytes, int value, int offset) {
    bytes[offset] = (byte) (0xFF & (value >> 24));
    bytes[offset + 1] = (byte) (0xFF & (value >> 16));
    bytes[offset + 2] = (byte) (0xFF & (value >> 8));
    bytes[offset + 3] = (byte) (0xFF & value);
  }

  /**
   * Read an int from the byte array starting at the given offset
   *
   * @param bytes  The byte array to read from
   * @param offset The offset to start reading at
   * @return The int read
   */
  public static int readInt(byte[] bytes, int offset) {
    return (((bytes[offset + 0] & 0xff) << 24) | ((bytes[offset + 1] & 0xff) << 16) | ((bytes[offset + 2] & 0xff) << 8)
        | (bytes[offset + 3] & 0xff));
  }

  /**
   * Write a short to the byte array starting at the given offset
   *
   * @param bytes  The byte array
   * @param value  The short to write
   * @param offset The offset to begin writing at
   */
  public static void writeShort(byte[] bytes, short value, int offset) {
    bytes[offset] = (byte) (0xFF & (value >> 8));
    bytes[offset + 1] = (byte) (0xFF & value);
  }

  /**
   * Read a short from the byte array starting at the given offset
   *
   * @param bytes  The byte array to read from
   * @param offset The offset to start reading at
   * @return The short read
   */
  public static short readShort(byte[] bytes, int offset) {
    return (short) ((bytes[offset] << 8) | (bytes[offset + 1] & 0xff));
  }

  /**
   * Write a boolean to the byte array starting at the given offset
   *
   * @param bytes  The byte array
   * @param value  The boolean to write
   * @param offset The offset to begin writing at
   */
  public static void writeBoolean(byte[] bytes, Boolean value, int offset) {
    bytes[offset] = (byte) (value ? 0x01 : 0x00);
  }

  /**
   * Read a boolean from the byte array starting at the given offset
   *
   * @param bytes  The byte array to read from
   * @param offset The offset to start reading at
   * @return The boolean read
   */
  public static boolean readBoolean(byte[] bytes, int offset) {
    return bytes[offset] == 0x01;
  }

  /**
   * A comparator for byte arrays.
   *
   * Taken from: https://stackoverflow.com/a/5108711/791758 (and originally coming for Apache HBase)
   */
  public static int compare(byte[] left, byte[] right) {
    for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
      int a = (left[i] & 0xff);
      int b = (right[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return left.length - right.length;
  }

  /**
   * Compare whether two byte array is the same from specific offsets.
   */
  public static boolean equals(byte[] left, int leftPosition, byte[] right, int rightPosition) {
    if (left.length - leftPosition != right.length - rightPosition) {
      return false;
    }
    for (int i = leftPosition, j = 0; i < left.length; i++, j++) {
      if (left[i] != right[rightPosition + j]) {
        return false;
      }
    }
    return true;
  }

  public static boolean canUseBackedArray(ByteBuffer byteBuffer) {
    return byteBuffer.hasArray() && byteBuffer.array().length == byteBuffer.remaining();
  }

  public static byte[] extractByteArray(ByteBuffer byteBuffer) {
    if (ByteUtils.canUseBackedArray(byteBuffer)) {
      // We could safely use the backed array.
      return byteBuffer.array();
    } else {
      final int dataSize = byteBuffer.remaining();
      byte[] value = new byte[dataSize];
      extractByteArray(byteBuffer, value, 0, dataSize);
      return value;
    }
  }

  public static byte[] copyByteArray(ByteBuffer byteBuffer) {
    if (!byteBuffer.hasRemaining()) {
      return new byte[0];
    }
    int size = byteBuffer.remaining();
    byte[] ret = new byte[size];
    if (byteBuffer.isDirect()) {
      extractByteArray(byteBuffer, ret, 0, size);
    } else {
      System.arraycopy(byteBuffer.array(), byteBuffer.position(), ret, 0, size);
    }
    return ret;
  }

  /**
   * Extract the data in the ByteBuffer into the destination array by copying "length" bytes from the source
   * buffer into the given array, starting at the current position of the source buffer and at the given offset in the
   * destination array.
   * @param src The src buffer to copy data from
   * @param destination The destination array to copy data into
   * @param offset The position in destination where to start copying to
   * @param length The number of bytes to copy
   */
  public static void extractByteArray(ByteBuffer src, byte[] destination, int offset, int length) {
    src.mark();
    src.get(destination, offset, length);
    src.reset();
  }

  /**
   * Convert bytes to "Human-readable" output. Uses unit suffixes: B, KiB, MiB, GiB, TiB and PiB. Negative values are
   * handled for completeness and it is the responsibility of the caller to validate the inputs.
   * @param bytes the value to be converted to "Human-readable" output
   * @return "Human-readable" output of the input byte count
   */
  public static String generateHumanReadableByteCountString(long bytes) {
    // Math.abs(Long.MIN_VALUE) returns Long.MIN_VALUE, which is negative. To avoid this, we force the absolute value of
    // Long.MIN_VALUE to be Long.MAX_VALUE.
    long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
    if (absB < 1024) {
      return bytes + " B";
    }
    long value = absB;
    CharacterIterator ci = new StringCharacterIterator("KMGTPE");
    for (int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10) {
      value >>= 10;
      ci.next();
    }
    value *= Long.signum(bytes);
    return String.format("%.1f %ciB", value / 1024.0, ci.current());
  }

  /**
   * This function is to simulate the deserialization logic in {@literal com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer}
   * to leave some room at the beginning of byte buffer of 'byteBuffer'
   * @param byteBuffer The ByteBuffer whose size needs to be expanded.
   * @return
   */
  public static ByteBuffer enlargeByteBufferForIntHeader(ByteBuffer byteBuffer) {
    int originalPosition = byteBuffer.position();
    ByteBuffer enlargedByteBuffer = ByteBuffer.allocate(ByteUtils.SIZE_OF_INT + byteBuffer.remaining());
    enlargedByteBuffer.position(ByteUtils.SIZE_OF_INT);
    enlargedByteBuffer.put(byteBuffer);
    enlargedByteBuffer.position(ByteUtils.SIZE_OF_INT);
    byteBuffer.position(originalPosition);
    return enlargedByteBuffer;
  }

  /**
   * Extract an integer header from the ByteBuffer provided. The header is extracted from the bytes immediately
   * preceding the current position.
   * @param originalBuffer The buffer which contains the header.
   * @return The integer header that is extracted from the ByteBuffer provided.
   */
  public static int getIntHeaderFromByteBuffer(ByteBuffer originalBuffer) {
    if (originalBuffer.position() < SIZE_OF_INT) {
      throw new VeniceException("Start position of 'putValue' ByteBuffer shouldn't be less than " + SIZE_OF_INT);
    }

    originalBuffer.position(originalBuffer.position() - SIZE_OF_INT);
    return originalBuffer.getInt();
  }

  /**
   * This function will return a ByteBuffer that has the integer prepended as a header from the current position. The
   * position of the buffer will be the same as that of the input.
   * @param originalBuffer The buffer to which the header should be prepended.
   * @param header The header value to prepend to the buffer provided.
   * @param reuseOriginalBuffer If the original ByteBuffer should be reused.
   * @return The ByteBuffer that has the header prepended. If {@param reuseOriginalBuffer} is true, then return object is
   * the same as original buffer.
   */
  public static ByteBuffer prependIntHeaderToByteBuffer(
      ByteBuffer originalBuffer,
      int header,
      boolean reuseOriginalBuffer) {
    if (reuseOriginalBuffer) {
      if (originalBuffer.position() < SIZE_OF_INT) {
        throw new VeniceException(
            "Start position of 'originalBuffer' ByteBuffer shouldn't be less than " + SIZE_OF_INT);
      }

      originalBuffer.position(originalBuffer.position() - SIZE_OF_INT);
      originalBuffer.putInt(header);
      return originalBuffer;
    } else {
      ByteBuffer byteBufferWithHeader = ByteBuffer.allocate(SIZE_OF_INT + originalBuffer.remaining())
          .putInt(header)
          .put(originalBuffer.array(), originalBuffer.position(), originalBuffer.remaining());
      byteBufferWithHeader.flip();
      byteBufferWithHeader.position(SIZE_OF_INT);
      return byteBufferWithHeader;
    }
  }
}
