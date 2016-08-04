package com.linkedin.venice.utils;

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

  private final static int MAX_LENGTH_TO_LOG = 50;

  /**
   * Translate the given byte array into a hexidecimal string
   *
   * @param bytes The bytes to translate
   * @return The string
   */
  public static String toHexString(byte[] bytes) {
    return Hex.encodeHexString(bytes);
  }

  /**
   * Translate the given byte array to a String so that it can be used in logging.
   * This function handles truncation of the String to prevent log from overflowing.
   *
   * @param bytes
   * @return String
   */
  public static String toLogString(byte[] bytes) {
    if (bytes == null) {
      return "null";
    }

    String str = toHexString(bytes);
    if (str.length() > MAX_LENGTH_TO_LOG) {
      return str.substring(0, MAX_LENGTH_TO_LOG) + "...truncated";
    }
    return str;
  }

  /**
   * Translate the given hexidecimal string into a byte array
   *
   * @param hexString The hex string to translate
   * @return The bytes
   * @throws DecoderException
   */
  public static byte[] fromHexString(String hexString)
      throws DecoderException {
    return Hex.decodeHex(hexString.toCharArray());
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
    return (((long) (bytes[offset + 0] & 0xff) << 56) | ((long) (bytes[offset + 1] & 0xff) << 48) | (
        (long) (bytes[offset + 2] & 0xff) << 40) | ((long) (bytes[offset + 3] & 0xff) << 32) | (
        (long) (bytes[offset + 4] & 0xff) << 24) | ((long) (bytes[offset + 5] & 0xff) << 16) | (
        (long) (bytes[offset + 6] & 0xff) << 8) | ((long) bytes[offset + 7] & 0xff));
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
    return (((bytes[offset + 0] & 0xff) << 24) | ((bytes[offset + 1] & 0xff) << 16)
        | ((bytes[offset + 2] & 0xff) << 8) | (bytes[offset + 3] & 0xff));
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
}
