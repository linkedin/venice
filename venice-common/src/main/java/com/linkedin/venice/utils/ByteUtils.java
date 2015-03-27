package com.linkedin.venice.utils;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;


/**
 * Utility functions for munging on bytes
 */
public class ByteUtils {

    public static final int BYTES_PER_KB = 1024;
    public static final int BYTES_PER_MB = BYTES_PER_KB * 1024;
    public static final long BYTES_PER_GB = BYTES_PER_MB * 1024;
    public static final int SIZE_OF_LONG = Long.SIZE / Byte.SIZE;
    public static final int SIZE_OF_INT = Integer.SIZE / Byte.SIZE;

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
     * @param bytes The byte array
     * @param value The int to write
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
     * Compare two byte arrays. Two arrays are equal if they are the same size
     * and have the same contents. Otherwise b1 is smaller iff it is a prefix of
     * b2 or for the first index i for which b1[i] != b2[i], b1[i] < b2[i].
     * <p>
     * <strong> bytes are considered unsigned. passing negative values into byte
     * will cause them to be considered as unsigned two's complement value.
     * </strong>
     *
     * @param b1 The first array
     * @param b2 The second array
     * @return -1 if b1 < b2, 1 if b1 > b2, and 0 if they are equal
     */
    public static int compare(byte[] b1, byte[] b2) {
        return compare(b1, b2, 0, b2.length);
    }

    /**
     * Compare a byte array ( b1 ) with a sub
     *
     * @param b1     The first array
     * @param b2     The second array
     * @param offset The offset in b2 from which to compare
     * @param to     The least offset in b2 which we don't compare
     * @return -1 if b1 < b2, 1 if b1 > b2, and 0 if they are equal
     */
    public static int compare(byte[] b1, byte[] b2, int offset, int to) {
        int j = offset;
        int b2Length = to - offset;
        if (to > b2.length) {
            throw new IllegalArgumentException("To offset (" + to + ") should be <= than length (" + b2.length + ")");
        }
        for (int i = 0; i < b1.length && j < to; i++, j++) {
            int a = (b1[i] & 0xff);
            int b = (b2[j] & 0xff);
            if (a != b) {
                return (a - b) / (Math.abs(a - b));
            }
        }
        return (b1.length - b2Length) / (Math.max(1, Math.abs(b1.length - b2Length)));
    }
}
