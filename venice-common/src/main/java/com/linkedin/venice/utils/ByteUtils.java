package com.linkedin.venice.utils;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;


/**
 * Utility functions for munging on bytes
 *
 *
 */
public class ByteUtils {

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
}
