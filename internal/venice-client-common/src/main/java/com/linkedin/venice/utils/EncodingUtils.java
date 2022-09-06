package com.linkedin.venice.utils;

import java.nio.ByteBuffer;
import java.util.Base64;


public class EncodingUtils {
  private static Base64.Encoder encoder = Base64.getUrlEncoder();
  private static Base64.Decoder decoder = Base64.getUrlDecoder();

  public static byte[] base64Encode(byte[] bytes) {
    return encoder.encode(bytes);
  }

  public static String base64EncodeToString(byte[] bytes) {
    return encoder.encodeToString(bytes);
  }

  public static String base64EncodeToString(ByteBuffer byteBuffer) {
    byteBuffer.mark();
    ByteBuffer encoded = encoder.encode(byteBuffer);
    byteBuffer.reset();
    return new String(encoded.array());
  }

  public static byte[] base64Decode(byte[] bytes) {
    return decoder.decode(bytes);
  }

  public static byte[] base64DecodeFromString(String src) {
    return decoder.decode(src);
  }
}
