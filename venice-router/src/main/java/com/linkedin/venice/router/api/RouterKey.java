package com.linkedin.venice.router.api;

import com.linkedin.venice.utils.EncodingUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


public class RouterKey implements Comparable<RouterKey>{
  private ByteBuffer keyBuffer;

  public RouterKey(byte[] key){
    this.keyBuffer = ByteBuffer.wrap(key);
  }
  public RouterKey(ByteBuffer key) {
    this.keyBuffer = key;
  }
  public static RouterKey fromString(String s){
    return new RouterKey(s.getBytes(StandardCharsets.UTF_8));
  }
  public static RouterKey fromBase64(String s){
    return new RouterKey(EncodingUtils.base64DecodeFromString(s));
  }

  public String base64Encoded(){
    return EncodingUtils.base64EncodeToString(keyBuffer);
  }

  public ByteBuffer getKeyBuffer() {
    return keyBuffer;
  }

  @Override
  public int compareTo(RouterKey other){
    return keyBuffer.compareTo(other.keyBuffer);
  }

  @Override
  public String toString() {
    return EncodingUtils.base64EncodeToString(keyBuffer);
  }
}
