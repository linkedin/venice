package com.linkedin.venice.router.api;

import com.linkedin.venice.utils.EncodingUtils;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang.builder.CompareToBuilder;


public class RouterKey implements Comparable<RouterKey>{
  private byte[] key;
  public RouterKey(byte[] key){
    this.key = key;
  }
  public static RouterKey fromString(String s){
    return new RouterKey(s.getBytes(StandardCharsets.UTF_8));
  }
  public static RouterKey fromBase64(String s){
    return new RouterKey(EncodingUtils.base64DecodeFromString(s));
  }

  public String base64Encoded(){
    return EncodingUtils.base64EncodeToString(key);
  }

  public byte[] getBytes(){
    return key;
  }

  @Override
  public int compareTo(RouterKey other){
    return new CompareToBuilder()
        .append(this.key, other.key)
        .toComparison();
  }
}
