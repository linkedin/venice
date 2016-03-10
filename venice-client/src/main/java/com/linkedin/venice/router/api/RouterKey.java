package com.linkedin.venice.router.api;

import java.nio.charset.StandardCharsets;
import java.util.Base64;


/**
 * Created by mwise on 3/3/16.
 */
public class RouterKey implements Comparable<RouterKey>{

  private static final Base64.Encoder encoder = Base64.getEncoder();
  private static final Base64.Decoder decoder = Base64.getDecoder();

  private byte[] key;
  public RouterKey(byte[] key){
    this.key = key;
  }
  public static RouterKey fromString(String s){
    return new RouterKey(s.getBytes(StandardCharsets.UTF_8));
  }
  public static RouterKey fromBase64(String s){
    return new RouterKey(decoder.decode(s));
  }

  public String base64Encoded(){
    return encoder.encodeToString(key);
  }

  public byte[] getBytes(){
    return key;
  }

  @Override
  public int compareTo(RouterKey routerKey) {
    if (null==routerKey){
      throw new NullPointerException("Cannot compare RouterKey to null");
    }
    byte[] other = routerKey.getBytes();
    int smallest = key.length;
    if (other.length < smallest){
      smallest = other.length;
    }
    for (int i=0; i<smallest; i++){
      if (key[i] == other[i]){
        continue;
      } else if (key[i] < other[i]){
        return -1;
      } else {
        return 1;
      }
    }
    if (key.length == other.length){
      return 0;
    } else if (key.length < other.length){
      return -1;
    } else {
      return 1;
    }
  }
}
