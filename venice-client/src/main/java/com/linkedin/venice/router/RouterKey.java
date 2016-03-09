package com.linkedin.venice.router;

import com.google.common.base.Charsets;
import java.util.Base64;


/**
 * Created by mwise on 3/3/16.
 */
public class RouterKey {

  private static final Base64.Encoder encoder = Base64.getEncoder();
  private static final Base64.Decoder decoder = Base64.getDecoder();

  private byte[] key;
  public RouterKey(byte[] key){
    this.key = key;
  }
  public static RouterKey fromString(String s){
    return new RouterKey(s.getBytes(Charsets.UTF_8));
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

}
