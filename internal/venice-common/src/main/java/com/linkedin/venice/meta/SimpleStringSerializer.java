package com.linkedin.venice.meta;

import java.io.IOException;
import java.nio.charset.Charset;


public class SimpleStringSerializer implements VeniceSerializer<String> {
  public static final Charset UTF_8 = Charset.forName("UTF-8");

  @Override
  public byte[] serialize(String object, String path) throws IOException {
    return object.getBytes(UTF_8);
  }

  @Override
  public String deserialize(byte[] bytes, String path) throws IOException {
    return new String(bytes, UTF_8);
  }
}
