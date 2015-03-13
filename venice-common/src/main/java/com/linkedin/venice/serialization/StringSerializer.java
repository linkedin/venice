package com.linkedin.venice.serialization;

import kafka.utils.VerifiableProperties;

public class StringSerializer implements Serializer<String> {

  public StringSerializer(VerifiableProperties verifiableProperties) {
    /* This constructor is not used, but is required for compilation */
  }

  @Override
  /**
   * Converts from a byte array to a String
   * @param byteArray - byte array to be converted
   * @return Converted string
   * */
  public String fromBytes(byte[] byteArray) {
    return new String(byteArray);
  }

  @Override
  /**
   * Converts from a string to a byte array
   * @param byteArray - byte array to be converted
   * @return Converted string
   * */
  public byte[] toBytes(String string) {
    return string.getBytes();
  }
}
