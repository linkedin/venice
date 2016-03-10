package com.linkedin.venice.serialization;


public class StringSerializer implements VeniceSerializer<String> {

  public StringSerializer() {
    /* This constructor is not used, but is required for compilation */
  }

  @Override
  /**
   * Converts from a byte array to a String
   * @param byteArray - byte array to be converted
   * @return Converted string
   * */
  public String deserialize(byte[] byteArray) {
    return new String(byteArray);
  }

  /**
   * Close this serializer.
   * This method has to be idempotent if the serializer is used in KafkaProducer because it might be called
   * multiple times.
   */
  @Override
  public void close() {
    /* This function is not used, but is required for the interfaces. */
  }

  @Override
  /**
   * Converts from a string to a byte array
   * @param byteArray - byte array to be converted
   * @return Converted string
   * */
  public byte[] serialize(String string) {
    return string.getBytes();
  }
}
