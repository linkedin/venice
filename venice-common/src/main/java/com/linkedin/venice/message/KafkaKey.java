package com.linkedin.venice.message;

/**
 * Class which stores the components of a Kafka Key, and is the format specified in the KafkaKeySerializer
 */
public class KafkaKey {

  // TODO: eliminate magic numbers when finished debugging
  public static final byte DEFAULT_MAGIC_BYTE = 22;

  private byte magicByte;
  private byte[] key;

  public KafkaKey(byte[] key) {
    this.magicByte = DEFAULT_MAGIC_BYTE;
    this.key = key;
  }

  public byte getMagicByte() {
    return magicByte;
  }

  public byte[] getKey() {
    return key;
  }

  public String toString() {
    return key.toString();
  }
}
