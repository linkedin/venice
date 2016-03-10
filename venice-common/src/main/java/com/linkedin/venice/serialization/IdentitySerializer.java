package com.linkedin.venice.serialization;

/**
 * A pass-through for byte arrays
 */
public class IdentitySerializer implements VeniceSerializer<byte[]> {

  @Override
  public void close() {
    // no-op
  }

  @Override
  public byte[] serialize(byte[] object) {
    return object;
  }

  @Override
  public byte[] deserialize(byte[] bytes) {
    return bytes;
  }
}
