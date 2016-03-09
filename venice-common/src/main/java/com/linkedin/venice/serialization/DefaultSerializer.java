package com.linkedin.venice.serialization;


/**
 * Default Kafka VeniceSerializer.
 *
 * TODO: Decide if we really need this class at all.
 *
 * We could instead use {@link com.linkedin.venice.serialization.IdentitySerializer}
 * directly where appropriate.
 */
public class DefaultSerializer implements VeniceSerializer<byte[]> {
  IdentitySerializer serializer;

  public DefaultSerializer() {
    this.serializer = new IdentitySerializer();
  }

  @Override
  public void close() {
    /* This function is not used, but is required for the interface. */
  }

  @Override
  public byte[] serialize(byte[] bytes) {
    return serializer.serialize(bytes);
  }

  @Override
  public byte[] deserialize(byte[] bytes) {
    return serializer.deserialize(bytes);
  }
}
