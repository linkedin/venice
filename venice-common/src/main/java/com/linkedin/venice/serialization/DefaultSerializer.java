package com.linkedin.venice.serialization;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.DefaultEncoder;
import kafka.utils.VerifiableProperties;

/**
 * Default Kafka Serializer
 */
public class DefaultSerializer implements Serializer<byte[]> {
  DefaultEncoder encoder;
  DefaultDecoder decoder;

  public DefaultSerializer(VerifiableProperties verifiableProperties) {
    this.encoder = new DefaultEncoder(verifiableProperties);
    this.decoder = new DefaultDecoder(verifiableProperties);
  }

  @Override
  public byte[] toBytes(byte[] bytes) {
    return encoder.toBytes(bytes);
  }

  @Override
  public byte[] fromBytes(byte[] bytes) {
    return decoder.fromBytes(bytes);
  }
}
