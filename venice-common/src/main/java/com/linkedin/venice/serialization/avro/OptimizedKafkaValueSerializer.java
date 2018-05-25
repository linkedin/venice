package com.linkedin.venice.serialization.avro;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.OptimizedKafkaValueBinaryDecoderFactory;


/**
 * This class is to reuse the original array for put payload of each message
 * to reduce the unnecessary byte array allocation.
 */
public class OptimizedKafkaValueSerializer extends KafkaValueSerializer {
  private static final OptimizedKafkaValueBinaryDecoderFactory DECODER_FACTORY = new OptimizedKafkaValueBinaryDecoderFactory();

  @Override
  protected BinaryDecoder createBinaryDecoder(byte[] bytes, int offset,
      int length, BinaryDecoder reuse) {
    return DECODER_FACTORY.createBinaryDecoder(bytes, offset, length);
  }
}
