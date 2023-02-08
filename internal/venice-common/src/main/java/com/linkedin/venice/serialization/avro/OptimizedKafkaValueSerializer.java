package com.linkedin.venice.serialization.avro;

import java.util.function.BiConsumer;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;


/**
 * This class is to reuse the original array for put payload of each message
 * to reduce the unnecessary byte array allocation.
 */
public class OptimizedKafkaValueSerializer extends KafkaValueSerializer {
  private static final OptimizedBinaryDecoderFactory DECODER_FACTORY = OptimizedBinaryDecoderFactory.defaultFactory();

  public OptimizedKafkaValueSerializer() {
    super();
  }

  public OptimizedKafkaValueSerializer(BiConsumer<Integer, Schema> newSchemaEncountered) {
    super(newSchemaEncountered);
  }

  @Override
  protected BinaryDecoder createBinaryDecoder(byte[] bytes, int offset, int length, BinaryDecoder reuse) {
    return DECODER_FACTORY.createOptimizedBinaryDecoder(bytes, offset, length);
  }
}
