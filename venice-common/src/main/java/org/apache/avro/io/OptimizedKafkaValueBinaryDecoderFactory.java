package org.apache.avro.io;

public class OptimizedKafkaValueBinaryDecoderFactory {
  private final ThreadLocal<OptimizedKafkaValueBinaryDecoder> localBinaryDecoder = ThreadLocal.withInitial(
      () -> new OptimizedKafkaValueBinaryDecoder()
  );

  public OptimizedKafkaValueBinaryDecoder createBinaryDecoder(byte[] data, int offset, int length) {
    OptimizedKafkaValueBinaryDecoder decoder = localBinaryDecoder.get();
    decoder.init(data, offset, length);
    return decoder;
  }
}
