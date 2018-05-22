package org.apache.avro.io;

public class OptimizedBinaryDecoderFactory {
  private final ThreadLocal<OptimizedBinaryDecoder> localBinaryDecoder = ThreadLocal.withInitial(
      () -> new OptimizedBinaryDecoder()
  );

  private static OptimizedBinaryDecoderFactory DEFAULT_FACTORY = new OptimizedBinaryDecoderFactory();

  private OptimizedBinaryDecoderFactory() {
  }

  public static OptimizedBinaryDecoderFactory defaultFactory() {
    return DEFAULT_FACTORY;
  }


  /**
   * This function will create a optimized binary decoder.
   */
  public OptimizedBinaryDecoder createOptimizedBinaryDecoder(byte[] data, int offset, int length) {
    OptimizedBinaryDecoder decoder = localBinaryDecoder.get();
    decoder.init(data, offset, length);
    return decoder;
  }
}
