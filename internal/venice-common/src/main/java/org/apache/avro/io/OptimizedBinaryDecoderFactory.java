package org.apache.avro.io;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.nio.ByteBuffer;


public class OptimizedBinaryDecoderFactory {
  private final ThreadLocal<OptimizedBinaryDecoder> localBinaryDecoder =
      ThreadLocal.withInitial(OptimizedBinaryDecoder::new);

  private static OptimizedBinaryDecoderFactory DEFAULT_FACTORY = new OptimizedBinaryDecoderFactory();

  private OptimizedBinaryDecoderFactory() {
  }

  public static OptimizedBinaryDecoderFactory defaultFactory() {
    return DEFAULT_FACTORY;
  }

  public OptimizedBinaryDecoder createOptimizedBinaryDecoder(ByteBuffer bb) {
    return createOptimizedBinaryDecoder(bb.array(), bb.position(), bb.remaining());
  }

  /**
   * This function will create an optimized binary decoder.
   */
  public OptimizedBinaryDecoder createOptimizedBinaryDecoder(byte[] data, int offset, int length) {
    OptimizedBinaryDecoder threadLocalDecoder = localBinaryDecoder.get();
    BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(data, offset, length, threadLocalDecoder);
    if (threadLocalDecoder != decoder) {
      // Reuse failed...
      if (decoder instanceof OptimizedBinaryDecoder) {
        // In theory, this should never happen, which means that whenever reuse fails, we will end up fail fast
        OptimizedBinaryDecoder newDecoder = (OptimizedBinaryDecoder) threadLocalDecoder;
        localBinaryDecoder.set(newDecoder);
        threadLocalDecoder = newDecoder;
      } else {
        /**
         * We would not be able to call {@link OptimizedBinaryDecoder#configureByteBuffer(byte[], int, int)}
         * if we're not using a {@link OptimizedBinaryDecoder}, hence we fail fast instead of silently degrading
         * performance...
         */
        throw new IllegalStateException("Decoder reuse failed...!");
      }
    }
    threadLocalDecoder.configureByteBuffer(data, offset, length);
    return threadLocalDecoder;
  }
}
