package com.linkedin.venice.compression;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;


public class CompressorFactory {
  private static Map<CompressionStrategy, VeniceCompressor> compressorMap = new VeniceConcurrentHashMap<>();

  public static VeniceCompressor getCompressor(CompressionStrategy compressionStrategy) {
    return compressorMap.computeIfAbsent(compressionStrategy, key -> createCompressor(compressionStrategy));
  }

  private static VeniceCompressor createCompressor(CompressionStrategy compressionStrategy) {
    if (compressionStrategy == CompressionStrategy.GZIP) {
      return new GzipCompressor();
    } else if (compressionStrategy == CompressionStrategy.NO_OP) {
      return new NoopCompressor();
    }

    throw new IllegalArgumentException("unsupported compression strategy: " + compressionStrategy.toString());
  }
}
