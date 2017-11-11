package com.linkedin.venice.compression;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class CompressorFactory {
  private static Map<CompressionStrategy, VeniceCompressor> compressorMap = new ConcurrentHashMap<>();

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
