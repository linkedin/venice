package com.linkedin.venice.compression;

import com.github.luben.zstd.Zstd;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;


public class CompressorFactory {
  private static Map<CompressionStrategy, VeniceCompressor> compressorMap = new VeniceConcurrentHashMap<>();
  // TODO: Clean up retired store versions
  private static Map<String, VeniceCompressor> versionSpecificCompressorMap = new VeniceConcurrentHashMap<>();

  public static VeniceCompressor getCompressor(CompressionStrategy compressionStrategy) {
    return compressorMap.computeIfAbsent(compressionStrategy, key -> createCompressor(compressionStrategy));
  }

  public static VeniceCompressor getVersionSpecificCompressor(CompressionStrategy compressionStrategy, String kafkaTopic, final byte[] dictionary) {
    return getVersionSpecificCompressor(compressionStrategy, kafkaTopic, dictionary, Zstd.maxCompressionLevel());
  }

  public static VeniceCompressor getVersionSpecificCompressor(CompressionStrategy compressionStrategy, String kafkaTopic, final byte[] dictionary, int level) {
    return versionSpecificCompressorMap.computeIfAbsent(kafkaTopic, key -> createCompressorWithDictionary(compressionStrategy, dictionary, level));
  }

  public static VeniceCompressor getVersionSpecificCompressor(String kafkaTopic) {
    return versionSpecificCompressorMap.get(kafkaTopic);
  }

  public static void removeVersionSpecificCompressor(String kafkaTopic) {
    versionSpecificCompressorMap.remove(kafkaTopic);
  }

  private static VeniceCompressor createCompressor(CompressionStrategy compressionStrategy) {
    if (compressionStrategy == CompressionStrategy.GZIP) {
      return new GzipCompressor();
    } else if (compressionStrategy == CompressionStrategy.NO_OP) {
      return new NoopCompressor();
    }

    throw new IllegalArgumentException("unsupported compression strategy: " + compressionStrategy.toString());
  }

  private static VeniceCompressor createCompressorWithDictionary(CompressionStrategy compressionStrategy, final byte[] dictionary, int level) {
    if (compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
      return new ZstdWithDictCompressor(dictionary, level);
    }

    throw new IllegalArgumentException("unsupported compression strategy with dictionary: " + compressionStrategy.toString());
  }
}