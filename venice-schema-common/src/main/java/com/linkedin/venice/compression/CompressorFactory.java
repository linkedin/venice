package com.linkedin.venice.compression;

import com.github.luben.zstd.Zstd;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CompressorFactory implements Closeable, AutoCloseable {
  private static final Logger logger = LogManager.getLogger(CompressorFactory.class);
  private final Map<CompressionStrategy, VeniceCompressor> compressorMap = new VeniceConcurrentHashMap<>();
  private final Map<String, VeniceCompressor> versionSpecificCompressorMap = new VeniceConcurrentHashMap<>();

  public VeniceCompressor getCompressor(CompressionStrategy compressionStrategy) {
    return compressorMap.computeIfAbsent(compressionStrategy, key -> createCompressor(compressionStrategy));
  }

  public VeniceCompressor createVersionSpecificCompressorIfNotExist(
      CompressionStrategy compressionStrategy,
      String kafkaTopic,
      final byte[] dictionary) {
    if (compressionStrategy != CompressionStrategy.ZSTD_WITH_DICT) {
      return getCompressor(compressionStrategy);
    } else {
      return createVersionSpecificCompressorIfNotExist(
          compressionStrategy,
          kafkaTopic,
          dictionary,
          Zstd.maxCompressionLevel());
    }
  }

  public VeniceCompressor createVersionSpecificCompressorIfNotExist(
      CompressionStrategy compressionStrategy,
      String kafkaTopic,
      final byte[] dictionary,
      int level) {
    return versionSpecificCompressorMap
        .computeIfAbsent(kafkaTopic, key -> createCompressorWithDictionary(compressionStrategy, dictionary, level));
  }

  public VeniceCompressor getVersionSpecificCompressor(String kafkaTopic) {
    return versionSpecificCompressorMap.get(kafkaTopic);
  }

  public void removeVersionSpecificCompressor(String kafkaTopic) {
    VeniceCompressor previousCompressor = versionSpecificCompressorMap.remove(kafkaTopic);
    if (previousCompressor != null) {
      try {
        previousCompressor.close();
      } catch (IOException e) {
        logger.warn(
            "Previous compressor with strategy " + previousCompressor.getCompressionStrategy() + " for " + kafkaTopic
                + " exists but it could not be closed due to IO Exception: " + e.toString());
      }
    }
  }

  public boolean versionSpecificCompressorExists(String kafkaTopic) {
    return versionSpecificCompressorMap.containsKey(kafkaTopic);
  }

  public VeniceCompressor createCompressor(CompressionStrategy compressionStrategy) {
    if (compressionStrategy == CompressionStrategy.GZIP) {
      return new GzipCompressor();
    } else if (compressionStrategy == CompressionStrategy.NO_OP) {
      return new NoopCompressor();
    }

    throw new IllegalArgumentException("unsupported compression strategy: " + compressionStrategy.toString());
  }

  public VeniceCompressor createCompressorWithDictionary(
      CompressionStrategy compressionStrategy,
      final byte[] dictionary,
      int level) {
    if (compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
      return new ZstdWithDictCompressor(dictionary, level);
    }

    throw new IllegalArgumentException(
        "unsupported compression strategy with dictionary: " + compressionStrategy.toString());
  }

  @Override
  public void close() {
    for (VeniceCompressor compressor: compressorMap.values()) {
      IOUtils.closeQuietly(compressor, logger::error);
    }

    for (String topic: versionSpecificCompressorMap.keySet()) {
      removeVersionSpecificCompressor(topic);
    }
  }
}
