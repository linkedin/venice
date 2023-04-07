package com.linkedin.venice.compression;

import com.github.luben.zstd.Zstd;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CompressorFactory implements Closeable, AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(CompressorFactory.class);
  private final VeniceCompressor NO_OP_COMPRESSOR = new NoopCompressor();
  private final VeniceCompressor GZIP_COMPRESSOR = new GzipCompressor();
  private final Map<String, VeniceCompressor> versionSpecificCompressorMap = new VeniceConcurrentHashMap<>();

  public VeniceCompressor getCompressor(CompressionStrategy compressionStrategy) {
    switch (compressionStrategy) {
      case NO_OP:
        return NO_OP_COMPRESSOR;
      case GZIP:
        return GZIP_COMPRESSOR;
      case ZSTD_WITH_DICT:
        throw new IllegalArgumentException(
            "For " + CompressionStrategy.ZSTD_WITH_DICT + ", please call createVersionSpecificCompressorIfNotExist.");
      default:
        throw new IllegalArgumentException("Unsupported compression: " + compressionStrategy);
    }
  }

  public VeniceCompressor createVersionSpecificCompressorIfNotExist(
      CompressionStrategy compressionStrategy,
      String kafkaTopic,
      final byte[] dictionary) {
    return createVersionSpecificCompressorIfNotExist(
        compressionStrategy,
        kafkaTopic,
        dictionary,
        Zstd.maxCompressionLevel());
  }

  public VeniceCompressor createVersionSpecificCompressorIfNotExist(
      CompressionStrategy compressionStrategy,
      String kafkaTopic,
      final byte[] dictionary,
      int compressionLevel) {
    switch (compressionStrategy) {
      case ZSTD_WITH_DICT:
        if (dictionary == null) {
          throw new VeniceException("Null dictionary with ZSTD_WITH_DICT compression strategy isn't supported!");
        }
        return versionSpecificCompressorMap
            .computeIfAbsent(kafkaTopic, key -> createCompressorWithDictionary(dictionary, compressionLevel));
      default:
        return getCompressor(compressionStrategy);
    }
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
        LOGGER.warn(
            "Previous compressor with strategy {} for {} exists but it could not be closed due to IO Exception: {}",
            previousCompressor.getCompressionStrategy(),
            kafkaTopic,
            e);
      }
    }
  }

  public boolean versionSpecificCompressorExists(String kafkaTopic) {
    return versionSpecificCompressorMap.containsKey(kafkaTopic);
  }

  public VeniceCompressor createCompressorWithDictionary(final byte[] dictionary, int level) {
    return new ZstdWithDictCompressor(dictionary, level);
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(NO_OP_COMPRESSOR, LOGGER::error);
    IOUtils.closeQuietly(GZIP_COMPRESSOR, LOGGER::error);

    for (String topic: versionSpecificCompressorMap.keySet()) {
      removeVersionSpecificCompressor(topic);
    }
  }
}
