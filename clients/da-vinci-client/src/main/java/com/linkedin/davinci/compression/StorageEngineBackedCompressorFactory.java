package com.linkedin.davinci.compression;

import static com.linkedin.venice.compression.CompressionStrategy.ZSTD_WITH_DICT;

import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;


public class StorageEngineBackedCompressorFactory extends CompressorFactory {
  private final StorageMetadataService metadataService;

  public StorageEngineBackedCompressorFactory(StorageMetadataService metadataService) {
    this.metadataService = metadataService;
  }

  public VeniceCompressor getCompressor(CompressionStrategy compressionStrategy, String kafkaTopic) {
    if (ZSTD_WITH_DICT.equals(compressionStrategy)) {
      VeniceCompressor compressor = getVersionSpecificCompressor(kafkaTopic);
      if (compressor != null) {
        return compressor;
      }

      ByteBuffer dictionary = metadataService.getStoreVersionCompressionDictionary(kafkaTopic);
      if (dictionary == null) {
        throw new IllegalStateException("Got a null dictionary for: " + kafkaTopic);
      }
      return super.createVersionSpecificCompressorIfNotExist(
          compressionStrategy,
          kafkaTopic,
          ByteUtils.extractByteArray(dictionary));
    } else {
      return getCompressor(compressionStrategy);
    }
  }
}
