package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CompressionDictionaryHelper {
  private static final Logger LOGGER = LogManager.getLogger(CompressionDictionaryHelper.class);

  public static VeniceCompressor getCompressor(
      CompressionStrategy compressionStrategy,
      CompressorFactory compressorFactory,
      Map<Integer, ByteBuffer> versionZstdDictionaryMap,
      int version,
      String storeName) {
    if (compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
      String resourceName = storeName + "_v" + version;
      VeniceCompressor compressor = compressorFactory.getVersionSpecificCompressor(resourceName);
      if (compressor == null) {
        ByteBuffer dictionary = versionZstdDictionaryMap.get(version);
        if (dictionary == null) {
          throw new VeniceClientException(
              String.format(
                  "No dictionary available for decompressing zstd payload for store %s version %d ",
                  storeName,
                  version));
        } else {
          compressor = compressorFactory
              .createVersionSpecificCompressorIfNotExist(compressionStrategy, resourceName, dictionary.array());
        }
      }
      return compressor;
    } else {
      return compressorFactory.getCompressor(compressionStrategy);
    }
  }

  public static CompletableFuture<TransportClientResponse> performFetchRequest(
      CompletableFuture<TransportClientResponse> compressionDictionaryFuture,
      TransportClient transportClient,
      Map<Integer, ByteBuffer> versionZstdDictionaryMap,
      String url,
      int version,
      String storeName) {
    LOGGER.info("Fetching compression dictionary for version {} from URL {} ", version, url);
    transportClient.get(url).whenComplete((response, throwable) -> {
      if (throwable != null) {
        String message = String.format(
            "Problem fetching zstd compression dictionary from URL:%s for store:%s , version:%d",
            url,
            storeName,
            version);
        LOGGER.warn(message, throwable);
        compressionDictionaryFuture.completeExceptionally(throwable);
      } else {
        byte[] dictionary = response.getBody();
        versionZstdDictionaryMap.put(version, ByteBuffer.wrap(dictionary));
        compressionDictionaryFuture.complete(response);
      }
    });
    return compressionDictionaryFuture;
  }
}
