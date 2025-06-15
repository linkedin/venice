package com.linkedin.venice.cuda;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerFunctionalInterface;
import com.linkedin.davinci.client.factory.DaVinciClientFactory;
import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A wrapper around DaVinciClient that manages GPU-backed storage for Venice data.
 * This class handles the lifecycle of GPU transformers across different versions.
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public class GPUBackedDaVinciClient<K, V> implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(GPUBackedDaVinciClient.class);

  private final String storeName;
  private final DaVinciClientFactory daVinciClientFactory;
  private DaVinciClient<K, V> daVinciClient;
  // TODO: Think through what parameterization makes sense for the transformer relative to this class
  private final ConcurrentSkipListMap<Integer, GPUDaVinciRecordTransformer<K, V, V>> versionTransformers;
  private final int gpuDeviceId;
  private final int initialCapacity;

  /**
   * Constructor with custom transformer factory
   */
  public GPUBackedDaVinciClient(
      String storeName,
      String veniceUrl,
      int gpuDeviceId,
      int initialCapacity,
      DaVinciClientFactory daVinciClientFactory) {

    this.storeName = storeName;
    this.gpuDeviceId = gpuDeviceId;
    this.initialCapacity = initialCapacity;
    this.versionTransformers = new ConcurrentSkipListMap<>();
    this.daVinciClientFactory = daVinciClientFactory;
  }

  /**
   * Start the client and begin ingestion
   */
  public void start() {
    LOGGER.info("Starting GPU-backed DaVinci client for store: {}", storeName);

    DaVinciRecordTransformerFunctionalInterface recordTransformerFunction =
        (storeVersion, keySchema, inputValueSchema, outputValueSchema, config) -> {
          GPUDaVinciRecordTransformer<K, V, V> transformer = new GPUDaVinciRecordTransformer<K, V, V>(
              storeVersion,
              keySchema,
              inputValueSchema,
              outputValueSchema,
              config,
              this.gpuDeviceId,
              this.initialCapacity);

          versionTransformers.put(storeVersion, transformer);

          return transformer;
        };

    DaVinciRecordTransformerConfig recordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(recordTransformerFunction).build();

    DaVinciConfig daVinciConfig = new DaVinciConfig().setRecordTransformerConfig(recordTransformerConfig);

    // Create the DaVinci client
    this.daVinciClient = daVinciClientFactory.getAndStartGenericAvroClient(storeName, daVinciConfig);

    LOGGER.info("Started GPU-backed DaVinci client for store: {} with GPU device: {}", storeName, this.gpuDeviceId);
  }

  /**
   * Get a value from the current version's GPU storage
   */
  public byte[] get(K key) {
    GPUDaVinciRecordTransformer<K, V, V> transformer = getCurrentTransformer();
    if (transformer != null) {
      return transformer.get(key);
    }
    return null;
  }

  /**
   * Get the current active transformer
   */
  private GPUDaVinciRecordTransformer<K, V, V> getCurrentTransformer() {
    // TODO: we don't have a GREAT way to figure out which version is both the current
    // version and the most caught up version. So we apply the following heuristic:
    // we will always take the minimum version number which is still running. This works
    // most the time, but it doesn't handle rollback well. It doesn't handle it well
    // because a rolled back to version may be mid ingestion, and can't technically be
    // 'current' because it's not ready to serve. We need to augment the transformer
    // interface to track heartbeats and have a notion of being 'ready'.
    for (Integer storeVersion: versionTransformers.keySet()) {
      if (versionTransformers.get(storeVersion).isInitialized()) {
        return versionTransformers.get(storeVersion);
      } else {
        // We're relying on onEndIngestionTask to properly close out
        // and clean up gpu resources.
        versionTransformers.remove(storeVersion);
      }
    }
    return null;
  }

  CompletableFuture<Void> subscribeAll() {
    return daVinciClient.subscribeAll();
  }

  CompletableFuture<Void> subscribe(Set<Integer> partitions) {
    return daVinciClient.subscribe(partitions);
  }

  @Override
  public void close() {
    LOGGER.info("Closing GPU-backed DaVinci client for store: {}", storeName);
    // Close the DaVinci client
    try {
      daVinciClient.unsubscribeAll();
    } catch (Exception e) {
      LOGGER.error("Error closing DaVinci client", e);
    }

    LOGGER.info("GPU-backed DaVinci client closed for store: {}", storeName);
  }
}
