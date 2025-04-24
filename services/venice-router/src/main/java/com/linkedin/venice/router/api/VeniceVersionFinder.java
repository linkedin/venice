package com.linkedin.venice.router.api;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceStoreIsMigratedException;
import com.linkedin.venice.helix.HelixBaseRoutingRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.router.stats.RouterCurrentVersionStats;
import com.linkedin.venice.router.stats.StaleVersionReason;
import com.linkedin.venice.router.stats.StaleVersionStats;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@code VeniceVersionFinder} provides methods to find the version for a given store.
 */
public class VeniceVersionFinder {
  private static final Logger LOGGER = LogManager.getLogger(VeniceVersionFinder.class);
  private static final RedundantExceptionFilter EXCEPTION_FILTER =
      new RedundantExceptionFilter(RedundantExceptionFilter.DEFAULT_BITSET_SIZE, TimeUnit.MINUTES.toMillis(5));

  private final ReadOnlyStoreRepository metadataRepository;
  private final StaleVersionStats stats;
  private final ReadOnlyStoreConfigRepository storeConfigRepo;
  private final Map<String, String> clusterToD2Map;
  private final String clusterName;
  private final ConcurrentMap<String, Integer> lastCurrentVersionMap = new VeniceConcurrentHashMap<>();

  protected final Map<String, RouterCurrentVersionStats> storeStats = new VeniceConcurrentHashMap<>();

  private final HelixBaseRoutingRepository routingDataRepository;
  private final CompressorFactory compressorFactory;

  private final MetricsRepository metricsRepository;

  public VeniceVersionFinder(
      ReadOnlyStoreRepository metadataRepository,
      HelixBaseRoutingRepository routingDataRepository,
      StaleVersionStats stats,
      ReadOnlyStoreConfigRepository storeConfigRepo,
      Map<String, String> clusterToD2Map,
      String clusterName,
      CompressorFactory compressorFactory,
      MetricsRepository metricsRepository) {
    this.metadataRepository = metadataRepository;
    this.routingDataRepository = routingDataRepository;
    this.stats = stats;
    this.storeConfigRepo = storeConfigRepo;
    this.clusterToD2Map = clusterToD2Map;
    this.clusterName = clusterName;
    this.compressorFactory = compressorFactory;
    this.metricsRepository = metricsRepository;
  }

  public int getVersion(String storeName, BasicFullHttpRequest request) throws VeniceException {
    /**
     * TODO: clone a store object is too expensive, and we could choose to expose the necessary methods
     * in {@link ReadOnlyStoreRepository}, such as 'isEnableReads' and others.
     */
    Store store = metadataRepository.getStore(storeName);
    if (store == null || store.isMigrating()) {
      // The client might be idle for a long time while the store is migrated. Check for store migration.
      if (request != null && request.headers().contains(HttpConstants.VENICE_ALLOW_REDIRECT)) {
        Optional<StoreConfig> config = storeConfigRepo.getStoreConfig(storeName);
        if (config.isPresent()) {
          String newCluster = config.get().getCluster();
          if (!clusterName.equals(newCluster)) {
            String d2Service = clusterToD2Map.get(newCluster);
            throw new VeniceStoreIsMigratedException(storeName, newCluster, d2Service);
          }
        }
      }
      if (store == null) {
        throw new VeniceNoStoreException(storeName);
      }
    }
    if (!store.isEnableReads()) {
      throw new StoreDisabledException(storeName, "read");
    }

    int metadataCurrentVersionNumber = store.getCurrentVersion();
    String newVersionKafkaTopic;
    boolean newVersionPartitionResourcesReady;
    boolean newVersionDecompressorReady;

    Integer existingVersionNumber = lastCurrentVersionMap.get(storeName);
    if (existingVersionNumber == null) {
      if (metadataCurrentVersionNumber == Store.NON_EXISTING_VERSION) {
        /** new store push has completed but the routers are not up-to-date on the latest metadata yet
         * This should happen at most once per store, since we are adding the mapping to {@link lastCurrentVersion} */
        store = metadataRepository.refreshOneStore(storeName);
        metadataCurrentVersionNumber = store.getCurrentVersion();
      }
      // check if new store is ready to serve
      lastCurrentVersionMap.putIfAbsent(storeName, metadataCurrentVersionNumber);
      newVersionKafkaTopic = Version.composeKafkaTopic(storeName, metadataCurrentVersionNumber);
      newVersionPartitionResourcesReady = isPartitionResourcesReady(newVersionKafkaTopic);
      newVersionDecompressorReady =
          isDecompressorReady(store.getVersion(metadataCurrentVersionNumber), newVersionKafkaTopic);
      if (newVersionPartitionResourcesReady && newVersionDecompressorReady) {
        // new store ready to serve
        existingVersionNumber = metadataCurrentVersionNumber;
      } else {
        // new store not ready to serve
        return Store.NON_EXISTING_VERSION;
      }
    }

    if (existingVersionNumber == metadataCurrentVersionNumber) {
      // new store ready to serve or same store version (no version swap)
      stats.recordNotStale();
      return existingVersionNumber;
    }

    // version swap: check if new version of existing store is ready to serve
    newVersionKafkaTopic = Version.composeKafkaTopic(storeName, metadataCurrentVersionNumber);
    newVersionPartitionResourcesReady = isPartitionResourcesReady(newVersionKafkaTopic);
    newVersionDecompressorReady =
        isDecompressorReady(store.getVersion(metadataCurrentVersionNumber), newVersionKafkaTopic);
    if (newVersionPartitionResourcesReady && newVersionDecompressorReady) {
      // new version ready to serve
      storeStats.computeIfAbsent(storeName, metric -> new RouterCurrentVersionStats(metricsRepository, storeName))
          .updateCurrentVersion(metadataCurrentVersionNumber);
      lastCurrentVersionMap.put(storeName, metadataCurrentVersionNumber);
      return metadataCurrentVersionNumber;
    }

    // new version not ready to serve
    if (!newVersionPartitionResourcesReady) {
      stats.recordStalenessReason(StaleVersionReason.OFFLINE_PARTITIONS);
    }
    if (!newVersionDecompressorReady) {
      stats.recordStalenessReason(StaleVersionReason.DICTIONARY_NOT_DOWNLOADED);
    }

    /**
     * When the router has only one available version, despite offline partitions, or dictionary not yet downloaded,
     * it will return it as the available version.
     * If the partitions are still unavailable or the dictionary is not downloaded by the time the records needs to
     * be decompressed, then the router will return an error response.
     */
    // log if existing version ready to serve
    Version existingVersion = store.getVersion(existingVersionNumber);
    boolean existingVersionDecompressorReady =
        isDecompressorReady(existingVersion, Version.composeKafkaTopic(storeName, existingVersionNumber));
    // existing version ready to serve
    if (!EXCEPTION_FILTER.isRedundantException(storeName)) {
      LOGGER.warn(
          "Unable to serve new version: {}." + " Partition resources ready for new version? {}."
              + " Decompressor not ready for new version? {}." + " Continuing to serve existing version: {}."
              + " Decompressor ready for existing version? {}.",
          newVersionKafkaTopic,
          newVersionPartitionResourcesReady,
          newVersionDecompressorReady,
          existingVersionNumber,
          existingVersionDecompressorReady);
    }
    stats.recordStale(metadataCurrentVersionNumber, existingVersionNumber);
    lastCurrentVersionMap.put(storeName, existingVersionNumber);
    /** new and existing version both being not ready is a rare case. However, returning something is better than nothing. */
    return existingVersionNumber;
  }

  protected boolean isPartitionResourcesReady(String kafkaTopic) {
    if (!routingDataRepository.containsKafkaTopic(kafkaTopic)) {
      return false;
    }
    int partitionCount = routingDataRepository.getNumberOfPartitions(kafkaTopic);

    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      List<Instance> partitionHosts = routingDataRepository.getReadyToServeInstances(kafkaTopic, partitionId);
      if (partitionHosts.isEmpty()) {
        String partitionAssignment;
        String errorMessage = "";
        try {
          partitionAssignment = routingDataRepository.getAllInstances(kafkaTopic, partitionId).toString();
        } catch (Exception e) {
          errorMessage += "Failed to get partition assignment for resource: " + kafkaTopic + " " + e;
          partitionAssignment = "unknown";
        }

        errorMessage += "No online replica exists for partition " + partitionId + " of " + kafkaTopic
            + ", partition assignment: " + partitionAssignment;
        if (!EXCEPTION_FILTER.isRedundantException(errorMessage)) {
          LOGGER.warn(errorMessage);
        }
        return false;
      }
    }
    return true;
  }

  protected boolean isDecompressorReady(Version version, String kafkaTopicName) {
    if (version == null) {
      return false;
    }
    return version.getCompressionStrategy() != CompressionStrategy.ZSTD_WITH_DICT
        || compressorFactory.versionSpecificCompressorExists(kafkaTopicName);
  }
}
