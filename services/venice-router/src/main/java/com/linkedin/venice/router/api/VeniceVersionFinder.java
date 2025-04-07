package com.linkedin.venice.router.api;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceStoreIsMigratedException;
import com.linkedin.venice.exceptions.VeniceStoreNotReadyToServeException;
import com.linkedin.venice.helix.HelixBaseRoutingRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.router.stats.RouterCurrentVersionStats;
import com.linkedin.venice.router.stats.StaleVersionReason;
import com.linkedin.venice.router.stats.StaleVersionStats;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@code VeniceVersionFinder} provides methods to find the version for a given store.
 */
public class VeniceVersionFinder {
  private static final Logger LOGGER = LogManager.getLogger(VeniceVersionFinder.class);
  private static final RedundantExceptionFilter EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  private final ReadOnlyStoreRepository metadataRepository;
  private final StaleVersionStats stats;
  private final ReadOnlyStoreConfigRepository storeConfigRepo;
  private final Map<String, String> clusterToD2Map;
  private final String clusterName;
  private final VeniceConcurrentHashMap<String, Integer> lastCurrentVersionMap = new VeniceConcurrentHashMap<>();

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
    final Store[] store = { metadataRepository.getStore(storeName) };
    if (store[0] == null || store[0].isMigrating()) {
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
      if (store[0] == null) {
        throw new VeniceNoStoreException(storeName);
      }
    }
    if (!store[0].isEnableReads()) {
      throw new StoreDisabledException(storeName, "read");
    }

    final int[] metadataCurrentVersion = { store[0].getCurrentVersion() };
    lastCurrentVersionMap.computeIfAbsent(storeName, v -> {
      if (metadataCurrentVersion[0] == Store.NON_EXISTING_VERSION) {
        store[0] = metadataRepository.refreshOneStore(storeName);
        metadataCurrentVersion[0] = store[0].getCurrentVersion();
      }

      // check if new store is ready to serve
      VersionStatus versionStatus = store[0].getVersionStatus(metadataCurrentVersion[0]);
      String kafkaTopic = Version.composeKafkaTopic(storeName, metadataCurrentVersion[0]);
      if (versionStatus.equals(VersionStatus.ONLINE) && isPartitionResourcesReady(kafkaTopic)
          && isDecompressorReady(store[0], metadataCurrentVersion[0])) {
        // new store ready to serve
        return metadataCurrentVersion[0];
      }
      // new store not ready to serve
      return null;
    });

    if (!lastCurrentVersionMap.containsKey(storeName)) {
      throw new VeniceStoreNotReadyToServeException(storeName, metadataCurrentVersion[0]);
    }

    int existingVersion = lastCurrentVersionMap.get(storeName);
    if (existingVersion == metadataCurrentVersion[0]) {
      // new store ready to serve or same store version (no version swap)
      stats.recordNotStale();
      return existingVersion;
    }

    // version swap: new version
    VersionStatus newVersionStatus = store[0].getVersionStatus(metadataCurrentVersion[0]);
    String newVersionKafkaTopic = Version.composeKafkaTopic(storeName, metadataCurrentVersion[0]);
    boolean newVersionPartitionResourcesReady = isPartitionResourcesReady(newVersionKafkaTopic);
    boolean newVersionDecompressorReady = isDecompressorReady(store[0], metadataCurrentVersion[0]);
    if (newVersionPartitionResourcesReady && newVersionDecompressorReady) {
      // new version ready to serve
      storeStats.computeIfAbsent(storeName, metric -> new RouterCurrentVersionStats(metricsRepository, storeName))
          .updateCurrentVersion(metadataCurrentVersion[0]);
      lastCurrentVersionMap.put(storeName, metadataCurrentVersion[0]);
      return metadataCurrentVersion[0];
    }

    // new version not ready to serve
    String errorMessage = "Unable to serve new version: " + newVersionKafkaTopic + ".";
    if (!newVersionPartitionResourcesReady) {
      errorMessage += " Partition resources not ready for new version.";
      stats.recordStalenessReason(StaleVersionReason.OFFLINE_PARTITIONS);
    }
    if (!newVersionDecompressorReady) {
      errorMessage += " Decompressor not ready for new version (Has dictionary downloaded?).";
      stats.recordStalenessReason(StaleVersionReason.DICTIONARY_NOT_DOWNLOADED);
    }

    // check if existing version ready to serve
    VersionStatus existingVersionStatus = store[0].getVersionStatus(existingVersion);
    String existingVersionKafkaTopic = Version.composeKafkaTopic(storeName, existingVersion);
    boolean existingVersionStatusOnline = existingVersionStatus.equals(VersionStatus.ONLINE);
    boolean existingVersionDecompressorReady = isDecompressorReady(store[0], existingVersion);
    if (existingVersionStatusOnline && existingVersionDecompressorReady) {
      // existing version ready to serve
      if (!EXCEPTION_FILTER.isRedundantException(errorMessage)) {
        LOGGER.warn(errorMessage);
      }
      stats.recordStale(metadataCurrentVersion[0], existingVersion);
      return existingVersion;
    }

    // existing version not ready to serve -> no version ready to serve
    errorMessage += " Unable to serve existing version: " + existingVersion + ".";
    if (!existingVersionStatusOnline) {
      errorMessage += " Previous version has status: " + existingVersionStatus + ".";
    }
    if (!existingVersionDecompressorReady) {
      errorMessage += " Decompressor not ready for previous version (Has dictionary downloaded?).";
    }
    errorMessage += " No version ready to serve.";
    if (!EXCEPTION_FILTER.isRedundantException(errorMessage)) {
      LOGGER.warn(errorMessage);
    }
    stats.recordStale(metadataCurrentVersion[0], Store.NON_EXISTING_VERSION);
    lastCurrentVersionMap.remove(storeName);
    throw new VeniceStoreNotReadyToServeException(storeName);
  }

  private boolean isPartitionResourcesReady(String kafkaTopic) {
    if (!routingDataRepository.containsKafkaTopic(kafkaTopic)) {
      return false;
    }
    int partitionCount = routingDataRepository.getNumberOfPartitions(kafkaTopic);
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      List<Instance> partitionHosts = routingDataRepository.getReadyToServeInstances(kafkaTopic, partitionId);
      if (partitionHosts.isEmpty()) {
        String partitionAssignment;
        try {
          partitionAssignment = routingDataRepository.getAllInstances(kafkaTopic, partitionId).toString();
        } catch (Exception e) {
          LOGGER.warn("Failed to get partition assignment for resource: {}", kafkaTopic, e);
          partitionAssignment = "unknown";
        }
        String message = "No online replica exists for partition " + partitionId + " of " + kafkaTopic
            + ", partition assignment: " + partitionAssignment;
        if (!EXCEPTION_FILTER.isRedundantException(message)) {
          LOGGER.warn(message);
        }
        return false;
      }
    }
    return true;
  }

  private boolean isDecompressorReady(Store store, int versionNumber) {
    Version version = store.getVersion(versionNumber);
    if (version == null) {
      return false;
    }
    return version.getCompressionStrategy() != CompressionStrategy.ZSTD_WITH_DICT
        || compressorFactory.versionSpecificCompressorExists(Version.composeKafkaTopic(store.getName(), versionNumber));
  }
}
