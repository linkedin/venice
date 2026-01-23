package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.stats.AbstractVeniceAggStoreStats;
import com.linkedin.venice.stats.StatsSupplier;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;


/**
 * Aggregates host-level ingestion statistics across all stores.
 * This class provides aggregate initialization and management support for {@link HostLevelIngestionStats},
 * enabling centralized tracking of ingestion metrics at the host level. It extends
 * {@link AbstractVeniceAggStoreStats} to provide automatic aggregation across all stores on the host.
 */
public class AggHostLevelIngestionStats extends AbstractVeniceAggStoreStats<HostLevelIngestionStats> {
  /**
   * Constructs an AggHostLevelIngestionStats instance.
   *
   * @param metricsRepository the metrics repository for recording statistics
   * @param serverConfig the Venice server configuration
   * @param ingestionTaskMap a map of store names to their corresponding ingestion tasks
   * @param metadataRepository the store metadata repository
   * @param unregisterMetricForDeletedStoreEnabled whether to unregister metrics when stores are deleted
   * @param time the time instance for timestamp operations
   */
  public AggHostLevelIngestionStats(
      MetricsRepository metricsRepository,
      VeniceServerConfig serverConfig,
      Map<String, StoreIngestionTask> ingestionTaskMap,
      ReadOnlyStoreRepository metadataRepository,
      boolean unregisterMetricForDeletedStoreEnabled,
      Time time) {
    super(
        serverConfig.getClusterName(),
        metricsRepository,
        new HostLevelStoreIngestionStatsSupplier(serverConfig, ingestionTaskMap, time),
        metadataRepository,
        unregisterMetricForDeletedStoreEnabled,
        false);
  }

  /**
   * Supplier implementation for creating {@link HostLevelIngestionStats} instances.
   * This class encapsulates the configuration and dependencies needed to instantiate
   * host-level ingestion statistics for individual stores.
   */
  static class HostLevelStoreIngestionStatsSupplier implements StatsSupplier<HostLevelIngestionStats> {
    private final VeniceServerConfig serverConfig;
    private final Map<String, StoreIngestionTask> ingestionTaskMap;
    private final Time time;

    /**
     * Constructs a HostLevelStoreIngestionStatsSupplier.
     *
     * @param serverConfig the Venice server configuration
     * @param ingestionTaskMap a map of store names to their corresponding ingestion tasks
     * @param time the time instance for timestamp operations
     */
    HostLevelStoreIngestionStatsSupplier(
        VeniceServerConfig serverConfig,
        Map<String, StoreIngestionTask> ingestionTaskMap,
        Time time) {
      this.serverConfig = serverConfig;
      this.ingestionTaskMap = ingestionTaskMap;
      this.time = time;
    }

    /**
     * This method is not supported and will throw an exception.
     * Use {@link #get(MetricsRepository, String, String, HostLevelIngestionStats)} instead.
     *
     * @param metricsRepository the metrics repository
     * @param storeName the store name
     * @param clusterName the cluster name
     * @return never returns normally
     * @throws VeniceException always thrown as this method should not be called
     */
    @Override
    public HostLevelIngestionStats get(MetricsRepository metricsRepository, String storeName, String clusterName) {
      throw new VeniceException("Should not be called.");
    }

    /**
     * Creates a new {@link HostLevelIngestionStats} instance for the specified store.
     *
     * @param metricsRepository the metrics repository for recording statistics
     * @param storeName the name of the store
     * @param clusterName the name of the cluster
     * @param totalStats the total stats instance for aggregation across all stores
     * @return a new HostLevelIngestionStats instance for the specified store
     */
    @Override
    public HostLevelIngestionStats get(
        MetricsRepository metricsRepository,
        String storeName,
        String clusterName,
        HostLevelIngestionStats totalStats) {
      return new HostLevelIngestionStats(
          metricsRepository,
          serverConfig,
          storeName,
          totalStats,
          ingestionTaskMap,
          time);
    }
  }
}
