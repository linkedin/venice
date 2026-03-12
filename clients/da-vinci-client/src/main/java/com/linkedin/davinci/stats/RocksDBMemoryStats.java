package com.linkedin.davinci.stats;

import com.linkedin.davinci.store.rocksdb.RocksDBStoragePartition;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Cache;
import org.rocksdb.SstFileManager;


/**
 * Class that exposes RocksDB memory consumption stats based on all properties
 * that are made available in https://github.com/facebook/rocksdb/blob/master/include/rocksdb/db.h#L870
 *
 * Properties exist on a per RockDB database basis (equivalent to a Venice partition).
 * This class aggregates across the partitions and emits aggregate stats
 */
public class RocksDBMemoryStats extends AbstractVeniceStats {
  private static final Logger LOGGER = LogManager.getLogger(RocksDBMemoryStats.class);

  // Derived from the OTel enum to avoid duplicating property strings in two places.
  static final List<String> PARTITION_METRIC_DOMAINS = buildPartitionMetricDomains();

  private static List<String> buildPartitionMetricDomains() {
    List<String> list = new ArrayList<>();
    for (RocksDBMemoryOtelMetricEntity entity: RocksDBMemoryOtelMetricEntity.values()) {
      if (entity.getRocksDBProperty() != null) {
        list.add(entity.getRocksDBProperty());
      }
    }
    return Collections.unmodifiableList(list);
  }

  // metrics emitted on a per instance basis need only be collected once, not aggregated
  private static final Set<String> INSTANCE_METRIC_DOMAINS = Collections.unmodifiableSet(
      new HashSet<>(
          Arrays.asList(
              RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_CAPACITY.getRocksDBProperty(),
              RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_PINNED_USAGE.getRocksDBProperty(),
              RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_USAGE.getRocksDBProperty())));
  private volatile long memoryLimit = -1;
  private volatile SstFileManager sstFileManager;

  // metrics related to block cache, which should not be collected when plain table format is enabled.
  private static final Set<String> BLOCK_CACHE_METRICS =
      PARTITION_METRIC_DOMAINS.stream().filter(s -> s.contains("rocksdb.block-cache")).collect(Collectors.toSet());

  private Map<String, RocksDBStoragePartition> hostedRocksDBPartitions = new ConcurrentHashMap<>();

  /** OTel repository; null when OTel is disabled or when using a plain {@link MetricsRepository}. */
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private final Attributes baseAttributes;

  /**
   * Tehuti metric name enum for all RocksDB memory sensors.
   *
   * <p>Property-backed constants (no-arg) derive their sensor name from the matching
   * {@link RocksDBMemoryOtelMetricEntity} constant's {@code getRocksDBProperty()}.
   * Non-property constants supply an explicit sensor name.
   */
  enum TehutiMetricName implements TehutiMetricNameEnum {
    // Property-backed metrics — sensor name derived from OTel enum's rocksDBProperty
    NUM_IMMUTABLE_MEM_TABLE, MEM_TABLE_FLUSH_PENDING, COMPACTION_PENDING, BACKGROUND_ERRORS, CUR_SIZE_ACTIVE_MEM_TABLE,
    CUR_SIZE_ALL_MEM_TABLES, SIZE_ALL_MEM_TABLES, NUM_ENTRIES_ACTIVE_MEM_TABLE, NUM_ENTRIES_IMM_MEM_TABLES,
    NUM_DELETES_ACTIVE_MEM_TABLE, NUM_DELETES_IMM_MEM_TABLES, ESTIMATE_NUM_KEYS, ESTIMATE_TABLE_READERS_MEM,
    NUM_SNAPSHOTS, NUM_LIVE_VERSIONS, ESTIMATE_LIVE_DATA_SIZE, MIN_LOG_NUMBER_TO_KEEP, TOTAL_SST_FILES_SIZE,
    LIVE_SST_FILES_SIZE, ESTIMATE_PENDING_COMPACTION_BYTES, NUM_RUNNING_COMPACTIONS, NUM_RUNNING_FLUSHES,
    ACTUAL_DELAYED_WRITE_RATE, BLOCK_CACHE_CAPACITY, BLOCK_CACHE_PINNED_USAGE, BLOCK_CACHE_USAGE, NUM_BLOB_FILES,
    TOTAL_BLOB_FILE_SIZE, LIVE_BLOB_FILE_SIZE, LIVE_BLOB_FILE_GARBAGE_SIZE,

    // Non-property metrics — explicit sensor name (these don't map to a RocksDB property)
    MEMORY_LIMIT("memory_limit"), MEMORY_USAGE("memory_usage"),
    RMD_BLOCK_CACHE_CAPACITY("rocksdb.rmd-block-cache-capacity"),
    RMD_BLOCK_CACHE_USAGE("rocksdb.rmd-block-cache-usage"),
    RMD_BLOCK_CACHE_PINNED_USAGE("rocksdb.rmd-block-cache-pinned-usage");

    private static final Map<String, TehutiMetricName> PROPERTY_LOOKUP;

    static {
      Map<String, TehutiMetricName> map = new HashMap<>();
      for (TehutiMetricName value: values()) {
        map.put(value.sensorName, value);
      }
      PROPERTY_LOOKUP = Collections.unmodifiableMap(map);
    }

    private final String sensorName;

    /** Derives sensor name from the matching {@link RocksDBMemoryOtelMetricEntity}'s rocksDBProperty. */
    TehutiMetricName() {
      this.sensorName = RocksDBMemoryOtelMetricEntity.valueOf(name()).getRocksDBProperty();
    }

    /** Explicit sensor name for non-property metrics. */
    TehutiMetricName(String sensorName) {
      this.sensorName = sensorName;
    }

    @Override
    public String getMetricName() {
      return sensorName;
    }

    static TehutiMetricName fromSensorName(String sensorName) {
      TehutiMetricName result = PROPERTY_LOOKUP.get(sensorName);
      if (result == null) {
        throw new IllegalArgumentException("Unknown sensor name: " + sensorName);
      }
      return result;
    }
  }

  public RocksDBMemoryStats(
      MetricsRepository metricsRepository,
      String name,
      boolean plainTableEnabled,
      String clusterName) {
    super(metricsRepository, name);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();
    this.baseAttributes = otelData.getBaseAttributes();

    for (String metric: PARTITION_METRIC_DOMAINS) {
      // Skip the block cache related metrics when PlainTable format is enabled.
      if (plainTableEnabled && BLOCK_CACHE_METRICS.contains(metric)) {
        continue;
      }
      registerAsyncGauge(
          RocksDBMemoryOtelMetricEntity.fromRocksDBProperty(metric),
          TehutiMetricName.fromSensorName(metric),
          () -> getAggregatedPropertyValue(metric));
    }

    registerAsyncGauge(RocksDBMemoryOtelMetricEntity.MEMORY_LIMIT, TehutiMetricName.MEMORY_LIMIT, () -> memoryLimit);
    registerAsyncGauge(RocksDBMemoryOtelMetricEntity.MEMORY_USAGE, TehutiMetricName.MEMORY_USAGE, this::getMemoryUsage);
  }

  public void setMemoryLimit(long memoryLimit) {
    this.memoryLimit = memoryLimit;
  }

  public void setSstFileManager(SstFileManager sstFileManager) {
    this.sstFileManager = sstFileManager;
  }

  public void registerPartition(String partitionName, RocksDBStoragePartition rocksDBPartition) {
    hostedRocksDBPartitions.put(partitionName, rocksDBPartition);
  }

  public void deregisterPartition(String partitionName) {
    // Synchronize on the hosted partitions so that this method does not return while
    // a metric collection is ongoing. This prevents venice-server from potentially
    // closing a RocksDB database while a property is being read.
    synchronized (hostedRocksDBPartitions) {
      hostedRocksDBPartitions.remove(partitionName);
    }
  }

  public void setRMDBlockCache(Cache rmdCache, long rmdCacheCapacity) {
    registerAsyncGauge(
        RocksDBMemoryOtelMetricEntity.RMD_BLOCK_CACHE_CAPACITY,
        TehutiMetricName.RMD_BLOCK_CACHE_CAPACITY,
        () -> rmdCacheCapacity);
    registerAsyncGauge(
        RocksDBMemoryOtelMetricEntity.RMD_BLOCK_CACHE_USAGE,
        TehutiMetricName.RMD_BLOCK_CACHE_USAGE,
        rmdCache::getUsage);
    registerAsyncGauge(
        RocksDBMemoryOtelMetricEntity.RMD_BLOCK_CACHE_PINNED_USAGE,
        TehutiMetricName.RMD_BLOCK_CACHE_PINNED_USAGE,
        rmdCache::getPinnedUsage);
  }

  /** Registers a joint Tehuti+OTel async gauge metric. */
  private void registerAsyncGauge(
      RocksDBMemoryOtelMetricEntity otelEntity,
      TehutiMetricName tehutiName,
      LongSupplier valueSupplier) {
    String sensorName = tehutiName.getMetricName();
    AsyncMetricEntityStateBase.create(
        otelEntity.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        tehutiName,
        Collections.singletonList(new AsyncGauge((ig, ig2) -> valueSupplier.getAsLong(), sensorName)),
        baseDimensionsMap,
        baseAttributes,
        valueSupplier);
  }

  /**
   * Aggregates the value of a RocksDB property across all hosted partitions.
   * Instance-level metrics (block cache) are sampled from a single partition.
   */
  private long getAggregatedPropertyValue(String metric) {
    long total = 0L;
    synchronized (hostedRocksDBPartitions) {
      for (RocksDBStoragePartition dbPartition: hostedRocksDBPartitions.values()) {
        try {
          total += dbPartition.getRocksDBStatValue(metric);
        } catch (VeniceException e) {
          LOGGER.warn("Could not get rocksDB metric {} with error:", metric, e);
          continue;
        }
        if (INSTANCE_METRIC_DOMAINS.contains(metric)) {
          // Collect this metric once from any available partition and move on
          break;
        }
      }
    }
    return total;
  }

  private long getMemoryUsage() {
    if (memoryLimit > 0 && sstFileManager != null) {
      return sstFileManager.getTotalSize();
    }
    return -1;
  }
}
