package com.linkedin.davinci.stats;

import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.davinci.store.rocksdb.RocksDBStoragePartition;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Cache;


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
  private static final Set<String> INSTANCE_METRIC_DOMAINS = setOf(
      RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_CAPACITY.getRocksDBProperty(),
      RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_PINNED_USAGE.getRocksDBProperty(),
      RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_USAGE.getRocksDBProperty());

  // metrics related to block cache, which should not be collected when plain table format is enabled.
  private static final Set<String> BLOCK_CACHE_METRICS = setOf(
      RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_CAPACITY.getRocksDBProperty(),
      RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_USAGE.getRocksDBProperty(),
      RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_PINNED_USAGE.getRocksDBProperty());

  private final Map<String, RocksDBStoragePartition> hostedRocksDBPartitions = new VeniceConcurrentHashMap<>();
  private final AtomicBoolean rmdBlockCacheRegistered = new AtomicBoolean(false);

  /** OTel repository; null when OTel is disabled or when using a plain {@link MetricsRepository}. */
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private final Attributes baseAttributes;

  /**
   * Tehuti metric name enum for all RocksDB memory sensors.
   *
   * <p>Property-backed constants derive their sensor name from the explicitly referenced
   * {@link RocksDBMemoryOtelMetricEntity} constant's {@code getRocksDBProperty()}.
   * Non-property constants supply an explicit sensor name.
   */
  enum TehutiMetricName implements TehutiMetricNameEnum {
    // Property-backed metrics — sensor name derived from the explicit OTel entity reference
    NUM_IMMUTABLE_MEM_TABLE(RocksDBMemoryOtelMetricEntity.NUM_IMMUTABLE_MEM_TABLE),
    MEM_TABLE_FLUSH_PENDING(RocksDBMemoryOtelMetricEntity.MEM_TABLE_FLUSH_PENDING),
    COMPACTION_PENDING(RocksDBMemoryOtelMetricEntity.COMPACTION_PENDING),
    BACKGROUND_ERRORS(RocksDBMemoryOtelMetricEntity.BACKGROUND_ERRORS),
    CUR_SIZE_ACTIVE_MEM_TABLE(RocksDBMemoryOtelMetricEntity.CUR_SIZE_ACTIVE_MEM_TABLE),
    CUR_SIZE_ALL_MEM_TABLES(RocksDBMemoryOtelMetricEntity.CUR_SIZE_ALL_MEM_TABLES),
    SIZE_ALL_MEM_TABLES(RocksDBMemoryOtelMetricEntity.SIZE_ALL_MEM_TABLES),
    NUM_ENTRIES_ACTIVE_MEM_TABLE(RocksDBMemoryOtelMetricEntity.NUM_ENTRIES_ACTIVE_MEM_TABLE),
    NUM_ENTRIES_IMMUTABLE_MEM_TABLES(RocksDBMemoryOtelMetricEntity.NUM_ENTRIES_IMMUTABLE_MEM_TABLES),
    NUM_DELETES_ACTIVE_MEM_TABLE(RocksDBMemoryOtelMetricEntity.NUM_DELETES_ACTIVE_MEM_TABLE),
    NUM_DELETES_IMMUTABLE_MEM_TABLES(RocksDBMemoryOtelMetricEntity.NUM_DELETES_IMMUTABLE_MEM_TABLES),
    ESTIMATE_NUM_KEYS(RocksDBMemoryOtelMetricEntity.ESTIMATE_NUM_KEYS),
    ESTIMATE_TABLE_READERS_MEM(RocksDBMemoryOtelMetricEntity.ESTIMATE_TABLE_READERS_MEM),
    NUM_SNAPSHOTS(RocksDBMemoryOtelMetricEntity.NUM_SNAPSHOTS),
    NUM_LIVE_VERSIONS(RocksDBMemoryOtelMetricEntity.NUM_LIVE_VERSIONS),
    ESTIMATE_LIVE_DATA_SIZE(RocksDBMemoryOtelMetricEntity.ESTIMATE_LIVE_DATA_SIZE),
    MIN_LOG_NUMBER_TO_KEEP(RocksDBMemoryOtelMetricEntity.MIN_LOG_NUMBER_TO_KEEP),
    TOTAL_SST_FILES_SIZE(RocksDBMemoryOtelMetricEntity.TOTAL_SST_FILES_SIZE),
    LIVE_SST_FILES_SIZE(RocksDBMemoryOtelMetricEntity.LIVE_SST_FILES_SIZE),
    ESTIMATE_PENDING_COMPACTION_BYTES(RocksDBMemoryOtelMetricEntity.ESTIMATE_PENDING_COMPACTION_BYTES),
    NUM_RUNNING_COMPACTIONS(RocksDBMemoryOtelMetricEntity.NUM_RUNNING_COMPACTIONS),
    NUM_RUNNING_FLUSHES(RocksDBMemoryOtelMetricEntity.NUM_RUNNING_FLUSHES),
    ACTUAL_DELAYED_WRITE_RATE(RocksDBMemoryOtelMetricEntity.ACTUAL_DELAYED_WRITE_RATE),
    BLOCK_CACHE_CAPACITY(RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_CAPACITY),
    BLOCK_CACHE_PINNED_USAGE(RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_PINNED_USAGE),
    BLOCK_CACHE_USAGE(RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_USAGE),
    NUM_BLOB_FILES(RocksDBMemoryOtelMetricEntity.NUM_BLOB_FILES),
    TOTAL_BLOB_FILE_SIZE(RocksDBMemoryOtelMetricEntity.TOTAL_BLOB_FILE_SIZE),
    LIVE_BLOB_FILE_SIZE(RocksDBMemoryOtelMetricEntity.LIVE_BLOB_FILE_SIZE),
    LIVE_BLOB_FILE_GARBAGE_SIZE(RocksDBMemoryOtelMetricEntity.LIVE_BLOB_FILE_GARBAGE_SIZE),

    // Non-property metrics — explicit sensor name (these don't map to a RocksDB property)
    RMD_BLOCK_CACHE_CAPACITY("rocksdb.rmd-block-cache-capacity"),
    RMD_BLOCK_CACHE_USAGE("rocksdb.rmd-block-cache-usage"),
    RMD_BLOCK_CACHE_PINNED_USAGE("rocksdb.rmd-block-cache-pinned-usage");

    private static final Map<String, TehutiMetricName> SENSOR_LOOKUP;

    static {
      Map<String, TehutiMetricName> map = new HashMap<>();
      for (TehutiMetricName value: values()) {
        map.put(value.sensorName, value);
      }
      SENSOR_LOOKUP = Collections.unmodifiableMap(map);
    }

    private final String sensorName;

    /** Derives sensor name from the matching {@link RocksDBMemoryOtelMetricEntity}'s rocksDBProperty. */
    TehutiMetricName(RocksDBMemoryOtelMetricEntity otelEntity) {
      this.sensorName = otelEntity.getRocksDBProperty();
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
      TehutiMetricName result = SENSOR_LOOKUP.get(sensorName);
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

  /**
   * Volatile reference cleared by {@link #closeRMDBlockCache()} before the native Cache is freed.
   * Callbacks read this reference into a local variable — if null, the cache is closed and they
   * return 0. The local variable pins the reference on the stack, eliminating any TOCTOU race
   * between the null check and the JNI call without requiring locks.
   */
  private volatile Cache rmdCache;

  public void setRMDBlockCache(Cache rmdCache, long rmdCacheCapacity) {
    if (!rmdBlockCacheRegistered.compareAndSet(false, true)) {
      LOGGER.warn("setRMDBlockCache called more than once; ignoring duplicate registration");
      return;
    }
    this.rmdCache = rmdCache;
    registerAsyncGauge(
        RocksDBMemoryOtelMetricEntity.RMD_BLOCK_CACHE_CAPACITY,
        TehutiMetricName.RMD_BLOCK_CACHE_CAPACITY,
        () -> rmdCacheCapacity);
    registerAsyncGauge(
        RocksDBMemoryOtelMetricEntity.RMD_BLOCK_CACHE_USAGE,
        TehutiMetricName.RMD_BLOCK_CACHE_USAGE,
        this::getRMDCacheUsage);
    registerAsyncGauge(
        RocksDBMemoryOtelMetricEntity.RMD_BLOCK_CACHE_PINNED_USAGE,
        TehutiMetricName.RMD_BLOCK_CACHE_PINNED_USAGE,
        this::getRMDCachePinnedUsage);
  }

  /**
   * Must be called before closing the native Cache object to prevent use-after-free.
   * Nulls out the volatile reference so in-flight and future callbacks return 0.
   */
  public void closeRMDBlockCache() {
    this.rmdCache = null;
  }

  private long getRMDCacheUsage() {
    Cache cache = this.rmdCache;
    return cache != null ? cache.getUsage() : 0L;
  }

  private long getRMDCachePinnedUsage() {
    Cache cache = this.rmdCache;
    return cache != null ? cache.getPinnedUsage() : 0L;
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
}
