package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOCK_CACHE_ADD_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOCK_CACHE_ADD_FAILURE_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOCK_CACHE_BYTES_INSERTED;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOCK_CACHE_HIT_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOCK_CACHE_MISS_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOCK_CACHE_READ_BYTES;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOCK_CACHE_WRITE_BYTES;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOOM_FILTER_USEFUL_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.COMPACTION_CANCELLED_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.GET_HIT_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.MEMTABLE_HIT_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.MEMTABLE_MISS_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.READ_AMPLIFICATION_FACTOR;
import static org.rocksdb.TickerType.BLOCK_CACHE_ADD;
import static org.rocksdb.TickerType.BLOCK_CACHE_ADD_FAILURES;
import static org.rocksdb.TickerType.BLOCK_CACHE_BYTES_READ;
import static org.rocksdb.TickerType.BLOCK_CACHE_BYTES_WRITE;
import static org.rocksdb.TickerType.BLOCK_CACHE_COMPRESSION_DICT_ADD;
import static org.rocksdb.TickerType.BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT;
import static org.rocksdb.TickerType.BLOCK_CACHE_COMPRESSION_DICT_HIT;
import static org.rocksdb.TickerType.BLOCK_CACHE_COMPRESSION_DICT_MISS;
import static org.rocksdb.TickerType.BLOCK_CACHE_DATA_ADD;
import static org.rocksdb.TickerType.BLOCK_CACHE_DATA_BYTES_INSERT;
import static org.rocksdb.TickerType.BLOCK_CACHE_DATA_HIT;
import static org.rocksdb.TickerType.BLOCK_CACHE_DATA_MISS;
import static org.rocksdb.TickerType.BLOCK_CACHE_FILTER_ADD;
import static org.rocksdb.TickerType.BLOCK_CACHE_FILTER_BYTES_INSERT;
import static org.rocksdb.TickerType.BLOCK_CACHE_FILTER_HIT;
import static org.rocksdb.TickerType.BLOCK_CACHE_FILTER_MISS;
import static org.rocksdb.TickerType.BLOCK_CACHE_HIT;
import static org.rocksdb.TickerType.BLOCK_CACHE_INDEX_ADD;
import static org.rocksdb.TickerType.BLOCK_CACHE_INDEX_BYTES_INSERT;
import static org.rocksdb.TickerType.BLOCK_CACHE_INDEX_HIT;
import static org.rocksdb.TickerType.BLOCK_CACHE_INDEX_MISS;
import static org.rocksdb.TickerType.BLOCK_CACHE_MISS;
import static org.rocksdb.TickerType.BLOOM_FILTER_USEFUL;
import static org.rocksdb.TickerType.COMPACTION_CANCELLED;
import static org.rocksdb.TickerType.GET_HIT_L0;
import static org.rocksdb.TickerType.GET_HIT_L1;
import static org.rocksdb.TickerType.GET_HIT_L2_AND_UP;
import static org.rocksdb.TickerType.MEMTABLE_HIT;
import static org.rocksdb.TickerType.MEMTABLE_MISS;
import static org.rocksdb.TickerType.READ_AMP_ESTIMATE_USEFUL_BYTES;
import static org.rocksdb.TickerType.READ_AMP_TOTAL_READ_BYTES;

import com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceRocksDBBlockCacheComponent;
import com.linkedin.venice.stats.dimensions.VeniceRocksDBLevel;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateOneEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;


/**
 * RocksDB ticker-based statistics. Gated by {@code rocksdb.statistics.enabled} (default false).
 *
 * <p>Per-component block cache metrics use a {@code BLOCK_CACHE_COMPONENT} dimension
 * (INDEX/FILTER/DATA/COMPRESSION_DICT). Overall block cache miss/hit/add counters are
 * Tehuti-only — OTel derives totals via {@code sum by (component)}. Overall-only counters
 * without per-component equivalents (add failures, bytes read/write) are joint Tehuti+OTel.
 *
 * <p>Get-hit-by-level uses a {@code LEVEL} dimension (LEVEL_0/LEVEL_1/LEVEL_2_AND_UP).
 * Block cache hit ratio is Tehuti-only (derivable from per-component OTel gauge metrics).
 *
 * @see RocksDBStatsOtelMetricEntity
 */
public class RocksDBStats extends AbstractVeniceStats {
  private static final EnumMap<VeniceRocksDBLevel, TickerType> GET_HIT_TICKER_BY_LEVEL =
      new EnumMap<>(VeniceRocksDBLevel.class);
  private static final EnumMap<VeniceRocksDBBlockCacheComponent, TickerType> MISS_TICKER_BY_COMPONENT =
      new EnumMap<>(VeniceRocksDBBlockCacheComponent.class);
  private static final EnumMap<VeniceRocksDBBlockCacheComponent, TickerType> HIT_TICKER_BY_COMPONENT =
      new EnumMap<>(VeniceRocksDBBlockCacheComponent.class);
  private static final EnumMap<VeniceRocksDBBlockCacheComponent, TickerType> ADD_TICKER_BY_COMPONENT =
      new EnumMap<>(VeniceRocksDBBlockCacheComponent.class);
  private static final EnumMap<VeniceRocksDBBlockCacheComponent, TickerType> BYTES_INSERT_TICKER_BY_COMPONENT =
      new EnumMap<>(VeniceRocksDBBlockCacheComponent.class);
  static {
    GET_HIT_TICKER_BY_LEVEL.put(VeniceRocksDBLevel.LEVEL_0, GET_HIT_L0);
    GET_HIT_TICKER_BY_LEVEL.put(VeniceRocksDBLevel.LEVEL_1, GET_HIT_L1);
    GET_HIT_TICKER_BY_LEVEL.put(VeniceRocksDBLevel.LEVEL_2_AND_UP, GET_HIT_L2_AND_UP);

    MISS_TICKER_BY_COMPONENT.put(VeniceRocksDBBlockCacheComponent.INDEX, BLOCK_CACHE_INDEX_MISS);
    MISS_TICKER_BY_COMPONENT.put(VeniceRocksDBBlockCacheComponent.FILTER, BLOCK_CACHE_FILTER_MISS);
    MISS_TICKER_BY_COMPONENT.put(VeniceRocksDBBlockCacheComponent.DATA, BLOCK_CACHE_DATA_MISS);
    MISS_TICKER_BY_COMPONENT.put(VeniceRocksDBBlockCacheComponent.COMPRESSION_DICT, BLOCK_CACHE_COMPRESSION_DICT_MISS);

    HIT_TICKER_BY_COMPONENT.put(VeniceRocksDBBlockCacheComponent.INDEX, BLOCK_CACHE_INDEX_HIT);
    HIT_TICKER_BY_COMPONENT.put(VeniceRocksDBBlockCacheComponent.FILTER, BLOCK_CACHE_FILTER_HIT);
    HIT_TICKER_BY_COMPONENT.put(VeniceRocksDBBlockCacheComponent.DATA, BLOCK_CACHE_DATA_HIT);
    HIT_TICKER_BY_COMPONENT.put(VeniceRocksDBBlockCacheComponent.COMPRESSION_DICT, BLOCK_CACHE_COMPRESSION_DICT_HIT);

    ADD_TICKER_BY_COMPONENT.put(VeniceRocksDBBlockCacheComponent.INDEX, BLOCK_CACHE_INDEX_ADD);
    ADD_TICKER_BY_COMPONENT.put(VeniceRocksDBBlockCacheComponent.FILTER, BLOCK_CACHE_FILTER_ADD);
    ADD_TICKER_BY_COMPONENT.put(VeniceRocksDBBlockCacheComponent.DATA, BLOCK_CACHE_DATA_ADD);
    ADD_TICKER_BY_COMPONENT.put(VeniceRocksDBBlockCacheComponent.COMPRESSION_DICT, BLOCK_CACHE_COMPRESSION_DICT_ADD);

    BYTES_INSERT_TICKER_BY_COMPONENT.put(VeniceRocksDBBlockCacheComponent.INDEX, BLOCK_CACHE_INDEX_BYTES_INSERT);
    BYTES_INSERT_TICKER_BY_COMPONENT.put(VeniceRocksDBBlockCacheComponent.FILTER, BLOCK_CACHE_FILTER_BYTES_INSERT);
    BYTES_INSERT_TICKER_BY_COMPONENT.put(VeniceRocksDBBlockCacheComponent.DATA, BLOCK_CACHE_DATA_BYTES_INSERT);
    BYTES_INSERT_TICKER_BY_COMPONENT
        .put(VeniceRocksDBBlockCacheComponent.COMPRESSION_DICT, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT);
  }

  private Statistics rocksDBStat;

  public RocksDBStats(MetricsRepository metricsRepository, String name, String clusterName) {
    super(metricsRepository, name);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .setClusterName(clusterName)
            .isTotalStats(isTotalStats())
            .build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    // --- Block Cache: overall (Tehuti-only, OTel derives from per-component sum) ---
    registerTickerSensor("rocksdb_block_cache_miss", BLOCK_CACHE_MISS);
    registerTickerSensor("rocksdb_block_cache_hit", BLOCK_CACHE_HIT);
    registerTickerSensor("rocksdb_block_cache_add", BLOCK_CACHE_ADD);

    // --- Block Cache: per-component (Tehuti sensors + OTel with COMPONENT dimension) ---
    // Tehuti: 16 individual sensors (4 components × 4 operations)
    registerTickerSensor("rocksdb_block_cache_index_miss", BLOCK_CACHE_INDEX_MISS);
    registerTickerSensor("rocksdb_block_cache_index_hit", BLOCK_CACHE_INDEX_HIT);
    registerTickerSensor("rocksdb_block_cache_index_add", BLOCK_CACHE_INDEX_ADD);
    registerTickerSensor("rocksdb_block_cache_index_bytes_insert", BLOCK_CACHE_INDEX_BYTES_INSERT);
    registerTickerSensor("rocksdb_block_cache_filter_miss", BLOCK_CACHE_FILTER_MISS);
    registerTickerSensor("rocksdb_block_cache_filter_hit", BLOCK_CACHE_FILTER_HIT);
    registerTickerSensor("rocksdb_block_cache_filter_add", BLOCK_CACHE_FILTER_ADD);
    registerTickerSensor("rocksdb_block_cache_filter_bytes_insert", BLOCK_CACHE_FILTER_BYTES_INSERT);
    registerTickerSensor("rocksdb_block_cache_data_miss", BLOCK_CACHE_DATA_MISS);
    registerTickerSensor("rocksdb_block_cache_data_hit", BLOCK_CACHE_DATA_HIT);
    registerTickerSensor("rocksdb_block_cache_data_add", BLOCK_CACHE_DATA_ADD);
    registerTickerSensor("rocksdb_block_cache_data_bytes_insert", BLOCK_CACHE_DATA_BYTES_INSERT);
    registerTickerSensor("rocksdb_block_cache_compression_dict_miss", BLOCK_CACHE_COMPRESSION_DICT_MISS);
    registerTickerSensor("rocksdb_block_cache_compression_dict_hit", BLOCK_CACHE_COMPRESSION_DICT_HIT);
    registerTickerSensor("rocksdb_block_cache_compression_dict_add", BLOCK_CACHE_COMPRESSION_DICT_ADD);
    registerTickerSensor(
        "rocksdb_block_cache_compression_dict_bytes_insert",
        BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT);

    // OTel: 4 metrics with COMPONENT dimension
    registerComponentMetric(otelRepository, baseDimensionsMap, BLOCK_CACHE_MISS_COUNT, MISS_TICKER_BY_COMPONENT);
    registerComponentMetric(otelRepository, baseDimensionsMap, BLOCK_CACHE_HIT_COUNT, HIT_TICKER_BY_COMPONENT);
    registerComponentMetric(otelRepository, baseDimensionsMap, BLOCK_CACHE_ADD_COUNT, ADD_TICKER_BY_COMPONENT);
    registerComponentMetric(
        otelRepository,
        baseDimensionsMap,
        BLOCK_CACHE_BYTES_INSERTED,
        BYTES_INSERT_TICKER_BY_COMPONENT);

    // --- Block Cache: overall-only (joint Tehuti+OTel, no per-component breakdown) ---
    registerJointTickerMetric(
        "rocksdb_block_cache_add_failures",
        BLOCK_CACHE_ADD_FAILURES,
        BLOCK_CACHE_ADD_FAILURE_COUNT,
        otelRepository,
        baseDimensionsMap,
        baseAttributes);
    registerJointTickerMetric(
        "rocksdb_block_cache_bytes_read",
        BLOCK_CACHE_BYTES_READ,
        BLOCK_CACHE_READ_BYTES,
        otelRepository,
        baseDimensionsMap,
        baseAttributes);
    registerJointTickerMetric(
        "rocksdb_block_cache_bytes_write",
        BLOCK_CACHE_BYTES_WRITE,
        BLOCK_CACHE_WRITE_BYTES,
        otelRepository,
        baseDimensionsMap,
        baseAttributes);

    // --- Bloom Filter, Memtable, Compaction (joint Tehuti+OTel) ---
    registerJointTickerMetric(
        "rocksdb_bloom_filter_useful",
        BLOOM_FILTER_USEFUL,
        BLOOM_FILTER_USEFUL_COUNT,
        otelRepository,
        baseDimensionsMap,
        baseAttributes);
    registerJointTickerMetric(
        "rocksdb_memtable_hit",
        MEMTABLE_HIT,
        MEMTABLE_HIT_COUNT,
        otelRepository,
        baseDimensionsMap,
        baseAttributes);
    registerJointTickerMetric(
        "rocksdb_memtable_miss",
        MEMTABLE_MISS,
        MEMTABLE_MISS_COUNT,
        otelRepository,
        baseDimensionsMap,
        baseAttributes);
    registerJointTickerMetric(
        "rocksdb_compaction_cancelled",
        COMPACTION_CANCELLED,
        COMPACTION_CANCELLED_COUNT,
        otelRepository,
        baseDimensionsMap,
        baseAttributes);

    // --- Get Hit by Level: 3 Tehuti sensors + 1 OTel with LEVEL dimension ---
    registerTickerSensor("rocksdb_get_hit_l0", GET_HIT_L0);
    registerTickerSensor("rocksdb_get_hit_l1", GET_HIT_L1);
    registerTickerSensor("rocksdb_get_hit_l2_and_up", GET_HIT_L2_AND_UP);
    AsyncMetricEntityStateOneEnum
        .create(GET_HIT_COUNT.getMetricEntity(), otelRepository, baseDimensionsMap, VeniceRocksDBLevel.class, level -> {
          TickerType ticker = GET_HIT_TICKER_BY_LEVEL.get(level);
          if (ticker == null) {
            throw new IllegalStateException("No TickerType mapping for level: " + level);
          }
          return () -> rocksDBStat != null ? rocksDBStat.getTickerCount(ticker) : -1;
        });

    // --- Block Cache Hit Ratio: Tehuti-only (OTel derivable: hit{data} / (hit{data} + sum(miss))) ---
    registerSensorIfAbsent(new AsyncGauge((ig, ig2) -> {
      if (rocksDBStat != null) {
        long dataHit = rocksDBStat.getTickerCount(BLOCK_CACHE_DATA_HIT);
        long miss = rocksDBStat.getTickerCount(BLOCK_CACHE_MISS);
        long denominator = dataHit + miss;
        return denominator == 0 ? Double.NaN : dataHit / (double) denominator;
      }
      return -1;
    }, "rocksdb_block_cache_hit_ratio"));

    // --- Read Amplification: Tehuti + OTel ASYNC_DOUBLE_GAUGE ---
    // OTel callback returns NaN when unavailable (SDK drops the data point).
    // Tehuti callback returns -1 as the established sentinel for uninitialized state.
    DoubleSupplier readAmpOtelCallback = () -> {
      if (rocksDBStat != null) {
        long total = rocksDBStat.getTickerCount(READ_AMP_TOTAL_READ_BYTES);
        long useful = rocksDBStat.getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES);
        return useful == 0 ? Double.NaN : total / (double) useful;
      }
      return Double.NaN;
    };
    registerSensorIfAbsent(new AsyncGauge((ig, ig2) -> {
      if (rocksDBStat != null) {
        long total = rocksDBStat.getTickerCount(READ_AMP_TOTAL_READ_BYTES);
        long useful = rocksDBStat.getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES);
        return useful == 0 ? -1 : total / (double) useful;
      }
      return -1;
    }, "rocksdb_read_amplification_factor"));
    AsyncMetricEntityStateBase.create(
        READ_AMPLIFICATION_FACTOR.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        baseAttributes,
        readAmpOtelCallback);
  }

  /** Registers a Tehuti-only AsyncGauge for a RocksDB TickerType counter. */
  private void registerTickerSensor(String sensorName, TickerType tickerType) {
    registerSensorIfAbsent(
        new AsyncGauge((ig, ig2) -> rocksDBStat != null ? rocksDBStat.getTickerCount(tickerType) : -1, sensorName));
  }

  /** Registers a joint Tehuti+OTel AsyncGauge for a RocksDB TickerType counter. */
  private void registerJointTickerMetric(
      String tehutiSensorName,
      TickerType tickerType,
      RocksDBStatsOtelMetricEntity otelEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Attributes baseAttributes) {
    LongSupplier callback = () -> rocksDBStat != null ? rocksDBStat.getTickerCount(tickerType) : -1;
    registerSensorIfAbsent(new AsyncGauge((ig, ig2) -> callback.getAsLong(), tehutiSensorName));
    AsyncMetricEntityStateBase
        .create(otelEntity.getMetricEntity(), otelRepository, baseDimensionsMap, baseAttributes, callback);
  }

  /** Registers an OTel-only AsyncMetricEntityStateOneEnum for per-component block cache metrics. */
  private void registerComponentMetric(
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      RocksDBStatsOtelMetricEntity otelEntity,
      EnumMap<VeniceRocksDBBlockCacheComponent, TickerType> componentTickers) {
    AsyncMetricEntityStateOneEnum.create(
        otelEntity.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VeniceRocksDBBlockCacheComponent.class,
        component -> {
          TickerType ticker = componentTickers.get(component);
          if (ticker == null) {
            throw new IllegalStateException("No TickerType mapping for component: " + component);
          }
          return () -> rocksDBStat != null ? rocksDBStat.getTickerCount(ticker) : -1;
        });
  }

  public void setRocksDBStat(Statistics stat) {
    if (stat == null) {
      throw new IllegalArgumentException("'stat' must not be null");
    }
    if (this.rocksDBStat != null) {
      throw new VeniceException("'rocksDBStat' has already been initialized");
    }
    this.rocksDBStat = stat;
  }
}
