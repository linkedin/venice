package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ROCKSDB_BLOCK_CACHE_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ROCKSDB_LEVEL;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/**
 * OTel metric entity definitions for {@link com.linkedin.venice.stats.RocksDBStats}.
 *
 * <p>Per-component block cache metrics (index, filter, data, compression_dict) are consolidated
 * into single metrics with a {@code VENICE_ROCKSDB_BLOCK_CACHE_COMPONENT} dimension. Overall
 * block cache miss/hit/add counters are Tehuti-only — OTel derives totals via
 * {@code sum by (component)}. Overall-only counters without per-component equivalents
 * (add failures, bytes read/write) are emitted as joint Tehuti+OTel.
 *
 * <p>Get-hit-by-level metrics are consolidated into {@link #GET_HIT_COUNT} with a
 * {@code VENICE_ROCKSDB_LEVEL} dimension.
 *
 * <p>Block cache hit ratio is Tehuti-only (derivable from existing OTel gauge metrics).
 *
 * @see com.linkedin.venice.stats.dimensions.VeniceRocksDBBlockCacheComponent
 * @see com.linkedin.venice.stats.dimensions.VeniceRocksDBLevel
 */
public enum RocksDBStatsOtelMetricEntity implements ModuleMetricEntityInterface {
  // Block Cache — per-component (COMPONENT dimension: INDEX/FILTER/DATA/COMPRESSION_DICT)
  BLOCK_CACHE_MISS_COUNT(
      "rocksdb.block_cache.miss.count", MetricUnit.NUMBER, "Block cache misses by component",
      setOf(VENICE_CLUSTER_NAME, VENICE_ROCKSDB_BLOCK_CACHE_COMPONENT)
  ),

  BLOCK_CACHE_HIT_COUNT(
      "rocksdb.block_cache.hit.count", MetricUnit.NUMBER, "Block cache hits by component",
      setOf(VENICE_CLUSTER_NAME, VENICE_ROCKSDB_BLOCK_CACHE_COMPONENT)
  ),

  BLOCK_CACHE_ADD_COUNT(
      "rocksdb.block_cache.add.count", MetricUnit.NUMBER, "Blocks added to cache by component",
      setOf(VENICE_CLUSTER_NAME, VENICE_ROCKSDB_BLOCK_CACHE_COMPONENT)
  ),

  BLOCK_CACHE_BYTES_INSERTED(
      "rocksdb.block_cache.bytes.inserted", MetricUnit.BYTES, "Bytes inserted into cache by component",
      setOf(VENICE_CLUSTER_NAME, VENICE_ROCKSDB_BLOCK_CACHE_COMPONENT)
  ),

  // Block Cache — overall (no per-component breakdown available)
  BLOCK_CACHE_ADD_FAILURE_COUNT(
      "rocksdb.block_cache.add.failure.count", MetricUnit.NUMBER, "Failed attempts to add blocks to cache",
      setOf(VENICE_CLUSTER_NAME)
  ),

  BLOCK_CACHE_READ_BYTES(
      "rocksdb.block_cache.bytes.read", MetricUnit.BYTES, "Bytes served from block cache on hits",
      setOf(VENICE_CLUSTER_NAME)
  ),

  BLOCK_CACHE_WRITE_BYTES(
      "rocksdb.block_cache.bytes.written", MetricUnit.BYTES, "Bytes written into block cache on misses",
      setOf(VENICE_CLUSTER_NAME)
  ),

  // Bloom Filter
  BLOOM_FILTER_USEFUL_COUNT(
      "rocksdb.bloom_filter.useful_count", MetricUnit.NUMBER, "Bloom filter checks that avoided a disk read",
      setOf(VENICE_CLUSTER_NAME)
  ),

  // Memtable
  MEMTABLE_HIT_COUNT(
      "rocksdb.memtable.hit.count", MetricUnit.NUMBER, "Get requests served from memtable", setOf(VENICE_CLUSTER_NAME)
  ),

  MEMTABLE_MISS_COUNT(
      "rocksdb.memtable.miss.count", MetricUnit.NUMBER, "Get requests not found in memtable", setOf(VENICE_CLUSTER_NAME)
  ),

  // Get Hit by Level (LEVEL dimension: LEVEL_0/LEVEL_1/LEVEL_2_AND_UP)
  GET_HIT_COUNT(
      "rocksdb.get.hit.count", MetricUnit.NUMBER, "Get requests served from SST files by level",
      setOf(VENICE_CLUSTER_NAME, VENICE_ROCKSDB_LEVEL)
  ),

  // Compaction
  COMPACTION_CANCELLED_COUNT(
      "rocksdb.compaction.cancelled_count", MetricUnit.NUMBER, "Compactions cancelled", setOf(VENICE_CLUSTER_NAME)
  ),

  // Computed Ratio
  READ_AMPLIFICATION_FACTOR(
      "rocksdb.read_amplification_factor", MetricType.ASYNC_DOUBLE_GAUGE, MetricUnit.RATIO,
      "Read amplification — ratio of total bytes read to useful bytes. Values > 1.0 indicate amplification",
      setOf(VENICE_CLUSTER_NAME)
  );

  private final MetricEntity metricEntity;

  /** Constructor for ASYNC_GAUGE metrics (default type). */
  RocksDBStatsOtelMetricEntity(
      String metricName,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensions) {
    this(metricName, MetricType.ASYNC_GAUGE, unit, description, dimensions);
  }

  /** Constructor with explicit MetricType. */
  RocksDBStatsOtelMetricEntity(
      String metricName,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensions) {
    this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensions);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
