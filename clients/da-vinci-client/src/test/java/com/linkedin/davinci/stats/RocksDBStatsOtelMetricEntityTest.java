package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ROCKSDB_BLOCK_CACHE_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ROCKSDB_LEVEL;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class RocksDBStatsOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(RocksDBStatsOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<RocksDBStatsOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<RocksDBStatsOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();

    // Block Cache — per-component (COMPONENT dimension)
    map.put(
        RocksDBStatsOtelMetricEntity.BLOCK_CACHE_MISS_COUNT,
        new MetricEntityExpectation(
            "rocksdb.block_cache.miss.count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Block cache misses by component",
            setOf(VENICE_CLUSTER_NAME, VENICE_ROCKSDB_BLOCK_CACHE_COMPONENT)));
    map.put(
        RocksDBStatsOtelMetricEntity.BLOCK_CACHE_HIT_COUNT,
        new MetricEntityExpectation(
            "rocksdb.block_cache.hit.count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Block cache hits by component",
            setOf(VENICE_CLUSTER_NAME, VENICE_ROCKSDB_BLOCK_CACHE_COMPONENT)));
    map.put(
        RocksDBStatsOtelMetricEntity.BLOCK_CACHE_ADD_COUNT,
        new MetricEntityExpectation(
            "rocksdb.block_cache.add.count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Blocks added to cache by component",
            setOf(VENICE_CLUSTER_NAME, VENICE_ROCKSDB_BLOCK_CACHE_COMPONENT)));
    map.put(
        RocksDBStatsOtelMetricEntity.BLOCK_CACHE_BYTES_INSERTED,
        new MetricEntityExpectation(
            "rocksdb.block_cache.bytes.inserted",
            MetricType.ASYNC_GAUGE,
            MetricUnit.BYTES,
            "Bytes inserted into cache by component",
            setOf(VENICE_CLUSTER_NAME, VENICE_ROCKSDB_BLOCK_CACHE_COMPONENT)));

    // Block Cache — overall (no per-component breakdown)
    map.put(
        RocksDBStatsOtelMetricEntity.BLOCK_CACHE_ADD_FAILURE_COUNT,
        new MetricEntityExpectation(
            "rocksdb.block_cache.add.failure.count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Failed attempts to add blocks to cache",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        RocksDBStatsOtelMetricEntity.BLOCK_CACHE_READ_BYTES,
        new MetricEntityExpectation(
            "rocksdb.block_cache.bytes.read",
            MetricType.ASYNC_GAUGE,
            MetricUnit.BYTES,
            "Bytes served from block cache on hits",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        RocksDBStatsOtelMetricEntity.BLOCK_CACHE_WRITE_BYTES,
        new MetricEntityExpectation(
            "rocksdb.block_cache.bytes.written",
            MetricType.ASYNC_GAUGE,
            MetricUnit.BYTES,
            "Bytes written into block cache on misses",
            setOf(VENICE_CLUSTER_NAME)));

    // Bloom Filter, Memtable, Compaction
    map.put(
        RocksDBStatsOtelMetricEntity.BLOOM_FILTER_USEFUL_COUNT,
        new MetricEntityExpectation(
            "rocksdb.bloom_filter.useful_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Bloom filter checks that avoided a disk read",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        RocksDBStatsOtelMetricEntity.MEMTABLE_HIT_COUNT,
        new MetricEntityExpectation(
            "rocksdb.memtable.hit.count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Get requests served from memtable",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        RocksDBStatsOtelMetricEntity.MEMTABLE_MISS_COUNT,
        new MetricEntityExpectation(
            "rocksdb.memtable.miss.count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Get requests not found in memtable",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        RocksDBStatsOtelMetricEntity.COMPACTION_CANCELLED_COUNT,
        new MetricEntityExpectation(
            "rocksdb.compaction.cancelled_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Compactions cancelled",
            setOf(VENICE_CLUSTER_NAME)));

    // Get Hit by Level (SST_LEVEL dimension)
    map.put(
        RocksDBStatsOtelMetricEntity.GET_HIT_COUNT,
        new MetricEntityExpectation(
            "rocksdb.get.hit.count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Get requests served from SST files by level",
            setOf(VENICE_CLUSTER_NAME, VENICE_ROCKSDB_LEVEL)));

    // Computed Ratio
    map.put(
        RocksDBStatsOtelMetricEntity.READ_AMPLIFICATION_FACTOR,
        new MetricEntityExpectation(
            "rocksdb.read_amplification_factor",
            MetricType.ASYNC_DOUBLE_GAUGE,
            MetricUnit.RATIO,
            "Read amplification — ratio of total bytes read to useful bytes. Values > 1.0 indicate amplification",
            setOf(VENICE_CLUSTER_NAME)));

    return map;
  }
}
