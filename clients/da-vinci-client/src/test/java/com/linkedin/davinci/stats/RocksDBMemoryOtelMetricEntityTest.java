package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;


public class RocksDBMemoryOtelMetricEntityTest {
  private static final Set<VeniceMetricsDimensions> DIMS = Collections.singleton(VENICE_CLUSTER_NAME);

  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(RocksDBMemoryOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  /** Verifies that fromRocksDBProperty throws for an unknown property name. */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFromRocksDBPropertyThrowsForUnknownProperty() {
    RocksDBMemoryOtelMetricEntity.fromRocksDBProperty("rocksdb.nonexistent-property");
  }

  /**
   * Verifies that every property-backed OTel entity has a matching Tehuti name (via fromSensorName).
   * PARTITION_METRIC_DOMAINS is derived from the OTel enum, so the OTel check is implicit.
   */
  @Test
  public void testPartitionMetricDomainsConsistentWithEnums() {
    for (String property: RocksDBMemoryStats.PARTITION_METRIC_DOMAINS) {
      assertNotNull(
          RocksDBMemoryStats.TehutiMetricName.fromSensorName(property),
          "PARTITION_METRIC_DOMAINS entry '" + property + "' has no matching TehutiMetricName");
    }
  }

  /**
   * Guards against PARTITION_METRIC_DOMAINS size drifting from the expected count.
   * If a new RocksDB property is added, this test forces updating both enums.
   */
  @Test
  public void testPartitionMetricDomainsCount() {
    assertEquals(
        RocksDBMemoryStats.PARTITION_METRIC_DOMAINS.size(),
        30,
        "Expected exactly 30 partition metric domains; update both enums if adding new properties");
  }

  /**
   * Verifies that the total enum count (35) = partition properties (30) + non-property metrics (5).
   * The 5 non-property metrics are: MEMORY_LIMIT, MEMORY_USAGE, RMD_BLOCK_CACHE_CAPACITY,
   * RMD_BLOCK_CACHE_USAGE, RMD_BLOCK_CACHE_PINNED_USAGE.
   */
  @Test
  public void testTotalEntityCountMatchesPartitionPlusNonProperty() {
    int nonPropertyCount = 5; // MEMORY_LIMIT, MEMORY_USAGE, 3 RMD
    assertEquals(
        RocksDBMemoryOtelMetricEntity.values().length,
        RocksDBMemoryStats.PARTITION_METRIC_DOMAINS.size() + nonPropertyCount,
        "Total OTel entities should equal partition domains + non-property metrics");
  }

  private static Map<RocksDBMemoryOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<RocksDBMemoryOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();

    // Per-partition RocksDB properties
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_IMMUTABLE_MEM_TABLE,
        expect("rocksdb.num_immutable_mem_table", MetricUnit.NUMBER, "Number of immutable memtables"));
    map.put(
        RocksDBMemoryOtelMetricEntity.MEM_TABLE_FLUSH_PENDING,
        expect("rocksdb.mem_table_flush_pending", MetricUnit.NUMBER, "Number of pending memtable flushes"));
    map.put(
        RocksDBMemoryOtelMetricEntity.COMPACTION_PENDING,
        expect("rocksdb.compaction_pending", MetricUnit.NUMBER, "Number of pending compactions"));
    map.put(
        RocksDBMemoryOtelMetricEntity.BACKGROUND_ERRORS,
        expect("rocksdb.background_errors", MetricUnit.NUMBER, "Number of background errors"));
    map.put(
        RocksDBMemoryOtelMetricEntity.CUR_SIZE_ACTIVE_MEM_TABLE,
        expect("rocksdb.cur_size_active_mem_table", MetricUnit.BYTES, "Current size of active memtables"));
    map.put(
        RocksDBMemoryOtelMetricEntity.CUR_SIZE_ALL_MEM_TABLES,
        expect("rocksdb.cur_size_all_mem_tables", MetricUnit.BYTES, "Current size of all memtables"));
    map.put(
        RocksDBMemoryOtelMetricEntity.SIZE_ALL_MEM_TABLES,
        expect("rocksdb.size_all_mem_tables", MetricUnit.BYTES, "Total size of all memtables"));
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_ENTRIES_ACTIVE_MEM_TABLE,
        expect("rocksdb.num_entries_active_mem_table", MetricUnit.NUMBER, "Number of entries in active memtables"));
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_ENTRIES_IMM_MEM_TABLES,
        expect("rocksdb.num_entries_imm_mem_tables", MetricUnit.NUMBER, "Number of entries in immutable memtables"));
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_DELETES_ACTIVE_MEM_TABLE,
        expect("rocksdb.num_deletes_active_mem_table", MetricUnit.NUMBER, "Number of deletes in active memtables"));
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_DELETES_IMM_MEM_TABLES,
        expect("rocksdb.num_deletes_imm_mem_tables", MetricUnit.NUMBER, "Number of deletes in immutable memtables"));
    map.put(
        RocksDBMemoryOtelMetricEntity.ESTIMATE_NUM_KEYS,
        expect("rocksdb.estimate_num_keys", MetricUnit.NUMBER, "Estimated number of keys"));
    map.put(
        RocksDBMemoryOtelMetricEntity.ESTIMATE_TABLE_READERS_MEM,
        expect("rocksdb.estimate_table_readers_mem", MetricUnit.BYTES, "Estimated memory used by table readers"));
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_SNAPSHOTS,
        expect("rocksdb.num_snapshots", MetricUnit.NUMBER, "Number of snapshots"));
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_LIVE_VERSIONS,
        expect("rocksdb.num_live_versions", MetricUnit.NUMBER, "Number of live versions"));
    map.put(
        RocksDBMemoryOtelMetricEntity.ESTIMATE_LIVE_DATA_SIZE,
        expect("rocksdb.estimate_live_data_size", MetricUnit.BYTES, "Estimated live data size"));
    map.put(
        RocksDBMemoryOtelMetricEntity.MIN_LOG_NUMBER_TO_KEEP,
        expect("rocksdb.min_log_number_to_keep", MetricUnit.NUMBER, "Minimum log number to keep"));
    map.put(
        RocksDBMemoryOtelMetricEntity.TOTAL_SST_FILES_SIZE,
        expect("rocksdb.total_sst_files_size", MetricUnit.BYTES, "Total SST file size"));
    map.put(
        RocksDBMemoryOtelMetricEntity.LIVE_SST_FILES_SIZE,
        expect("rocksdb.live_sst_files_size", MetricUnit.BYTES, "Live SST file size"));
    map.put(
        RocksDBMemoryOtelMetricEntity.ESTIMATE_PENDING_COMPACTION_BYTES,
        expect("rocksdb.estimate_pending_compaction_bytes", MetricUnit.BYTES, "Estimated pending compaction bytes"));
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_RUNNING_COMPACTIONS,
        expect("rocksdb.num_running_compactions", MetricUnit.NUMBER, "Number of running compactions"));
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_RUNNING_FLUSHES,
        expect("rocksdb.num_running_flushes", MetricUnit.NUMBER, "Number of running flushes"));
    map.put(
        RocksDBMemoryOtelMetricEntity.ACTUAL_DELAYED_WRITE_RATE,
        expect("rocksdb.actual_delayed_write_rate", MetricUnit.NUMBER, "Actual delayed write rate"));

    // Block cache properties
    map.put(
        RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_CAPACITY,
        expect("rocksdb.block_cache_capacity", MetricUnit.BYTES, "Block cache capacity"));
    map.put(
        RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_PINNED_USAGE,
        expect("rocksdb.block_cache_pinned_usage", MetricUnit.BYTES, "Block cache pinned usage"));
    map.put(
        RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_USAGE,
        expect("rocksdb.block_cache_usage", MetricUnit.BYTES, "Block cache usage"));

    // Blob file metrics
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_BLOB_FILES,
        expect("rocksdb.num_blob_files", MetricUnit.NUMBER, "Number of blob files"));
    map.put(
        RocksDBMemoryOtelMetricEntity.TOTAL_BLOB_FILE_SIZE,
        expect("rocksdb.total_blob_file_size", MetricUnit.BYTES, "Total blob file size"));
    map.put(
        RocksDBMemoryOtelMetricEntity.LIVE_BLOB_FILE_SIZE,
        expect("rocksdb.live_blob_file_size", MetricUnit.BYTES, "Live blob file size"));
    map.put(
        RocksDBMemoryOtelMetricEntity.LIVE_BLOB_FILE_GARBAGE_SIZE,
        expect("rocksdb.live_blob_file_garbage_size", MetricUnit.BYTES, "Live blob file garbage size"));

    // Server-level memory metrics
    map.put(
        RocksDBMemoryOtelMetricEntity.MEMORY_LIMIT,
        expect("rocksdb.memory_limit", MetricUnit.BYTES, "RocksDB memory limit for this server"));
    map.put(
        RocksDBMemoryOtelMetricEntity.MEMORY_USAGE,
        expect("rocksdb.memory_usage", MetricUnit.BYTES, "RocksDB SST file manager total size"));

    // RMD block cache metrics
    map.put(
        RocksDBMemoryOtelMetricEntity.RMD_BLOCK_CACHE_CAPACITY,
        expect("rocksdb.rmd_block_cache_capacity", MetricUnit.BYTES, "RMD block cache capacity"));
    map.put(
        RocksDBMemoryOtelMetricEntity.RMD_BLOCK_CACHE_USAGE,
        expect("rocksdb.rmd_block_cache_usage", MetricUnit.BYTES, "RMD block cache usage"));
    map.put(
        RocksDBMemoryOtelMetricEntity.RMD_BLOCK_CACHE_PINNED_USAGE,
        expect("rocksdb.rmd_block_cache_pinned_usage", MetricUnit.BYTES, "RMD block cache pinned usage"));

    return map;
  }

  private static MetricEntityExpectation expect(String name, MetricUnit unit, String description) {
    return new MetricEntityExpectation(name, MetricType.ASYNC_GAUGE, unit, description, DIMS);
  }
}
