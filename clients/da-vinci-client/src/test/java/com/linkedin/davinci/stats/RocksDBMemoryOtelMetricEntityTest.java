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
   * Verifies that the total enum count (33) = partition properties (30) + non-property metrics (3).
   * The 3 non-property metrics are: RMD_BLOCK_CACHE_CAPACITY, RMD_BLOCK_CACHE_USAGE,
   * RMD_BLOCK_CACHE_PINNED_USAGE.
   */
  @Test
  public void testTotalEntityCountMatchesPartitionPlusNonProperty() {
    int nonPropertyCount = 3; // 3 RMD
    assertEquals(
        RocksDBMemoryOtelMetricEntity.values().length,
        RocksDBMemoryStats.PARTITION_METRIC_DOMAINS.size() + nonPropertyCount,
        "Total OTel entities should equal partition domains + non-property metrics");
  }

  private static Map<RocksDBMemoryOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<RocksDBMemoryOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();

    // Memtable metrics
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_IMMUTABLE_MEM_TABLE,
        expect(
            "rocksdb.memtable.immutable.unflushed_count",
            MetricUnit.NUMBER,
            "Number of immutable memtables that have not yet been flushed"));
    map.put(
        RocksDBMemoryOtelMetricEntity.MEM_TABLE_FLUSH_PENDING,
        expect(
            "rocksdb.memtable.immutable.flush_pending",
            MetricUnit.NUMBER,
            "1 if a memtable flush is pending, 0 otherwise"));
    map.put(
        RocksDBMemoryOtelMetricEntity.CUR_SIZE_ACTIVE_MEM_TABLE,
        expect("rocksdb.memtable.active.size", MetricUnit.BYTES, "Approximate size of the active memtable in bytes"));
    map.put(
        RocksDBMemoryOtelMetricEntity.CUR_SIZE_ALL_MEM_TABLES,
        expect(
            "rocksdb.memtable.active_and_unflushed_immutable.size",
            MetricUnit.BYTES,
            "Approximate size of active and unflushed immutable memtables in bytes"));
    map.put(
        RocksDBMemoryOtelMetricEntity.SIZE_ALL_MEM_TABLES,
        expect(
            "rocksdb.memtable.all.size",
            MetricUnit.BYTES,
            "Approximate size of active, unflushed immutable, and pinned immutable memtables in bytes"));
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_ENTRIES_ACTIVE_MEM_TABLE,
        expect(
            "rocksdb.memtable.active.entry_count",
            MetricUnit.NUMBER,
            "Total number of entries in the active memtable"));
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_ENTRIES_IMMUTABLE_MEM_TABLES,
        expect(
            "rocksdb.memtable.immutable.entry_count",
            MetricUnit.NUMBER,
            "Total number of entries in the unflushed immutable memtables"));
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_DELETES_ACTIVE_MEM_TABLE,
        expect(
            "rocksdb.memtable.active.delete_count",
            MetricUnit.NUMBER,
            "Total number of delete entries in the active memtable"));
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_DELETES_IMMUTABLE_MEM_TABLES,
        expect(
            "rocksdb.memtable.immutable.delete_count",
            MetricUnit.NUMBER,
            "Total number of delete entries in the unflushed immutable memtables"));

    // Compaction metrics
    map.put(
        RocksDBMemoryOtelMetricEntity.COMPACTION_PENDING,
        expect(
            "rocksdb.compaction.pending",
            MetricUnit.NUMBER,
            "1 if at least one compaction is pending, 0 otherwise"));
    map.put(
        RocksDBMemoryOtelMetricEntity.ESTIMATE_PENDING_COMPACTION_BYTES,
        expect(
            "rocksdb.compaction.estimated_pending_bytes",
            MetricUnit.BYTES,
            "Estimated total bytes compaction needs to rewrite to get all levels down to target size"));
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_RUNNING_COMPACTIONS,
        expect("rocksdb.compaction.running_count", MetricUnit.NUMBER, "Number of currently running compactions"));

    // Flush metrics
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_RUNNING_FLUSHES,
        expect("rocksdb.flush.running_count", MetricUnit.NUMBER, "Number of currently running flushes"));

    // General DB metrics
    map.put(
        RocksDBMemoryOtelMetricEntity.BACKGROUND_ERRORS,
        expect("rocksdb.background_errors", MetricUnit.NUMBER, "Accumulated number of background errors"));
    map.put(
        RocksDBMemoryOtelMetricEntity.ESTIMATE_NUM_KEYS,
        expect(
            "rocksdb.keys.estimated_count",
            MetricUnit.NUMBER,
            "Estimated number of total keys in the active and unflushed immutable memtables and storage"));
    map.put(
        RocksDBMemoryOtelMetricEntity.ESTIMATE_TABLE_READERS_MEM,
        expect(
            "rocksdb.table_readers.estimated_memory",
            MetricUnit.BYTES,
            "Estimated memory used for reading SST tables, excluding block cache"));
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_SNAPSHOTS,
        expect("rocksdb.snapshots.count", MetricUnit.NUMBER, "Number of unreleased snapshots of the database"));
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_LIVE_VERSIONS,
        expect(
            "rocksdb.versions.live_count",
            MetricUnit.NUMBER,
            "Number of live versions. More live versions often mean more SST files are held from being deleted"));
    map.put(
        RocksDBMemoryOtelMetricEntity.ESTIMATE_LIVE_DATA_SIZE,
        expect(
            "rocksdb.data.estimated_live_size",
            MetricUnit.BYTES,
            "Estimated amount of live data in bytes, including blob file live bytes"));
    map.put(
        RocksDBMemoryOtelMetricEntity.MIN_LOG_NUMBER_TO_KEEP,
        expect(
            "rocksdb.min_log_number_to_keep",
            MetricUnit.NUMBER,
            "Minimum log number of the log files that should be kept"));
    map.put(
        RocksDBMemoryOtelMetricEntity.ACTUAL_DELAYED_WRITE_RATE,
        expect(
            "rocksdb.actual_delayed_write_rate",
            MetricUnit.NUMBER,
            "Current actual delayed write rate in bytes per second. 0 means no delay"));

    // SST file metrics
    map.put(
        RocksDBMemoryOtelMetricEntity.TOTAL_SST_FILES_SIZE,
        expect("rocksdb.sst.total_size", MetricUnit.BYTES, "Total size of all SST files across all versions"));
    map.put(
        RocksDBMemoryOtelMetricEntity.LIVE_SST_FILES_SIZE,
        expect("rocksdb.sst.live_size", MetricUnit.BYTES, "Total size of all SST files in the current version"));

    // Block cache metrics
    map.put(
        RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_CAPACITY,
        expect("rocksdb.block_cache.capacity", MetricUnit.BYTES, "Block cache capacity"));
    map.put(
        RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_USAGE,
        expect("rocksdb.block_cache.usage", MetricUnit.BYTES, "Memory size for the entries residing in block cache"));
    map.put(
        RocksDBMemoryOtelMetricEntity.BLOCK_CACHE_PINNED_USAGE,
        expect(
            "rocksdb.block_cache.pinned_usage",
            MetricUnit.BYTES,
            "Memory size for the entries being pinned in block cache"));

    // Blob file metrics
    map.put(
        RocksDBMemoryOtelMetricEntity.NUM_BLOB_FILES,
        expect("rocksdb.blob.file_count", MetricUnit.NUMBER, "Number of blob files in the current version"));
    map.put(
        RocksDBMemoryOtelMetricEntity.TOTAL_BLOB_FILE_SIZE,
        expect("rocksdb.blob.total_size", MetricUnit.BYTES, "Total size of all blob files across all versions"));
    map.put(
        RocksDBMemoryOtelMetricEntity.LIVE_BLOB_FILE_SIZE,
        expect("rocksdb.blob.live_size", MetricUnit.BYTES, "Total size of all blob files in the current version"));
    map.put(
        RocksDBMemoryOtelMetricEntity.LIVE_BLOB_FILE_GARBAGE_SIZE,
        expect(
            "rocksdb.blob.garbage_size",
            MetricUnit.BYTES,
            "Total amount of garbage in the blob files in the current version"));

    // RMD block cache metrics
    map.put(
        RocksDBMemoryOtelMetricEntity.RMD_BLOCK_CACHE_CAPACITY,
        expect("rocksdb.rmd_block_cache.capacity", MetricUnit.BYTES, "RMD block cache capacity"));
    map.put(
        RocksDBMemoryOtelMetricEntity.RMD_BLOCK_CACHE_USAGE,
        expect("rocksdb.rmd_block_cache.usage", MetricUnit.BYTES, "RMD block cache usage"));
    map.put(
        RocksDBMemoryOtelMetricEntity.RMD_BLOCK_CACHE_PINNED_USAGE,
        expect("rocksdb.rmd_block_cache.pinned_usage", MetricUnit.BYTES, "RMD block cache pinned usage"));

    return map;
  }

  private static MetricEntityExpectation expect(String name, MetricUnit unit, String description) {
    return new MetricEntityExpectation(name, MetricType.ASYNC_GAUGE, unit, description, DIMS);
  }
}
