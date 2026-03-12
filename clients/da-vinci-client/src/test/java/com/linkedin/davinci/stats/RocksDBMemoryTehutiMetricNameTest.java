package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class RocksDBMemoryTehutiMetricNameTest {
  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(RocksDBMemoryStats.TehutiMetricName.class, expectedMetricNames()).assertAll();
  }

  /** Verifies that fromSensorName throws for an unknown sensor name. */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFromSensorNameThrowsForUnknownName() {
    RocksDBMemoryStats.TehutiMetricName.fromSensorName("nonexistent-sensor");
  }

  private static Map<RocksDBMemoryStats.TehutiMetricName, String> expectedMetricNames() {
    Map<RocksDBMemoryStats.TehutiMetricName, String> map = new HashMap<>();

    // Partition-aggregated metrics
    map.put(RocksDBMemoryStats.TehutiMetricName.NUM_IMMUTABLE_MEM_TABLE, "rocksdb.num-immutable-mem-table");
    map.put(RocksDBMemoryStats.TehutiMetricName.MEM_TABLE_FLUSH_PENDING, "rocksdb.mem-table-flush-pending");
    map.put(RocksDBMemoryStats.TehutiMetricName.COMPACTION_PENDING, "rocksdb.compaction-pending");
    map.put(RocksDBMemoryStats.TehutiMetricName.BACKGROUND_ERRORS, "rocksdb.background-errors");
    map.put(RocksDBMemoryStats.TehutiMetricName.CUR_SIZE_ACTIVE_MEM_TABLE, "rocksdb.cur-size-active-mem-table");
    map.put(RocksDBMemoryStats.TehutiMetricName.CUR_SIZE_ALL_MEM_TABLES, "rocksdb.cur-size-all-mem-tables");
    map.put(RocksDBMemoryStats.TehutiMetricName.SIZE_ALL_MEM_TABLES, "rocksdb.size-all-mem-tables");
    map.put(RocksDBMemoryStats.TehutiMetricName.NUM_ENTRIES_ACTIVE_MEM_TABLE, "rocksdb.num-entries-active-mem-table");
    map.put(RocksDBMemoryStats.TehutiMetricName.NUM_ENTRIES_IMM_MEM_TABLES, "rocksdb.num-entries-imm-mem-tables");
    map.put(RocksDBMemoryStats.TehutiMetricName.NUM_DELETES_ACTIVE_MEM_TABLE, "rocksdb.num-deletes-active-mem-table");
    map.put(RocksDBMemoryStats.TehutiMetricName.NUM_DELETES_IMM_MEM_TABLES, "rocksdb.num-deletes-imm-mem-tables");
    map.put(RocksDBMemoryStats.TehutiMetricName.ESTIMATE_NUM_KEYS, "rocksdb.estimate-num-keys");
    map.put(RocksDBMemoryStats.TehutiMetricName.ESTIMATE_TABLE_READERS_MEM, "rocksdb.estimate-table-readers-mem");
    map.put(RocksDBMemoryStats.TehutiMetricName.NUM_SNAPSHOTS, "rocksdb.num-snapshots");
    map.put(RocksDBMemoryStats.TehutiMetricName.NUM_LIVE_VERSIONS, "rocksdb.num-live-versions");
    map.put(RocksDBMemoryStats.TehutiMetricName.ESTIMATE_LIVE_DATA_SIZE, "rocksdb.estimate-live-data-size");
    map.put(RocksDBMemoryStats.TehutiMetricName.MIN_LOG_NUMBER_TO_KEEP, "rocksdb.min-log-number-to-keep");
    map.put(RocksDBMemoryStats.TehutiMetricName.TOTAL_SST_FILES_SIZE, "rocksdb.total-sst-files-size");
    map.put(RocksDBMemoryStats.TehutiMetricName.LIVE_SST_FILES_SIZE, "rocksdb.live-sst-files-size");
    map.put(
        RocksDBMemoryStats.TehutiMetricName.ESTIMATE_PENDING_COMPACTION_BYTES,
        "rocksdb.estimate-pending-compaction-bytes");
    map.put(RocksDBMemoryStats.TehutiMetricName.NUM_RUNNING_COMPACTIONS, "rocksdb.num-running-compactions");
    map.put(RocksDBMemoryStats.TehutiMetricName.NUM_RUNNING_FLUSHES, "rocksdb.num-running-flushes");
    map.put(RocksDBMemoryStats.TehutiMetricName.ACTUAL_DELAYED_WRITE_RATE, "rocksdb.actual-delayed-write-rate");
    map.put(RocksDBMemoryStats.TehutiMetricName.BLOCK_CACHE_CAPACITY, "rocksdb.block-cache-capacity");
    map.put(RocksDBMemoryStats.TehutiMetricName.BLOCK_CACHE_PINNED_USAGE, "rocksdb.block-cache-pinned-usage");
    map.put(RocksDBMemoryStats.TehutiMetricName.BLOCK_CACHE_USAGE, "rocksdb.block-cache-usage");
    map.put(RocksDBMemoryStats.TehutiMetricName.NUM_BLOB_FILES, "rocksdb.num-blob-files");
    map.put(RocksDBMemoryStats.TehutiMetricName.TOTAL_BLOB_FILE_SIZE, "rocksdb.total-blob-file-size");
    map.put(RocksDBMemoryStats.TehutiMetricName.LIVE_BLOB_FILE_SIZE, "rocksdb.live-blob-file-size");
    map.put(RocksDBMemoryStats.TehutiMetricName.LIVE_BLOB_FILE_GARBAGE_SIZE, "rocksdb.live-blob-file-garbage-size");

    // Server-level memory metrics
    map.put(RocksDBMemoryStats.TehutiMetricName.MEMORY_LIMIT, "memory_limit");
    map.put(RocksDBMemoryStats.TehutiMetricName.MEMORY_USAGE, "memory_usage");

    // RMD block cache metrics
    map.put(RocksDBMemoryStats.TehutiMetricName.RMD_BLOCK_CACHE_CAPACITY, "rocksdb.rmd-block-cache-capacity");
    map.put(RocksDBMemoryStats.TehutiMetricName.RMD_BLOCK_CACHE_USAGE, "rocksdb.rmd-block-cache-usage");
    map.put(RocksDBMemoryStats.TehutiMetricName.RMD_BLOCK_CACHE_PINNED_USAGE, "rocksdb.rmd-block-cache-pinned-usage");

    return map;
  }
}
