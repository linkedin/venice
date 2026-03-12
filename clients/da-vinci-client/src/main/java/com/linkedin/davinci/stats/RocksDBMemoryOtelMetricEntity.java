package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * OTel metric entities for RocksDB memory consumption stats.
 *
 * <p>Maps 1:1 from Tehuti async gauges in {@link RocksDBMemoryStats} to OTel ASYNC_GAUGE metrics.
 *
 * <p>Constants that correspond to a RocksDB property (queried via
 * {@link com.linkedin.davinci.store.rocksdb.RocksDBStoragePartition#getRocksDBStatValue})
 * carry the original hyphenated property name (e.g. {@code "rocksdb.num-immutable-mem-table"})
 * and are discoverable via {@link #fromRocksDBProperty(String)}.
 */
public enum RocksDBMemoryOtelMetricEntity implements ModuleMetricEntityInterface {
  NUM_IMMUTABLE_MEM_TABLE(
      "rocksdb.num_immutable_mem_table", MetricUnit.NUMBER, "Number of immutable memtables",
      "rocksdb.num-immutable-mem-table"
  ),
  MEM_TABLE_FLUSH_PENDING(
      "rocksdb.mem_table_flush_pending", MetricUnit.NUMBER, "Number of pending memtable flushes",
      "rocksdb.mem-table-flush-pending"
  ),
  COMPACTION_PENDING(
      "rocksdb.compaction_pending", MetricUnit.NUMBER, "Number of pending compactions", "rocksdb.compaction-pending"
  ),
  BACKGROUND_ERRORS(
      "rocksdb.background_errors", MetricUnit.NUMBER, "Number of background errors", "rocksdb.background-errors"
  ),
  CUR_SIZE_ACTIVE_MEM_TABLE(
      "rocksdb.cur_size_active_mem_table", MetricUnit.BYTES, "Current size of active memtables",
      "rocksdb.cur-size-active-mem-table"
  ),
  CUR_SIZE_ALL_MEM_TABLES(
      "rocksdb.cur_size_all_mem_tables", MetricUnit.BYTES, "Current size of all memtables",
      "rocksdb.cur-size-all-mem-tables"
  ),
  SIZE_ALL_MEM_TABLES(
      "rocksdb.size_all_mem_tables", MetricUnit.BYTES, "Total size of all memtables", "rocksdb.size-all-mem-tables"
  ),
  NUM_ENTRIES_ACTIVE_MEM_TABLE(
      "rocksdb.num_entries_active_mem_table", MetricUnit.NUMBER, "Number of entries in active memtables",
      "rocksdb.num-entries-active-mem-table"
  ),
  NUM_ENTRIES_IMM_MEM_TABLES(
      "rocksdb.num_entries_imm_mem_tables", MetricUnit.NUMBER, "Number of entries in immutable memtables",
      "rocksdb.num-entries-imm-mem-tables"
  ),
  NUM_DELETES_ACTIVE_MEM_TABLE(
      "rocksdb.num_deletes_active_mem_table", MetricUnit.NUMBER, "Number of deletes in active memtables",
      "rocksdb.num-deletes-active-mem-table"
  ),
  NUM_DELETES_IMM_MEM_TABLES(
      "rocksdb.num_deletes_imm_mem_tables", MetricUnit.NUMBER, "Number of deletes in immutable memtables",
      "rocksdb.num-deletes-imm-mem-tables"
  ),
  ESTIMATE_NUM_KEYS(
      "rocksdb.estimate_num_keys", MetricUnit.NUMBER, "Estimated number of keys", "rocksdb.estimate-num-keys"
  ),
  ESTIMATE_TABLE_READERS_MEM(
      "rocksdb.estimate_table_readers_mem", MetricUnit.BYTES, "Estimated memory used by table readers",
      "rocksdb.estimate-table-readers-mem"
  ), NUM_SNAPSHOTS("rocksdb.num_snapshots", MetricUnit.NUMBER, "Number of snapshots", "rocksdb.num-snapshots"),
  NUM_LIVE_VERSIONS(
      "rocksdb.num_live_versions", MetricUnit.NUMBER, "Number of live versions", "rocksdb.num-live-versions"
  ),
  ESTIMATE_LIVE_DATA_SIZE(
      "rocksdb.estimate_live_data_size", MetricUnit.BYTES, "Estimated live data size", "rocksdb.estimate-live-data-size"
  ),
  MIN_LOG_NUMBER_TO_KEEP(
      "rocksdb.min_log_number_to_keep", MetricUnit.NUMBER, "Minimum log number to keep",
      "rocksdb.min-log-number-to-keep"
  ),
  TOTAL_SST_FILES_SIZE(
      "rocksdb.total_sst_files_size", MetricUnit.BYTES, "Total SST file size", "rocksdb.total-sst-files-size"
  ),
  LIVE_SST_FILES_SIZE(
      "rocksdb.live_sst_files_size", MetricUnit.BYTES, "Live SST file size", "rocksdb.live-sst-files-size"
  ),
  ESTIMATE_PENDING_COMPACTION_BYTES(
      "rocksdb.estimate_pending_compaction_bytes", MetricUnit.BYTES, "Estimated pending compaction bytes",
      "rocksdb.estimate-pending-compaction-bytes"
  ),
  NUM_RUNNING_COMPACTIONS(
      "rocksdb.num_running_compactions", MetricUnit.NUMBER, "Number of running compactions",
      "rocksdb.num-running-compactions"
  ),
  NUM_RUNNING_FLUSHES(
      "rocksdb.num_running_flushes", MetricUnit.NUMBER, "Number of running flushes", "rocksdb.num-running-flushes"
  ),
  ACTUAL_DELAYED_WRITE_RATE(
      "rocksdb.actual_delayed_write_rate", MetricUnit.NUMBER, "Actual delayed write rate",
      "rocksdb.actual-delayed-write-rate"
  ),

  // Block cache properties
  BLOCK_CACHE_CAPACITY(
      "rocksdb.block_cache_capacity", MetricUnit.BYTES, "Block cache capacity", "rocksdb.block-cache-capacity"
  ),
  BLOCK_CACHE_PINNED_USAGE(
      "rocksdb.block_cache_pinned_usage", MetricUnit.BYTES, "Block cache pinned usage",
      "rocksdb.block-cache-pinned-usage"
  ), BLOCK_CACHE_USAGE("rocksdb.block_cache_usage", MetricUnit.BYTES, "Block cache usage", "rocksdb.block-cache-usage"),

  // Blob file metrics
  NUM_BLOB_FILES("rocksdb.num_blob_files", MetricUnit.NUMBER, "Number of blob files", "rocksdb.num-blob-files"),
  TOTAL_BLOB_FILE_SIZE(
      "rocksdb.total_blob_file_size", MetricUnit.BYTES, "Total blob file size", "rocksdb.total-blob-file-size"
  ),
  LIVE_BLOB_FILE_SIZE(
      "rocksdb.live_blob_file_size", MetricUnit.BYTES, "Live blob file size", "rocksdb.live-blob-file-size"
  ),
  LIVE_BLOB_FILE_GARBAGE_SIZE(
      "rocksdb.live_blob_file_garbage_size", MetricUnit.BYTES, "Live blob file garbage size",
      "rocksdb.live-blob-file-garbage-size"
  ),

  // Server-level memory metrics (not backed by a RocksDB property)
  MEMORY_LIMIT("rocksdb.memory_limit", MetricUnit.BYTES, "RocksDB memory limit for this server"),
  MEMORY_USAGE("rocksdb.memory_usage", MetricUnit.BYTES, "RocksDB SST file manager total size"),

  // RMD (Replication Metadata) block cache metrics (not backed by a RocksDB property)
  RMD_BLOCK_CACHE_CAPACITY("rocksdb.rmd_block_cache_capacity", MetricUnit.BYTES, "RMD block cache capacity"),
  RMD_BLOCK_CACHE_USAGE("rocksdb.rmd_block_cache_usage", MetricUnit.BYTES, "RMD block cache usage"),
  RMD_BLOCK_CACHE_PINNED_USAGE(
      "rocksdb.rmd_block_cache_pinned_usage", MetricUnit.BYTES, "RMD block cache pinned usage"
  );

  /**
   * Lookup map from RocksDB property name (hyphenated, e.g. "rocksdb.num-immutable-mem-table")
   * to enum constant. Auto-built from constants that carry a non-null {@link #rocksDBProperty}.
   */
  private static final Map<String, RocksDBMemoryOtelMetricEntity> PROPERTY_LOOKUP;

  static {
    Map<String, RocksDBMemoryOtelMetricEntity> map = new HashMap<>();
    for (RocksDBMemoryOtelMetricEntity entity: values()) {
      if (entity.rocksDBProperty != null) {
        map.put(entity.rocksDBProperty, entity);
      }
    }
    PROPERTY_LOOKUP = Collections.unmodifiableMap(map);
  }

  private final MetricEntity metricEntity;

  /** Original RocksDB property name (hyphenated), or {@code null} for synthetic metrics. */
  private final String rocksDBProperty;

  /** Returns the original RocksDB property name (hyphenated), or {@code null} for synthetic metrics. */
  public String getRocksDBProperty() {
    return rocksDBProperty;
  }

  /** Constructor for metrics not backed by a RocksDB property (MEMORY_LIMIT, MEMORY_USAGE, RMD_*). */
  RocksDBMemoryOtelMetricEntity(String metricName, MetricUnit unit, String description) {
    this(metricName, unit, description, null);
  }

  /** Constructor for metrics backed by a RocksDB property. */
  RocksDBMemoryOtelMetricEntity(String metricName, MetricUnit unit, String description, String rocksDBProperty) {
    this.rocksDBProperty = rocksDBProperty;
    this.metricEntity =
        new MetricEntity(metricName, MetricType.ASYNC_GAUGE, unit, description, setOf(VENICE_CLUSTER_NAME));
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }

  /**
   * Returns the OTel metric entity for a given RocksDB property name (e.g., "rocksdb.num-immutable-mem-table").
   *
   * @throws IllegalArgumentException if the property name is not recognized
   */
  public static RocksDBMemoryOtelMetricEntity fromRocksDBProperty(String propertyName) {
    RocksDBMemoryOtelMetricEntity entity = PROPERTY_LOOKUP.get(propertyName);
    if (entity == null) {
      throw new IllegalArgumentException("Unknown RocksDB property: " + propertyName);
    }
    return entity;
  }
}
