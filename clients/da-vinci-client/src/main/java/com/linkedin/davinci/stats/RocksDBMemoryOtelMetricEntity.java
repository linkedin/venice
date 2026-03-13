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
 * OTel names use hierarchical dot-separated naming grouped by RocksDB subsystem
 * (memtable, compaction, flush, block_cache, sst, blob, rmd_block_cache).
 *
 * <p>Constants that correspond to a RocksDB property (queried via
 * {@link com.linkedin.davinci.store.rocksdb.RocksDBStoragePartition#getRocksDBStatValue})
 * carry the original hyphenated property name (e.g. {@code "rocksdb.num-immutable-mem-table"})
 * and are discoverable via {@link #fromRocksDBProperty(String)}.
 */
public enum RocksDBMemoryOtelMetricEntity implements ModuleMetricEntityInterface {
  // Memtable metrics
  NUM_IMMUTABLE_MEM_TABLE(
      "rocksdb.memtable.immutable.unflushed_count", MetricUnit.NUMBER,
      "Number of immutable memtables that have not yet been flushed", "rocksdb.num-immutable-mem-table"
  ),
  MEM_TABLE_FLUSH_PENDING(
      "rocksdb.memtable.immutable.flush_pending", MetricUnit.NUMBER, "1 if a memtable flush is pending, 0 otherwise",
      "rocksdb.mem-table-flush-pending"
  ),
  CUR_SIZE_ACTIVE_MEM_TABLE(
      "rocksdb.memtable.active.size", MetricUnit.BYTES, "Approximate size of the active memtable in bytes",
      "rocksdb.cur-size-active-mem-table"
  ),
  CUR_SIZE_ALL_MEM_TABLES(
      "rocksdb.memtable.active_and_unflushed_immutable.size", MetricUnit.BYTES,
      "Approximate size of active and unflushed immutable memtables in bytes", "rocksdb.cur-size-all-mem-tables"
  ),
  SIZE_ALL_MEM_TABLES(
      "rocksdb.memtable.all.size", MetricUnit.BYTES,
      "Approximate size of active, unflushed immutable, and pinned immutable memtables in bytes",
      "rocksdb.size-all-mem-tables"
  ),
  NUM_ENTRIES_ACTIVE_MEM_TABLE(
      "rocksdb.memtable.active.entry_count", MetricUnit.NUMBER, "Total number of entries in the active memtable",
      "rocksdb.num-entries-active-mem-table"
  ),
  NUM_ENTRIES_IMMUTABLE_MEM_TABLES(
      "rocksdb.memtable.immutable.entry_count", MetricUnit.NUMBER,
      "Total number of entries in the unflushed immutable memtables", "rocksdb.num-entries-imm-mem-tables"
  ),
  NUM_DELETES_ACTIVE_MEM_TABLE(
      "rocksdb.memtable.active.delete_count", MetricUnit.NUMBER,
      "Total number of delete entries in the active memtable", "rocksdb.num-deletes-active-mem-table"
  ),
  NUM_DELETES_IMMUTABLE_MEM_TABLES(
      "rocksdb.memtable.immutable.delete_count", MetricUnit.NUMBER,
      "Total number of delete entries in the unflushed immutable memtables", "rocksdb.num-deletes-imm-mem-tables"
  ),

  // Compaction metrics
  COMPACTION_PENDING(
      "rocksdb.compaction.pending", MetricUnit.NUMBER, "1 if at least one compaction is pending, 0 otherwise",
      "rocksdb.compaction-pending"
  ),
  ESTIMATE_PENDING_COMPACTION_BYTES(
      "rocksdb.compaction.estimated_pending_bytes", MetricUnit.BYTES,
      "Estimated total bytes compaction needs to rewrite to get all levels down to target size",
      "rocksdb.estimate-pending-compaction-bytes"
  ),
  NUM_RUNNING_COMPACTIONS(
      "rocksdb.compaction.running_count", MetricUnit.NUMBER, "Number of currently running compactions",
      "rocksdb.num-running-compactions"
  ),

  // Flush metrics
  NUM_RUNNING_FLUSHES(
      "rocksdb.flush.running_count", MetricUnit.NUMBER, "Number of currently running flushes",
      "rocksdb.num-running-flushes"
  ),

  // General DB metrics
  BACKGROUND_ERRORS(
      "rocksdb.background_errors", MetricUnit.NUMBER, "Accumulated number of background errors",
      "rocksdb.background-errors"
  ),
  ESTIMATE_NUM_KEYS(
      "rocksdb.keys.estimated_count", MetricUnit.NUMBER,
      "Estimated number of total keys in the active and unflushed immutable memtables and storage",
      "rocksdb.estimate-num-keys"
  ),
  ESTIMATE_TABLE_READERS_MEM(
      "rocksdb.table_readers.estimated_memory", MetricUnit.BYTES,
      "Estimated memory used for reading SST tables, excluding block cache", "rocksdb.estimate-table-readers-mem"
  ),
  NUM_SNAPSHOTS(
      "rocksdb.snapshots.count", MetricUnit.NUMBER, "Number of unreleased snapshots of the database",
      "rocksdb.num-snapshots"
  ),
  NUM_LIVE_VERSIONS(
      "rocksdb.versions.live_count", MetricUnit.NUMBER,
      "Number of live versions. More live versions often mean more SST files are held from being deleted",
      "rocksdb.num-live-versions"
  ),
  ESTIMATE_LIVE_DATA_SIZE(
      "rocksdb.data.estimated_live_size", MetricUnit.BYTES,
      "Estimated amount of live data in bytes, including blob file live bytes", "rocksdb.estimate-live-data-size"
  ),
  MIN_LOG_NUMBER_TO_KEEP(
      "rocksdb.min_log_number_to_keep", MetricUnit.NUMBER, "Minimum log number of the log files that should be kept",
      "rocksdb.min-log-number-to-keep"
  ),
  ACTUAL_DELAYED_WRITE_RATE(
      "rocksdb.actual_delayed_write_rate", MetricUnit.NUMBER,
      "Current actual delayed write rate in bytes per second. 0 means no delay", "rocksdb.actual-delayed-write-rate"
  ),

  // SST file metrics
  TOTAL_SST_FILES_SIZE(
      "rocksdb.sst.total_size", MetricUnit.BYTES, "Total size of all SST files across all versions",
      "rocksdb.total-sst-files-size"
  ),
  LIVE_SST_FILES_SIZE(
      "rocksdb.sst.live_size", MetricUnit.BYTES, "Total size of all SST files in the current version",
      "rocksdb.live-sst-files-size"
  ),

  // Block cache metrics
  BLOCK_CACHE_CAPACITY(
      "rocksdb.block_cache.capacity", MetricUnit.BYTES, "Block cache capacity", "rocksdb.block-cache-capacity"
  ),
  BLOCK_CACHE_USAGE(
      "rocksdb.block_cache.usage", MetricUnit.BYTES, "Memory size for the entries residing in block cache",
      "rocksdb.block-cache-usage"
  ),
  BLOCK_CACHE_PINNED_USAGE(
      "rocksdb.block_cache.pinned_usage", MetricUnit.BYTES, "Memory size for the entries being pinned in block cache",
      "rocksdb.block-cache-pinned-usage"
  ),

  // Blob file metrics
  NUM_BLOB_FILES(
      "rocksdb.blob.file_count", MetricUnit.NUMBER, "Number of blob files in the current version",
      "rocksdb.num-blob-files"
  ),
  TOTAL_BLOB_FILE_SIZE(
      "rocksdb.blob.total_size", MetricUnit.BYTES, "Total size of all blob files across all versions",
      "rocksdb.total-blob-file-size"
  ),
  LIVE_BLOB_FILE_SIZE(
      "rocksdb.blob.live_size", MetricUnit.BYTES, "Total size of all blob files in the current version",
      "rocksdb.live-blob-file-size"
  ),
  LIVE_BLOB_FILE_GARBAGE_SIZE(
      "rocksdb.blob.garbage_size", MetricUnit.BYTES, "Total amount of garbage in the blob files in the current version",
      "rocksdb.live-blob-file-garbage-size"
  ),

  // RMD (Replication Metadata) block cache metrics (not backed by a RocksDB property)
  RMD_BLOCK_CACHE_CAPACITY("rocksdb.rmd_block_cache.capacity", MetricUnit.BYTES, "RMD block cache capacity"),
  RMD_BLOCK_CACHE_USAGE("rocksdb.rmd_block_cache.usage", MetricUnit.BYTES, "RMD block cache usage"),
  RMD_BLOCK_CACHE_PINNED_USAGE(
      "rocksdb.rmd_block_cache.pinned_usage", MetricUnit.BYTES, "RMD block cache pinned usage"
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

  /** Constructor for metrics not backed by a RocksDB property (RMD_*). */
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
