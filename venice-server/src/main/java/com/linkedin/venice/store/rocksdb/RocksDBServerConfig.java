package com.linkedin.venice.store.rocksdb;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;

import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;

import java.util.Arrays;


public class RocksDBServerConfig {
  /**
   * Ability to use direct IO for disk reads, might yield better performance on Azure disks.
   * Also makes caching behavior more consistent, by limiting the caching to only RocksDB.
   * This also reduces number of mem copies which might yield improved performance.
   */
  public static final String ROCKSDB_OPTIONS_USE_DIRECT_READS = "rocksdb.options.use.direct.reads";

  /**
   * Thread pool being used by all the RocksDB databases.
   * https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
   *
   * The maximum number of concurrent flush operations. It is usually good enough to set this to 1.
   */
  public static final String ROCKSDB_ENV_FLUSH_POOL_SIZE = "rocksdb.env.flush.pool.size";
  /**
   * Thread pool being used by all the RocksDB databases.
   * https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
   *
   * The maximum number of concurrent background compactions. The default is 1, but to fully utilize your
   * CPU and storage you might want to increase this to the minimum of (the number of cores in the system, the disk throughput divided
   * by the average throughput of one compaction thread).
   */
  public static final String ROCKSDB_ENV_COMPACTION_POOL_SIZE = "rocksdb.env.compaction.pool.size";

  /**
   * Compression type, and please check this enum class to find out all the available options:
   * {@link CompressionType}.
   * For now, the default option is to disable compression, and use Venice native compression support if necessary.
   */
  public static final String ROCKSDB_OPTIONS_COMPRESSION_TYPE = "rocksdb.options.compression.type";
  /**
   * Please check {@link CompactionStyle} to find all the available options.
   */
  public static final String ROCKSDB_OPTIONS_COMPACTION_STYLE = "rocksdb.options.compaction.style";

  /**
   * Shared block cache across all the RocksDB databases.
   */
  public static final String ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES = "rocksdb.block.cache.size.in.bytes";
  /**
   * Shared block cache for compressed data.
   */
  public static final String ROCKSDB_BLOCK_CACHE_COMPRESSED_SIZE_IN_BYTES = "rocksdb.block.cache.compressed.size.in.bytes";

  /**
   * number of bits to count cache shards, total shard count would be 2 to the power of this number.
   */
  public static final String ROCKSDB_BLOCK_CACHE_SHARD_BITS = "rocksdb.block.cache.shard.bits";
  /**
   * if set to True, Cache size will strictly stay within set bounds, by
   * allocating space for indexes and metadata within cache size.
   * This needs to be set to true to make OHC behavior and memory sizing predictable.
   */
  public static final String ROCKSDB_BLOCK_CACHE_STRICT_CAPACITY_LIMIT = "rocksdb.block.cache.strict.capacity.limit";

  /**
   * File block size, and this config has impact to the index size and read performance.
   */
  public static final String ROCKSDB_SST_FILE_BLOCK_SIZE_IN_BYTES = "rocksdb.sst.file.block.size.in.bytes";

  /**
   * Max memtable size per database;
   */
  public static final String ROCKSDB_MEMTABLE_SIZE_IN_BYTES = "rocksdb.memtable.size.in.bytes";
  /**
   * Max memtable count per database;
   */
  public static final String ROCKSDB_MAX_MEMTABLE_COUNT = "rocksdb.max.memtable.count";
  /**
   * Max total WAL log size per database;
   */
  public static final String ROCKSDB_MAX_TOTAL_WAL_SIZE_IN_BYTES = "rocksdb.max.total.wal.size.in.bytes";

  /**
   * Max size of level base, and by default the next level size will be 10 times bigger;
   */
  public static final String ROCKSDB_MAX_BYTES_FOR_LEVEL_BASE = "rocksdb.max.bytes.for.level.base";

  public static final String ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED = "rocksdb.plain.table.format.enabled";
  public static final String ROCKSDB_STORE_INDEX_IN_FILE = "rocksdb.store.index.in.file";
  public static final String ROCKSDB_HUGE_PAGE_TLB_SIZE = "rocksdb.huge.page.tlb.size";
  public static final String ROCKSDB_BLOOM_BITS_PER_KEY = "rocksdb.bloom.bits.per.key";
  public static final String ROCKSDB_HASH_TABLE_RATIO = "rocksdb.hash.table.ratio";
  public static final String ROCKSDB_MAX_OPEN_FILES = "rocksdb.max.open.files";

  /**
   * Target file size, and this will only apply to hybrid store since batch-only push from H2V is using SSTFileWriter directly.
   */
  public static final String ROCKSDB_TARGET_FILE_SIZE_IN_BYTES = "rocksdb.target.file.size.in.bytes";

  /**
   * Comments from rocksdb c++ code:
   *
   * Allows OS to incrementally sync files to disk while they are being
   * written, asynchronously, in the background. This operation can be used
   * to smooth out write I/Os over time. Users shouldn't rely on it for
   * persistency guarantee.
   * Issue one request for every bytes_per_sync written. 0 turns it off.
   * Default: 0
   */
  public static final String ROCKSDB_BYTES_PER_SYNC = "rocksdb.bytes.per.sync";

  /**
   * Whether to enable rocksdb statistics.
   * The reason to make it configurable is that there is about 5%-10% overhead by enabling statistics.
   * https://github.com/facebook/rocksdb/wiki/Statistics
   */
  public static final String ROCKSDB_STATISTICS_ENABLED = "rocksdb.statistics.enabled";

  /**
   * https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager
   *
   * The total memory usage cap of all the memtables for every RocksDB database.
   */
  public static final String ROCKSDB_TOTAL_MEMTABLE_USAGE_CAP_IN_BYTES = "rocksdb.total.memtable.usage.cap.in.bytes";

  private final boolean rocksDBUseDirectReads;

  private final int rocksDBEnvFlushPoolSize;
  private final int rocksDBEnvCompactionPoolSize;

  private final CompressionType rocksDBOptionsCompressionType;
  private final CompactionStyle rocksDBOptionsCompactionStyle;

  private final long rocksDBBlockCacheSizeInBytes;
  private final long rocksDBBlockCacheCompressedSizeInBytes;
  private final boolean rocksDBBlockCacheStrictCapacityLimit;
  private final int rocksDBBlockCacheShardBits;

  private final long rocksDBSSTFileBlockSizeInBytes;

  private final long rocksDBMemtableSizeInBytes;
  private final int rocksDBMaxMemtableCount;
  private final long rocksDBMaxTotalWalSizeInBytes;

  private final long rocksDBMaxBytesForLevelBase;

  private final long rocksDBBytesPerSync;


  private final boolean rocksDBStatisticsEnabled;

  private final boolean rocksDBPlainTableFormatEnabled;
  private final boolean rocksDBStoreIndexInFile;
  private final int rocksDBHugePageTlbSize;
  private final int rocksDBBloomBitsPerKey;

  private final long rocksDBTotalMemtableUsageCapInBytes;
  private final int maxOpenFiles;

  private final int targetFileSizeInBytes;


  public RocksDBServerConfig(VeniceProperties props) {
    // Do not use Direct IO for reads by default
    this.rocksDBUseDirectReads = props.getBoolean(ROCKSDB_OPTIONS_USE_DIRECT_READS,false);

    this.rocksDBEnvFlushPoolSize = props.getInt(ROCKSDB_ENV_FLUSH_POOL_SIZE, 1);
    this.rocksDBEnvCompactionPoolSize = props.getInt(ROCKSDB_ENV_COMPACTION_POOL_SIZE, 8);

    String compressionType = props.getString(ROCKSDB_OPTIONS_COMPRESSION_TYPE, CompressionType.NO_COMPRESSION.name());
    try {
      this.rocksDBOptionsCompressionType = CompressionType.valueOf(compressionType);
    } catch (IllegalArgumentException e) {
      throw new VeniceException("Invalid compression type: " + compressionType + ", available types: " + Arrays.toString(CompressionType.values()));
    }
    String compactionStyle = props.getString(ROCKSDB_OPTIONS_COMPACTION_STYLE, CompactionStyle.LEVEL.name());
    try {
      this.rocksDBOptionsCompactionStyle = CompactionStyle.valueOf(compactionStyle);
    } catch (IllegalArgumentException e) {
      throw new VeniceException("Invalid compaction style: " + compactionStyle + ", available styles: " + Arrays.toString(CompactionStyle.values()));
    }

    this.rocksDBBlockCacheSizeInBytes = props.getSizeInBytes(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 16 * 1024 * 1024 * 1024l); // 16GB
    this.rocksDBBlockCacheCompressedSizeInBytes = props.getSizeInBytes(ROCKSDB_BLOCK_CACHE_COMPRESSED_SIZE_IN_BYTES, 0l); // disable compressed cache

    this.rocksDBBlockCacheStrictCapacityLimit = props.getBoolean(ROCKSDB_BLOCK_CACHE_STRICT_CAPACITY_LIMIT, true); // make sure indexes stay within cache size limits.
    this.rocksDBBlockCacheShardBits = props.getInt(ROCKSDB_BLOCK_CACHE_SHARD_BITS, 4); // 16 shards
    // TODO : add and tune high_pri_pool_ratio to make sure most indexes stay in memory.
    // This only works properly if "cache_index_and_filter_blocks_with_high_priority" is implemented in table configs

    this.rocksDBSSTFileBlockSizeInBytes = props.getSizeInBytes(ROCKSDB_SST_FILE_BLOCK_SIZE_IN_BYTES, 16 * 1024l); // 16KB

    this.rocksDBMemtableSizeInBytes = props.getSizeInBytes(ROCKSDB_MEMTABLE_SIZE_IN_BYTES, 32 * 1024 * 1024l); // 32MB
    this.rocksDBMaxMemtableCount = props.getInt(ROCKSDB_MAX_MEMTABLE_COUNT, 2);
    /**
     * Default: 0 means letting RocksDB to decide the proper WAL size.
     * Here is the related docs in RocksDB C++ lib:
     * // Once write-ahead logs exceed this size, we will start forcing the flush of
     * // column families whose memtables are backed by the oldest live WAL file
     * // (i.e. the ones that are causing all the space amplification). If set to 0
     * // (default), we will dynamically choose the WAL size limit to be
     * // [sum of all write_buffer_size * max_write_buffer_number] * 4
     * // Default: 0
     */
    this.rocksDBMaxTotalWalSizeInBytes = props.getSizeInBytes(ROCKSDB_MAX_TOTAL_WAL_SIZE_IN_BYTES, 0l);

    this.rocksDBMaxBytesForLevelBase = props.getSizeInBytes(ROCKSDB_MAX_BYTES_FOR_LEVEL_BASE, 2 * 1024 * 1024 * 1024l); // 2GB

    // https://github.com/facebook/rocksdb/wiki/Set-Up-Options
    this.rocksDBBytesPerSync = props.getSizeInBytes(ROCKSDB_BYTES_PER_SYNC, 1024 * 1024); // 1MB

    // control whether to emit RocksDB metrics or not
    this.rocksDBStatisticsEnabled = props.getBoolean(ROCKSDB_STATISTICS_ENABLED, false);

    // DO NOT ENABLE except for new stores. https://github.com/facebook/rocksdb/wiki/PlainTable-Format
    this.rocksDBPlainTableFormatEnabled = props.getBoolean(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    this.rocksDBStoreIndexInFile = props.getBoolean(ROCKSDB_STORE_INDEX_IN_FILE, true);
    this.rocksDBHugePageTlbSize = props.getInt(ROCKSDB_HUGE_PAGE_TLB_SIZE, 0);
    this.rocksDBBloomBitsPerKey = props.getInt(ROCKSDB_BLOOM_BITS_PER_KEY, 10);

    this.rocksDBTotalMemtableUsageCapInBytes = props.getSizeInBytes(ROCKSDB_TOTAL_MEMTABLE_USAGE_CAP_IN_BYTES, 2 * 1024 * 1024 * 1024l); // 2GB
    this.maxOpenFiles = props.getInt(ROCKSDB_MAX_OPEN_FILES, -1);

    this.targetFileSizeInBytes = props.getInt(ROCKSDB_TARGET_FILE_SIZE_IN_BYTES, 64 * 1024 * 1024); // default: 64MB
  }

  public boolean getRocksDBUseDirectReads() {
    return rocksDBUseDirectReads;
  }

  public int getRocksDBEnvFlushPoolSize() {
    return rocksDBEnvFlushPoolSize;
  }

  public int getRocksDBEnvCompactionPoolSize() {
    return rocksDBEnvCompactionPoolSize;
  }

  public CompressionType getRocksDBOptionsCompressionType() {
    return rocksDBOptionsCompressionType;
  }

  public CompactionStyle getRocksDBOptionsCompactionStyle() {
    return rocksDBOptionsCompactionStyle;
  }

  public long getRocksDBBlockCacheSizeInBytes() {
    return rocksDBBlockCacheSizeInBytes;
  }

  public boolean getRocksDBBlockCacheStrictCapacityLimit() {
    return rocksDBBlockCacheStrictCapacityLimit;
  }

  public int getRocksDBBlockCacheShardBits() {
    return rocksDBBlockCacheShardBits;
  }

  public long getRocksDBBlockCacheCompressedSizeInBytes() {
    return rocksDBBlockCacheCompressedSizeInBytes;
  }

  public long getRocksDBSSTFileBlockSizeInBytes() {
    return rocksDBSSTFileBlockSizeInBytes;
  }

  public long getRocksDBMemtableSizeInBytes() {
    return rocksDBMemtableSizeInBytes;
  }

  public int getRocksDBMaxMemtableCount() {
    return rocksDBMaxMemtableCount;
  }

  public long getRocksDBMaxTotalWalSizeInBytes() {
    return rocksDBMaxTotalWalSizeInBytes;
  }

  public long getRocksDBMaxBytesForLevelBase() {
    return rocksDBMaxBytesForLevelBase;
  }

  public long getRocksDBBytesPerSync() {
    return rocksDBBytesPerSync;
  }

  public boolean isRocksDBStatisticsEnabled() {
    return rocksDBStatisticsEnabled;
  }

  /**
   *  DO NOT ENABLE! This is still experimental. PlainTable gives ultra low latency
   *  on smaller DB partition (< 2GB), but its not backward compatible so enabling would require
   *  storing the format type into metadata and enable based on the format or some similar approach.
   *  For details about PlainTable https://github.com/facebook/rocksdb/wiki/PlainTable-Format
   */
  public boolean isRocksDBPlainTableFormatEnabled() {
    return rocksDBPlainTableFormatEnabled;
  }

  public boolean isRocksDBStoreIndexInFile() {
    return rocksDBStoreIndexInFile;
  }

  public int getRocksDBHugePageTlbSize() {
    return rocksDBHugePageTlbSize;
  }

  public int getRocksDBBloomBitsPerKey() {
    return rocksDBBloomBitsPerKey;
  }

  public long getRocksDBTotalMemtableUsageCapInBytes() {
    return rocksDBTotalMemtableUsageCapInBytes;
  }

  public int getMaxOpenFiles() {
    return maxOpenFiles;
  }

  public int getTargetFileSizeInBytes() {
    return targetFileSizeInBytes;
  }
}
