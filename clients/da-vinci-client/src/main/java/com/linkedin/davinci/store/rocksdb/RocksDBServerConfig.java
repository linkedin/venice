package com.linkedin.davinci.store.rocksdb;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;


public class RocksDBServerConfig {
  private static final Logger LOGGER = LogManager.getLogger(RocksDBServerConfig.class);
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
   * Shared block cache used by RMD column family across all the RocksDB databases.
   */
  public static final String ROCKSDB_RMD_BLOCK_CACHE_SIZE_IN_BYTES = "rocksdb.rmd.block.cache.size.in.bytes";
  /**
   * Shared block cache for compressed data.
   */
  public static final String ROCKSDB_BLOCK_CACHE_COMPRESSED_SIZE_IN_BYTES =
      "rocksdb.block.cache.compressed.size.in.bytes";

  /**
   * number of bits to count cache shards, total shard count would be 2 to the power of this number.
   */
  public static final String ROCKSDB_BLOCK_CACHE_SHARD_BITS = "rocksdb.block.cache.shard.bits";

  /**
   * the implementation of block cache that the venice server should use.  For supported implementations @see {@link RocksDBBlockCacheImplementations}
   * Defaults to LRU
   */
  public static final String ROCKSDB_BLOCK_CACHE_IMPLEMENTATION = "rocksdb.block.cache.implementation";

  /**
   * if set to True, Cache size will strictly stay within set bounds, by
   * allocating space for indexes and metadata within cache size.
   * This needs to be set to true to make OHC behavior and memory sizing predictable.
   */
  public static final String ROCKSDB_BLOCK_CACHE_STRICT_CAPACITY_LIMIT = "rocksdb.block.cache.strict.capacity.limit";

  /**
   * If set to true, we will put index/filter blocks to the block cache. Otherwise, each "table reader" object will
   * pre-load index/filter block during table initialization.
   */
  public static final String ROCKSDB_SET_CACHE_INDEX_AND_FILTER_BLOCKS = "rocksdb.set.cache.index.and.filter.blocks";

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

  public static final String ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE = "rocksdb.min.write.buffer.number.to.merge";

  /**
   * Max total WAL log size per database;
   */
  public static final String ROCKSDB_MAX_TOTAL_WAL_SIZE_IN_BYTES = "rocksdb.max.total.wal.size.in.bytes";

  /**
   * Max size of level base, and by default the next level size will be 10 times bigger;
   */
  public static final String ROCKSDB_MAX_BYTES_FOR_LEVEL_BASE = "rocksdb.max.bytes.for.level.base";

  public static final String ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED = "rocksdb.plain.table.format.enabled";

  /**
   * Length of the capped prefix extractor used with RocksDB Plain Table. Be cautious when tweaking this config because
   * it might prevent reading old data written with a different extractor length. Therefore, changing it requires wiping
   * the server data and offset.
   */
  public static final String CAPPED_PREFIX_EXTRACTOR_LENGTH = "rocksdb.capped.prefix.extractor.length";

  public static final String ROCKSDB_STORE_INDEX_IN_FILE = "rocksdb.store.index.in.file";
  public static final String ROCKSDB_HUGE_PAGE_TLB_SIZE = "rocksdb.huge.page.tlb.size";
  public static final String ROCKSDB_BLOOM_BITS_PER_KEY = "rocksdb.bloom.bits.per.key";
  public static final String ROCKSDB_HASH_TABLE_RATIO = "rocksdb.hash.table.ratio";
  public static final String ROCKSDB_MAX_OPEN_FILES = "rocksdb.max.open.files";

  /**
   * Target file size, and this will only apply to hybrid store since batch-only push from VPJ is using SSTFileWriter directly.
   */
  public static final String ROCKSDB_TARGET_FILE_SIZE_IN_BYTES = "rocksdb.target.file.size.in.bytes";

  /**
   * Page size for huge page for the arena used by the memtable in rocksdb. If <=0, it
   * won't allocate from huge page but from malloc.
   * Users are responsible to reserve huge pages for it to be allocated. For
   * example:
   *       sysctl -w vm.nr_hugepages=20
   * See linux doc Documentation/vm/hugetlbpage.txt
   * If there isn't enough free huge page available, rocksdb will fall back to
   * malloc.
   */
  public static final String ROCKSDB_MEM_TABLE_HUGE_PAGE_SIZE_BYTES = "rocksdb.mem.table.huge.page.size.in.bytes";

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

  /**
   * Currently, the max file open thread cnt per open operation is 16 by default.
   * We could tune this param to reduce the possible maximum thread cnt;
   */
  public static final String ROCKSDB_MAX_FILE_OPENING_THREADS = "rocksdb.max.file.opening.threads";

  /**
   * When the number of level-0 SST files reaches level0_slowdown_writes_trigger, writes are stalled.
   * When the number of level-0 SST files reaches level0_stop_writes_trigger,
   * writes are fully stopped to wait for level-0 to level-1 compaction reduce the number of level-0 files.
   */
  public static final String ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER = "rocksdb.level0.file.num.compaction.trigger";
  public static final String ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER = "rocksdb.level0.slowdown.writes.trigger";
  public static final String ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER = "rocksdb.level0.stops.writes.trigger";

  public static final String ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_WRITE_ONLY_VERSION =
      "rocksdb.level0.file.num.compaction.trigger.write.only.version";
  public static final String ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_WRITE_ONLY_VERSION =
      "rocksdb.level0.slowdown.writes.trigger.write.only.version";
  public static final String ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_WRITE_ONLY_VERSION =
      "rocksdb.level0.stops.writes.trigger.write.only.version";

  public static final String ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_FOR_READ_WRITE_LEADER =
      "rocksdb.level0.file.num.compaction.trigger.for.read.write.leader";
  public static final String ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_FOR_READ_WRITE_LEADER =
      "rocksdb.level0.slowdown.writes.trigger.for.read.write.leader";
  public static final String ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_FOR_READ_WRITE_LEADER =
      "rocksdb.level0.stops.writes.trigger.for.read.write.leader";
  public static final String ROCKSDB_LEVEL0_COMPACTION_TUNING_FOR_READ_WRITE_LEADER_ENABLED =
      "rocksdb.level0.compaction.tuning.for.read.write.leader.enabled";

  public static final String ROCKSDB_PUT_REUSE_BYTE_BUFFER = "rocksdb.put.reuse.byte.buffer";

  /**
   * Every time, when RocksDB tries to open a database, it will spin up multiple threads to load the file metadata
   * in parallel, and the application could hit the thread limit issue if there are many RocksDB open operations
   * at the same time.
   * The following config is used to throttle the RocksDB open operations.
   */
  public static final String ROCKSDB_DB_OPEN_OPERATION_THROTTLE = "rocksdb.db.open.operation.throttle";

  /**
   * Check the following link for more details:
   * https://github.com/facebook/rocksdb/wiki/Rate-Limiter
   * This is used to throttle flush and compaction.
   */
  public static final String ROCKSDB_WRITE_QUOTA_BYTES_PER_SECOND = "rocksdb.write.quota.bytes.per.second";
  public static final String ROCKSDB_AUTO_TUNED_RATE_LIMITER_ENABLED = "rocksdb.auto.tuned.rate.limited.enabled";
  public static final String ROCKSDB_ATOMIC_FLUSH_ENABLED = "rocksdb.atomic.flush.enabled";
  public static final String ROCKSDB_SEPARATE_RMD_CACHE_ENABLED = "rocksdb.separate.rmd.cache.enabled";
  public static final String ROCKSDB_BLOCK_BASE_FORMAT_VERSION = "rocksdb.block.base.format.version";

  public static final String ROCKSDB_MAX_LOG_FILE_NUM = "rocksdb.max.log.file.num";
  public static final String ROCKSDB_MAX_LOG_FILE_SIZE = "rocksdb.max.log.file.size";
  public static final String RECORD_TRANSFORMER_VALUE_SCHEMA = "record.transformer.value.schema";

  /**
   * Check this page to find more details:
   * https://github.com/facebook/rocksdb/wiki/BlobDB
   */
  public static final String ROCKSDB_BLOB_FILES_ENABLED = "rocksdb.blob.files.enabled";
  public static final String ROCKSDB_MIN_BLOB_SIZE_IN_BYTES = "rocksdb.min.blob.size.in.bytes";
  public static final String ROCKSDB_BLOB_FILE_SIZE_IN_BYTES = "rocksdb.blob.file.size.in.bytes";
  public static final String ROCKSDB_BLOB_GARBAGE_COLLECTION_AGE_CUTOFF = "rocksdb.blob.garbage.collection.age.cutoff";
  public static final String ROCKSDB_BLOB_GARBAGE_COLLECTION_FORCE_THRESHOLD =
      "rocksdb.blob.garbage.collection.force.threshold";
  public static final String ROCKSDB_BLOB_FILE_STARTING_LEVEL = "rocksdb.blob.file.starting.level";

  public static final String ROCKSDB_BLOCK_CACHE_MEMORY_LIMIT = "rocksdb.block.cache.memory.limit";

  /**
   * Check these pages to find more details:
   * https://github.com/facebook/rocksdb/wiki/Iterator
   * https://javadoc.io/static/org.rocksdb/rocksdbjni/6.20.3/org/rocksdb/ReadOptions.html
   */

  /**
   * When this config is set to a value > 0, the RocksDB iterator will pre-fetch data asynchronously leading to better
   * iteration performance.
   * From testing, setting this to a value larger than 2MB doesn't result in any performance gain. Hypothetically, if
   * the records are large, setting this to a higher number may see noticeable gains.
   */
  public static final String ROCKSDB_ITERATOR_READ_AHEAD_SIZE_IN_BYTES = "rocksdb.iterator.read.ahead.size.in.bytes";

  private final boolean rocksDBUseDirectReads;

  private final int rocksDBEnvFlushPoolSize;
  private final int rocksDBEnvCompactionPoolSize;

  private CompressionType rocksDBOptionsCompressionType;
  private final CompactionStyle rocksDBOptionsCompactionStyle;

  private final long rocksDBBlockCacheSizeInBytes;
  private final long rocksDBRMDBlockCacheSizeInBytes;
  private final boolean rocksDBBlockCacheStrictCapacityLimit;
  private final boolean rocksDBSetCacheIndexAndFilterBlocks;
  private final int rocksDBBlockCacheShardBits;
  private final RocksDBBlockCacheImplementations rocksDBBlockCacheImplementation;

  private final long rocksDBSSTFileBlockSizeInBytes;

  private final long rocksDBMemtableSizeInBytes;
  private final int rocksDBMaxMemtableCount;

  private final int rocksDBMinWriteBufferNumberToMerge;
  private final long rocksDBMaxTotalWalSizeInBytes;

  private final long rocksDBMaxBytesForLevelBase;

  private final long rocksDBMemTableHugePageSize;

  private final long rocksDBBytesPerSync;

  private final boolean rocksDBStatisticsEnabled;

  private final boolean rocksDBPlainTableFormatEnabled;
  private final boolean rocksDBStoreIndexInFile;
  private final int rocksDBHugePageTlbSize;
  private final int rocksDBBloomBitsPerKey;

  private final long rocksDBTotalMemtableUsageCapInBytes;
  private final int maxOpenFiles;

  private final int targetFileSizeInBytes;

  private final int maxFileOpeningThreads;
  private final int databaseOpenOperationThrottle;
  private final int cappedPrefixExtractorLength;

  private final long writeQuotaBytesPerSecond;
  private final boolean autoTunedRateLimiterEnabled;

  private final int level0FileNumCompactionTrigger;
  private final int level0SlowdownWritesTrigger;
  private final int level0StopWritesTrigger;

  private final int level0FileNumCompactionTriggerWriteOnlyVersion;
  private final int level0SlowdownWritesTriggerWriteOnlyVersion;
  private final int level0StopWritesTriggerWriteOnlyVersion;

  private final int level0FileNumCompactionTriggerForReadWriteLeader;
  private final int level0SlowdownWritesTriggerForReadWriteLeader;
  private final int level0StopWritesTriggerForReadWriteLeader;
  private final boolean level0CompactionTuningForReadWriteLeaderEnabled;

  private final boolean putReuseByteBufferEnabled;
  private final boolean atomicFlushEnabled;
  private final boolean separateRMDCacheEnabled;
  private int blockBaseFormatVersion;
  private final int maxLogFileNum;
  private final long maxLogFileSize;
  private final String transformerValueSchema;

  private final boolean blobFilesEnabled;
  private final long minBlobSizeInBytes;
  private final long blobFileSizeInBytes;
  private final double blobGarbageCollectionAgeCutOff;
  private final double blobGarbageCollectionForceThreshold;
  private final int blobFileStartingLevel;
  private final double rocksdbBlockCacheMemoryLimit;

  private final long iteratorReadAheadSizeInBytes;

  public RocksDBServerConfig(VeniceProperties props) {
    // Do not use Direct IO for reads by default
    this.rocksDBUseDirectReads = props.getBoolean(ROCKSDB_OPTIONS_USE_DIRECT_READS, false);

    this.rocksDBEnvFlushPoolSize = props.getInt(ROCKSDB_ENV_FLUSH_POOL_SIZE, 1);
    this.rocksDBEnvCompactionPoolSize = props.getInt(ROCKSDB_ENV_COMPACTION_POOL_SIZE, 8);

    String compressionType = props.getString(ROCKSDB_OPTIONS_COMPRESSION_TYPE, CompressionType.NO_COMPRESSION.name());
    try {
      this.rocksDBOptionsCompressionType = CompressionType.valueOf(compressionType);
    } catch (IllegalArgumentException e) {
      throw new VeniceException(
          "Invalid compression type: " + compressionType + ", available types: "
              + Arrays.toString(CompressionType.values()));
    }
    String compactionStyle = props.getString(ROCKSDB_OPTIONS_COMPACTION_STYLE, CompactionStyle.LEVEL.name());
    try {
      this.rocksDBOptionsCompactionStyle = CompactionStyle.valueOf(compactionStyle);
    } catch (IllegalArgumentException e) {
      throw new VeniceException(
          "Invalid compaction style: " + compactionStyle + ", available styles: "
              + Arrays.toString(CompactionStyle.values()));
    }

    this.rocksDBBlockCacheSizeInBytes =
        props.getSizeInBytes(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 16 * 1024 * 1024 * 1024L); // 16GB
    this.rocksDBRMDBlockCacheSizeInBytes =
        props.getSizeInBytes(ROCKSDB_RMD_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024 * 1024L); // 2GB

    this.rocksDBBlockCacheImplementation = RocksDBBlockCacheImplementations
        .valueOf(props.getString(ROCKSDB_BLOCK_CACHE_IMPLEMENTATION, RocksDBBlockCacheImplementations.LRU.toString()));
    // settting the following to false, trying to mitigate `Caused by: org.rocksdb.RocksDBException: Insert failed due
    // to LRU cache being full.` exception
    this.rocksDBBlockCacheStrictCapacityLimit = props.getBoolean(ROCKSDB_BLOCK_CACHE_STRICT_CAPACITY_LIMIT, false);
    this.rocksDBSetCacheIndexAndFilterBlocks = props.getBoolean(ROCKSDB_SET_CACHE_INDEX_AND_FILTER_BLOCKS, true);
    this.rocksDBBlockCacheShardBits = props.getInt(ROCKSDB_BLOCK_CACHE_SHARD_BITS, 4); // 16 shards
    // TODO : add and tune high_pri_pool_ratio to make sure most indexes stay in memory.
    // This only works properly if "cache_index_and_filter_blocks_with_high_priority" is implemented in table configs

    this.rocksDBSSTFileBlockSizeInBytes = props.getSizeInBytes(ROCKSDB_SST_FILE_BLOCK_SIZE_IN_BYTES, 16 * 1024L); // 16KB

    this.rocksDBMemtableSizeInBytes = props.getSizeInBytes(ROCKSDB_MEMTABLE_SIZE_IN_BYTES, 32 * 1024 * 1024L); // 32MB
    this.rocksDBMaxMemtableCount = props.getInt(ROCKSDB_MAX_MEMTABLE_COUNT, 2);
    this.rocksDBMinWriteBufferNumberToMerge = props.getInt(ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE, 1);
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
    this.rocksDBMaxTotalWalSizeInBytes = props.getSizeInBytes(ROCKSDB_MAX_TOTAL_WAL_SIZE_IN_BYTES, 0L);

    this.rocksDBMaxBytesForLevelBase = props.getSizeInBytes(ROCKSDB_MAX_BYTES_FOR_LEVEL_BASE, 2 * 1024 * 1024 * 1024L); // 2GB

    this.rocksDBMemTableHugePageSize = props.getSizeInBytes(ROCKSDB_MEM_TABLE_HUGE_PAGE_SIZE_BYTES, 0);

    // https://github.com/facebook/rocksdb/wiki/Set-Up-Options
    this.rocksDBBytesPerSync = props.getSizeInBytes(ROCKSDB_BYTES_PER_SYNC, 1024 * 1024); // 1MB

    // control whether to emit RocksDB metrics or not
    this.rocksDBStatisticsEnabled = props.getBoolean(ROCKSDB_STATISTICS_ENABLED, false);

    // DO NOT ENABLE except for new stores. https://github.com/facebook/rocksdb/wiki/PlainTable-Format
    this.rocksDBPlainTableFormatEnabled = props.getBoolean(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    if (rocksDBPlainTableFormatEnabled && rocksDBUseDirectReads) {
      throw new VeniceException(
          "Invalid configuration combination, " + ROCKSDB_OPTIONS_USE_DIRECT_READS + " must be disabled to enable "
              + ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED);
    }
    this.rocksDBStoreIndexInFile = props.getBoolean(ROCKSDB_STORE_INDEX_IN_FILE, true);
    this.rocksDBHugePageTlbSize = props.getInt(ROCKSDB_HUGE_PAGE_TLB_SIZE, 0);
    this.rocksDBBloomBitsPerKey = props.getInt(ROCKSDB_BLOOM_BITS_PER_KEY, 10);

    this.rocksDBTotalMemtableUsageCapInBytes =
        props.getSizeInBytes(ROCKSDB_TOTAL_MEMTABLE_USAGE_CAP_IN_BYTES, 2 * 1024 * 1024 * 1024L); // 2GB
    this.maxOpenFiles = props.getInt(ROCKSDB_MAX_OPEN_FILES, -1);

    this.targetFileSizeInBytes = props.getInt(ROCKSDB_TARGET_FILE_SIZE_IN_BYTES, 64 * 1024 * 1024); // default: 64MB

    this.maxFileOpeningThreads = props.getInt(ROCKSDB_MAX_FILE_OPENING_THREADS, 16);
    this.databaseOpenOperationThrottle = props.getInt(ROCKSDB_DB_OPEN_OPERATION_THROTTLE, 3);
    this.cappedPrefixExtractorLength = props.getInt(CAPPED_PREFIX_EXTRACTOR_LENGTH, 16);
    this.writeQuotaBytesPerSecond = props.getSizeInBytes(ROCKSDB_WRITE_QUOTA_BYTES_PER_SECOND, 100L * 1024 * 1024); // 100MB
                                                                                                                    // by
                                                                                                                    // default
    this.autoTunedRateLimiterEnabled = props.getBoolean(ROCKSDB_AUTO_TUNED_RATE_LIMITER_ENABLED, false);
    this.level0FileNumCompactionTrigger = props.getInt(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER, 40);
    this.level0SlowdownWritesTrigger = props.getInt(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER, 60);
    this.level0StopWritesTrigger = props.getInt(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER, 80);

    this.level0FileNumCompactionTriggerWriteOnlyVersion =
        props.getInt(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_WRITE_ONLY_VERSION, 100);
    this.level0SlowdownWritesTriggerWriteOnlyVersion =
        props.getInt(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_WRITE_ONLY_VERSION, 120);
    this.level0StopWritesTriggerWriteOnlyVersion =
        props.getInt(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_WRITE_ONLY_VERSION, 160);

    this.level0FileNumCompactionTriggerForReadWriteLeader =
        props.getInt(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER_FOR_READ_WRITE_LEADER, 10);
    this.level0SlowdownWritesTriggerForReadWriteLeader =
        props.getInt(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER_FOR_READ_WRITE_LEADER, 20);
    this.level0StopWritesTriggerForReadWriteLeader =
        props.getInt(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER_FOR_READ_WRITE_LEADER, 40);
    this.level0CompactionTuningForReadWriteLeaderEnabled =
        props.getBoolean(ROCKSDB_LEVEL0_COMPACTION_TUNING_FOR_READ_WRITE_LEADER_ENABLED, false);

    this.putReuseByteBufferEnabled = props.getBoolean(ROCKSDB_PUT_REUSE_BYTE_BUFFER, false);
    this.atomicFlushEnabled = props.getBoolean(ROCKSDB_ATOMIC_FLUSH_ENABLED, true);
    this.separateRMDCacheEnabled = props.getBoolean(ROCKSDB_SEPARATE_RMD_CACHE_ENABLED, false);

    this.blockBaseFormatVersion = props.getInt(ROCKSDB_BLOCK_BASE_FORMAT_VERSION, 2);

    /**
     * The following configs are per store partition.
     */
    this.maxLogFileNum = props.getInt(ROCKSDB_MAX_LOG_FILE_NUM, 3);
    this.maxLogFileSize = props.getSizeInBytes(ROCKSDB_MAX_LOG_FILE_SIZE, 10 * 1024 * 1024); // 10MB;
    this.transformerValueSchema = props.getString(RECORD_TRANSFORMER_VALUE_SCHEMA, "null");

    /**
     *  Check this page to find more details:
     *  https://github.com/facebook/rocksdb/wiki/BlobDB
     */
    this.blobFilesEnabled = props.getBoolean(ROCKSDB_BLOB_FILES_ENABLED, false);
    if (this.blobFilesEnabled) {
      LOGGER.info("RocksDB Blob files feature is enabled");
    }
    this.minBlobSizeInBytes = props.getSizeInBytes(ROCKSDB_MIN_BLOB_SIZE_IN_BYTES, 4 * 1024); // default: 4KB
    this.blobFileSizeInBytes = props.getSizeInBytes(ROCKSDB_BLOB_FILE_SIZE_IN_BYTES, 256 * 1024 * 1024); // default:
                                                                                                         // 256MB
    this.blobGarbageCollectionAgeCutOff = props.getDouble(ROCKSDB_BLOB_GARBAGE_COLLECTION_AGE_CUTOFF, 0.25);
    this.blobGarbageCollectionForceThreshold = props.getDouble(ROCKSDB_BLOB_GARBAGE_COLLECTION_FORCE_THRESHOLD, 0.8);
    this.blobFileStartingLevel = props.getInt(ROCKSDB_BLOB_FILE_STARTING_LEVEL, 0);
    this.rocksdbBlockCacheMemoryLimit = props.getDouble(ROCKSDB_BLOCK_CACHE_MEMORY_LIMIT, 0.8);

    this.iteratorReadAheadSizeInBytes =
        props.getSizeInBytes(ROCKSDB_ITERATOR_READ_AHEAD_SIZE_IN_BYTES, 2 * 1024 * 1024); // default: 2MB
  }

  public int getLevel0FileNumCompactionTriggerWriteOnlyVersion() {
    return level0FileNumCompactionTriggerWriteOnlyVersion;
  }

  public int getLevel0SlowdownWritesTriggerWriteOnlyVersion() {
    return level0SlowdownWritesTriggerWriteOnlyVersion;
  }

  public int getLevel0StopWritesTriggerWriteOnlyVersion() {
    return level0StopWritesTriggerWriteOnlyVersion;
  }

  public int getLevel0FileNumCompactionTrigger() {
    return level0FileNumCompactionTrigger;
  }

  public int getLevel0SlowdownWritesTrigger() {
    return level0SlowdownWritesTrigger;
  }

  public int getLevel0StopWritesTrigger() {
    return level0StopWritesTrigger;
  }

  public int getLevel0FileNumCompactionTriggerForReadWriteLeader() {
    return level0FileNumCompactionTriggerForReadWriteLeader;
  }

  public int getLevel0SlowdownWritesTriggerForReadWriteLeader() {
    return level0SlowdownWritesTriggerForReadWriteLeader;
  }

  public int getLevel0StopWritesTriggerForReadWriteLeader() {
    return level0StopWritesTriggerForReadWriteLeader;
  }

  public boolean isLevel0CompactionTuningForReadWriteLeaderEnabled() {
    return level0CompactionTuningForReadWriteLeaderEnabled;
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

  public long getRocksDBRMDBlockCacheSizeInBytes() {
    return rocksDBRMDBlockCacheSizeInBytes;
  }

  public RocksDBBlockCacheImplementations getRocksDBBlockCacheImplementation() {
    return rocksDBBlockCacheImplementation;
  }

  public boolean getRocksDBBlockCacheStrictCapacityLimit() {
    return rocksDBBlockCacheStrictCapacityLimit;
  }

  public boolean isRocksDBSetCacheIndexAndFilterBlocks() {
    return rocksDBSetCacheIndexAndFilterBlocks;
  }

  public int getRocksDBBlockCacheShardBits() {
    return rocksDBBlockCacheShardBits;
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

  public int getRocksDBMinWriteBufferNumberToMerge() {
    return rocksDBMinWriteBufferNumberToMerge;
  }

  public long getRocksDBMaxTotalWalSizeInBytes() {
    return rocksDBMaxTotalWalSizeInBytes;
  }

  public long getRocksDBMaxBytesForLevelBase() {
    return rocksDBMaxBytesForLevelBase;
  }

  public long getMemTableHugePageSize() {
    return rocksDBMemTableHugePageSize;
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

  public int getMaxFileOpeningThreads() {
    return maxFileOpeningThreads;
  }

  public int getDatabaseOpenOperationThrottle() {
    return databaseOpenOperationThrottle;
  }

  public int getCappedPrefixExtractorLength() {
    return cappedPrefixExtractorLength;
  }

  public long getWriteQuotaBytesPerSecond() {
    return writeQuotaBytesPerSecond;
  }

  public boolean isAutoTunedRateLimiterEnabled() {
    return autoTunedRateLimiterEnabled;
  }

  public boolean isPutReuseByteBufferEnabled() {
    return putReuseByteBufferEnabled;
  }

  public boolean isAtomicFlushEnabled() {
    return atomicFlushEnabled;
  }

  public boolean isUseSeparateRMDCacheEnabled() {
    return separateRMDCacheEnabled;
  }

  public int getBlockBaseFormatVersion() {
    return blockBaseFormatVersion;
  }

  // For test only
  public void setBlockBaseFormatVersion(int version) {
    this.blockBaseFormatVersion = version;
  }

  public void setRocksdbOptionsCompressionType(String compressionType) {
    this.rocksDBOptionsCompressionType = CompressionType.valueOf(compressionType);
  }

  public int getMaxLogFileNum() {
    return maxLogFileNum;
  }

  public long getMaxLogFileSize() {
    return maxLogFileSize;
  }

  public String getTransformerValueSchema() {
    return transformerValueSchema;
  }

  public boolean isBlobFilesEnabled() {
    return blobFilesEnabled;
  }

  public long getMinBlobSizeInBytes() {
    return minBlobSizeInBytes;
  }

  public long getBlobFileSizeInBytes() {
    return blobFileSizeInBytes;
  }

  public double getBlobGarbageCollectionAgeCutOff() {
    return blobGarbageCollectionAgeCutOff;
  }

  public double getBlobGarbageCollectionForceThreshold() {
    return blobGarbageCollectionForceThreshold;
  }

  public int getBlobFileStartingLevel() {
    return blobFileStartingLevel;
  }

  public double getRocksdbBlockCacheMemoryLimit() {
    return rocksdbBlockCacheMemoryLimit;
  }

  public long getIteratorReadAheadSizeInBytes() {
    return iteratorReadAheadSizeInBytes;
  }
}
