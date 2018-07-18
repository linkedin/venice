package com.linkedin.venice.store.bdb;

import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.Time;


public class BdbServerConfig {

  public static final String BDB_CACHE_SIZE = "bdb.cache.size";
  public static final String BDB_WRITE_TRANSACTIONS = "bdb.write.transactions";
  public static final String BDB_FLUSH_TRANSACTIONS = "bdb.flush.transactions";
  public static final String BDB_MAX_LOGFILE_SIZE = "bdb.max.logfile.size";
  public static final String BDB_BTREE_FANOUT = "bdb.btree.fanout";
  public static final String BDB_MAX_DELTA = "bdb.max.delta";
  public static final String BDB_BIN_DELTA = "bdb.bin.delta";
  public static final String BDB_CHECKPOINT_INTERVAL_BYTES = "bdb.checkpoint.interval.bytes";
  public static final String BDB_CHECKPOINT_INTERVAL_MS = "bdb.checkpoint.interval.ms";
  public static final String BDB_ONE_ENV_PER_STORE = "bdb.one.env.per.store";
  public static final String BDB_CLEANER_MIN_FILE_UTILIZATION = "bdb.cleaner.min.file.utilization";
  public static final String BDB_CLEANER_MINUTILIZATION = "bdb.cleaner.minUtilization";
  public static final String BDB_CLEANER_THREADS = "bdb.cleaner.threads";
  public static final String BDB_CLEANER_INTERVAL_BYTES = "bdb.cleaner.interval.bytes";
  public static final String BDB_CLEANER_LOOKAHEAD_CACHE_SIZE = "bdb.cleaner.lookahead.cache.size";
  public static final String BDB_LOCK_TIMEOUT_MS = "bdb.lock.timeout.ms";
  public static final String BDB_LOCK_NLOCKTABLES = "bdb.lock.nLockTables";
  public static final String BDB_LOG_FAULT_READ_SIZE = "bdb.log.fault.read.size";
  public static final String BDB_LOG_ITERATOR_READ_SIZE = "bdb.log.iterator.read.size";
  public static final String BDB_FAIR_LATCHES = "bdb.fair.latches";
  public static final String BDB_CHECKPOINTER_HIGH_PRIORITY = "bdb.checkpointer.high.priority";
  public static final String BDB_CLEANER_MAX_BATCH_FILES = "bdb.cleaner.max.batch.files";
  public static final String BDB_LOCK_READ_UNCOMMITTED = "bdb.lock.read_uncommitted";
  public static final String BDB_STATS_CACHE_TTL_MS = "bdb.stats.cache.ttl.ms";
  public static final String BDB_EXPOSE_SPACE_UTILIZATION = "bdb.expose.space.utilization";
  public static final String BDB_MINIMUM_SHARED_CACHE = "bdb.minimum.shared.cache";
  public static final String BDB_CLEANER_LAZY_MIGRATION = "bdb.cleaner.lazy.migration";
  public static final String BDB_CACHE_EVICTLN = "bdb.cache.evictln";
  public static final String BDB_MINIMIZE_SCAN_IMPACT = "bdb.minimize.scan.impact";
  public static final String BDB_PREFIX_KEYS_WITH_PARTITIONID = "bdb.prefix.keys.with.partitionid";
  public static final String BDB_EVICT_BY_LEVEL = "bdb.evict.by.level";
  public static final String BDB_CHECKPOINTER_OFF_BATCH_WRITES = "bdb.checkpointer.off.batch.writes";
  public static final String BDB_CLEANER_FETCH_OBSOLETE_SIZE = "bdb.cleaner.fetch.obsolete.size";
  public static final String BDB_CLEANER_ADJUST_UTILIZATION = "bdb.cleaner.adjust.utilization";
  public static final String BDB_RECOVERY_FORCE_CHECKPOINT = "bdb.recovery.force.checkpoint";
  public static final String BDB_RAW_PROPERTY_STRING = "bdb.raw.property.string";
  public static final String BDB_DATABASE_KEY_PREFIXING = "bdb.database.key.prefixing";
  public static final String BDB_DROPPED_DB_CLEAN_UP_ENABLED = "bdb.dropped.db.clean.up.enabled";

  // bdb config parameters
  private long bdbCacheSize;
  private boolean bdbWriteTransactions;
  private boolean bdbFlushTransactions;
  private long bdbMaxLogFileSize;
  private int bdbBtreeFanout;
  private int bdbMaxDelta;
  private int bdbBinDelta;
  private long bdbCheckpointBytes;
  private long bdbCheckpointMs;
  private boolean bdbOneEnvPerStore;
  private int bdbCleanerMinFileUtilization;
  private int bdbCleanerMinUtilization;
  private int bdbCleanerLookAheadCacheSize;
  private long bdbCleanerBytesInterval;
  private boolean bdbCheckpointerHighPriority;
  private int bdbCleanerMaxBatchFiles;
  private boolean bdbReadUncommitted;
  private int bdbCleanerThreads;
  private long bdbLockTimeoutMs;
  private int bdbLockNLockTables;
  private int bdbLogFaultReadSize;
  private int bdbLogIteratorReadSize;
  private boolean bdbFairLatches;
  private long bdbStatsCacheTtlMs;
  private boolean bdbExposeSpaceUtilization;
  private long bdbMinimumSharedCache;
  private boolean bdbCleanerLazyMigration;
  private boolean bdbCacheModeEvictLN;
  private boolean bdbMinimizeScanImpact;
  private boolean bdbPrefixKeysWithPartitionId;
  private boolean bdbLevelBasedEviction;
  private boolean bdbCheckpointerOffForBatchWrites;
  private boolean bdbCleanerFetchObsoleteSize;
  private boolean bdbCleanerAdjustUtilization;
  private boolean bdbRecoveryForceCheckpoint;
  private boolean bdbDatabaseKeyPrefixing;
  private String bdbRawPropertyString;
  private boolean bdbDroppedDbCleanUpEnabled;

  public BdbServerConfig(VeniceProperties props) {
    this.bdbCacheSize = props.getSizeInBytes(BDB_CACHE_SIZE, 200 * 1024 * 1024);
    this.bdbWriteTransactions = props.getBoolean(BDB_WRITE_TRANSACTIONS, false);
    this.bdbFlushTransactions = props.getBoolean(BDB_FLUSH_TRANSACTIONS, false);
    this.bdbMaxLogFileSize = props.getSizeInBytes(BDB_MAX_LOGFILE_SIZE, 60 * 1024 * 1024);
    this.bdbBtreeFanout = props.getInt(BDB_BTREE_FANOUT, 512);
    this.bdbMaxDelta = props.getInt(BDB_MAX_DELTA, 100);
    this.bdbBinDelta = props.getInt(BDB_BIN_DELTA, 75);
    this.bdbCheckpointBytes = props.getLong(BDB_CHECKPOINT_INTERVAL_BYTES, 200 * 1024 * 1024);
    this.bdbCheckpointMs = props.getLong(BDB_CHECKPOINT_INTERVAL_MS, 30 * Time.MS_PER_SECOND);
    this.bdbOneEnvPerStore = props.getBoolean(BDB_ONE_ENV_PER_STORE, true);
    this.bdbCleanerMinFileUtilization = props.getInt(BDB_CLEANER_MIN_FILE_UTILIZATION, 0);
    // Increase the minimum utilization from 50% to 70% to improve disk utilization
    this.bdbCleanerMinUtilization = props.getInt(BDB_CLEANER_MINUTILIZATION, 70);
    this.bdbCleanerThreads = props.getInt(BDB_CLEANER_THREADS, 1);
    // by default, wake up the cleaner everytime we write log file size *
    // utilization% bytes. So, by default 30MB
    this.bdbCleanerBytesInterval = props.getLong(BDB_CLEANER_INTERVAL_BYTES, 30 * 1024 * 1024);
    this.bdbCleanerLookAheadCacheSize = props.getInt(BDB_CLEANER_LOOKAHEAD_CACHE_SIZE, 8192);
    this.bdbLockTimeoutMs = props.getLong(BDB_LOCK_TIMEOUT_MS, 500);
    this.bdbLockNLockTables = props.getInt(BDB_LOCK_NLOCKTABLES, 7);
    this.bdbLogFaultReadSize = props.getInt(BDB_LOG_FAULT_READ_SIZE, 2048);
    this.bdbLogIteratorReadSize = props.getInt(BDB_LOG_ITERATOR_READ_SIZE, 8192);
    this.bdbFairLatches = props.getBoolean(BDB_FAIR_LATCHES, false);
    this.bdbCheckpointerHighPriority = props.getBoolean(BDB_CHECKPOINTER_HIGH_PRIORITY, false);
    this.bdbCleanerMaxBatchFiles = props.getInt(BDB_CLEANER_MAX_BATCH_FILES, 0);
    this.bdbReadUncommitted = props.getBoolean(BDB_LOCK_READ_UNCOMMITTED, true);
    this.bdbStatsCacheTtlMs = props.getLong(BDB_STATS_CACHE_TTL_MS, 5 * Time.MS_PER_SECOND);
    this.bdbExposeSpaceUtilization = props.getBoolean(BDB_EXPOSE_SPACE_UTILIZATION, true);
    this.bdbMinimumSharedCache = props.getLong(BDB_MINIMUM_SHARED_CACHE, 0);
    this.bdbCleanerLazyMigration = props.getBoolean(BDB_CLEANER_LAZY_MIGRATION, false);
    this.bdbCacheModeEvictLN = props.getBoolean(BDB_CACHE_EVICTLN, true);
    this.bdbMinimizeScanImpact = props.getBoolean(BDB_MINIMIZE_SCAN_IMPACT, true);
    this.bdbPrefixKeysWithPartitionId = props.getBoolean(BDB_PREFIX_KEYS_WITH_PARTITIONID,
      true);
    this.bdbLevelBasedEviction = props.getBoolean(BDB_EVICT_BY_LEVEL, false);
    this.bdbCheckpointerOffForBatchWrites = props.getBoolean(BDB_CHECKPOINTER_OFF_BATCH_WRITES,
      false);
    this.bdbCleanerFetchObsoleteSize = props.getBoolean(BDB_CLEANER_FETCH_OBSOLETE_SIZE, true);
    this.bdbCleanerAdjustUtilization = props.getBoolean(BDB_CLEANER_ADJUST_UTILIZATION, false);
    this.bdbRecoveryForceCheckpoint = props.getBoolean(BDB_RECOVERY_FORCE_CHECKPOINT, false);
    this.bdbDatabaseKeyPrefixing = props.getBoolean(BDB_DATABASE_KEY_PREFIXING, false);
    this.bdbRawPropertyString = props.getString(BDB_RAW_PROPERTY_STRING, () -> null);
    this.bdbDroppedDbCleanUpEnabled = props.getBoolean(BDB_DROPPED_DB_CLEAN_UP_ENABLED, true);
  }

  public long getBdbCacheSize() {
    return bdbCacheSize;
  }

  /**
   * The size of BDB Cache to hold portions of the BTree.
   * <p/>
   * <ul>
   * <li>Property : "bdb.cache.size"</li>
   * <li>Default : 200MB</li>
   * </ul>
   */
  public void setBdbCacheSize(int bdbCacheSize) {
    this.bdbCacheSize = bdbCacheSize;
  }

  public boolean getBdbExposeSpaceUtilization() {
    return bdbExposeSpaceUtilization;
  }

  /**
   * This parameter controls whether we expose space utilization via MBean. If
   * set to false, stat will always return 0;
   * <p/>
   * <ul>
   * <li>Property : "bdb.expose.space.utilization"</li>
   * <li>Default : true</li>
   * </ul>
   */
  public void setBdbExposeSpaceUtilization(boolean bdbExposeSpaceUtilization) {
    this.bdbExposeSpaceUtilization = bdbExposeSpaceUtilization;
  }

  public boolean isBdbFlushTransactionsEnabled() {
    return bdbFlushTransactions;
  }

  /**
   * If true then sync transactions to disk immediately.
   * <p/>
   * <ul>
   * <li>Property : "bdb.flush.transactions"</li>
   * <li>Default : false</li>
   * </ul>
   */
  public void setBdbFlushTransactions(boolean bdbSyncTransactions) {
    this.bdbFlushTransactions = bdbSyncTransactions;
  }

  public String getBdbRawPropertyString() {
    return bdbRawPropertyString;
  }

  /**
   * When supplied with comma separated propkey=propvalue strings, enables
   * admin to arbitrarily set any BDB JE environment property
   * <p/>
   * eg:
   * bdb.raw.property.string=je.cleaner.threads=1,je.cleaner.lazyMigration=
   * true
   * <p/>
   * Since this is applied after the regular BDB parameter in this class, this
   * has the effect of overriding previous configs if they are specified here
   * again.
   * <p/>
   * <ul>
   * <li>Property : "bdb.raw.property.string"</li>
   * <li>Default : null</li>
   * </ul>
   */
  public void setBdbRawPropertyString(String bdbRawPropString) {
    this.bdbRawPropertyString = bdbRawPropString;
  }

  public long getBdbMaxLogFileSize() {
    return this.bdbMaxLogFileSize;
  }

  /**
   * The maximum size of a single .jdb log file in bytes.
   * <p/>
   * <ul>
   * <li>Property : "bdb.max.logfile.size"</li>
   * <li>Default : 60MB</li>
   * </ul>
   */
  public void setBdbMaxLogFileSize(long bdbMaxLogFileSize) {
    this.bdbMaxLogFileSize = bdbMaxLogFileSize;
  }

  public int getBdbCleanerMinFileUtilization() {
    return bdbCleanerMinFileUtilization;
  }

  /**
   * A log file will be cleaned if its utilization percentage is below this
   * value, irrespective of total utilization. In practice, setting this to a
   * value greater than 0, might potentially hurt if the workload generates a
   * cleaning pattern with a heavy skew of utilization distribution amongs the
   * jdb files
   * <p/>
   * <ul>
   * <li>property: "bdb.cleaner.minFileUtilization"</li>
   * <li>default: 0</li>
   * <li>minimum: 0</li>
   * <li>maximum: 50</li>
   * </ul>
   */
  public final void setBdbCleanerMinFileUtilization(int minFileUtilization) {
    if (minFileUtilization < 0 || minFileUtilization > 50)
      throw new IllegalArgumentException("minFileUtilization should be between 0 and 50 (both inclusive)");
    this.bdbCleanerMinFileUtilization = minFileUtilization;
  }

  public boolean getBdbCheckpointerHighPriority() {
    return bdbCheckpointerHighPriority;
  }

  /**
   * If true, the checkpointer uses more resources in order to complete the
   * checkpoint in a shorter time interval.
   * <p/>
   * <ul>
   * <li>property: "bdb.checkpointer.high.priority"</li>
   * <li>default: false</li>
   * </ul>
   */
  public final void setBdbCheckpointerHighPriority(boolean bdbCheckpointerHighPriority) {
    this.bdbCheckpointerHighPriority = bdbCheckpointerHighPriority;
  }

  public int getBdbCleanerMaxBatchFiles() {
    return bdbCleanerMaxBatchFiles;
  }

  /**
   * The maximum number of log files in the cleaner's backlog, or zero if
   * there is no limit
   * <p/>
   * <ul>
   * <li>property: "bdb.cleaner.max.batch.files"</li>
   * <li>default: 0</li>
   * <li>minimum: 0</li>
   * <li>maximum: 100000</li>
   * </ul>
   */
  public final void setBdbCleanerMaxBatchFiles(int bdbCleanerMaxBatchFiles) {
    if (bdbCleanerMaxBatchFiles < 0 || bdbCleanerMaxBatchFiles > 100000)
      throw new IllegalArgumentException("bdbCleanerMaxBatchFiles should be between 0 and 100000 (both inclusive)");
    this.bdbCleanerMaxBatchFiles = bdbCleanerMaxBatchFiles;
  }

  public int getBdbCleanerThreads() {
    return bdbCleanerThreads;
  }

  /**
   * The number of cleaner threads
   * <p/>
   * <ul>
   * <li>property: "bdb.cleaner.threads"</li>
   * <li>default: 1</li>
   * <li>minimum: 1</li>
   * </ul>
   */
  public final void setBdbCleanerThreads(int bdbCleanerThreads) {
    if (bdbCleanerThreads <= 0)
      throw new IllegalArgumentException("bdbCleanerThreads should be greater than 0");
    this.bdbCleanerThreads = bdbCleanerThreads;
  }

  public long getBdbCleanerBytesInterval() {
    return bdbCleanerBytesInterval;
  }

  /**
   * Amount of bytes written before the Cleaner wakes up to check for
   * utilization
   * <p/>
   * <ul>
   * <li>property: "bdb.cleaner.interval.bytes"</li>
   * <li>default: 30MB</li>
   * </ul>
   */
  public final void setCleanerBytesInterval(long bdbCleanerBytesInterval) {
    this.bdbCleanerBytesInterval = bdbCleanerBytesInterval;
  }

  public int getBdbCleanerLookAheadCacheSize() {
    return bdbCleanerLookAheadCacheSize;
  }

  /**
   * Buffer size used by cleaner to fetch BTree nodes during cleaning.
   * <p/>
   * <ul>
   * <li>property: "bdb.cleaner.lookahead.cache.size"</li>
   * <li>default: 8192</li>
   * </ul>
   */
  public final void setBdbCleanerLookAheadCacheSize(int bdbCleanerLookAheadCacheSize) {
    if (bdbCleanerLookAheadCacheSize < 0)
      throw new IllegalArgumentException("bdbCleanerLookAheadCacheSize should be at least 0");
    this.bdbCleanerLookAheadCacheSize = bdbCleanerLookAheadCacheSize;
  }

  public long getBdbLockTimeoutMs() {
    return bdbLockTimeoutMs;
  }

  /**
   * The lock timeout for all transactional and non-transactional operations.
   * Value of zero disables lock timeouts i.e. a deadlock scenario will block
   * forever. High locktimeout combined with a highly concurrent workload,
   * might have adverse impact on latency for all stores
   * <p/>
   * <ul>
   * <li>property: "bdb.lock.timeout.ms"</li>
   * <li>default: 500</li>
   * <li>minimum: 0</li>
   * <li>maximum: 75 * 60 * 1000</li>
   * </ul>
   */
  public final void setBdbLockTimeoutMs(long bdbLockTimeoutMs) {
    if (bdbLockTimeoutMs < 0)
      throw new IllegalArgumentException("bdbLockTimeoutMs should be greater than 0");
    this.bdbLockTimeoutMs = bdbLockTimeoutMs;
  }

  public int getBdbLockNLockTables() {
    return bdbLockNLockTables;
  }

  /**
   * The size of the lock table used by BDB JE
   * <p/>
   * <ul>
   * <li>Property : bdb.lock.nLockTables"</li>
   * <li>Default : 7</li>
   * </ul>
   */
  public void setBdbLockNLockTables(int bdbLockNLockTables) {
    if (bdbLockNLockTables < 1 || bdbLockNLockTables > 32767)
      throw new IllegalArgumentException("bdbLockNLockTables should be greater than 0 and "
        + "less than 32767");
    this.bdbLockNLockTables = bdbLockNLockTables;
  }

  public int getBdbLogFaultReadSize() {
    return bdbLogFaultReadSize;
  }

  /**
   * Buffer for faulting in objects from disk
   * <p/>
   * <ul>
   * <li>Property : "bdb.log.fault.read.size"</li>
   * <li>Default : 2048</li>
   * </ul>
   */
  public void setBdbLogFaultReadSize(int bdbLogFaultReadSize) {
    this.bdbLogFaultReadSize = bdbLogFaultReadSize;
  }

  public int getBdbLogIteratorReadSize() {
    return bdbLogIteratorReadSize;
  }

  /**
   * Buffer size used by BDB JE for reading the log eg: Cleaning.
   * <p/>
   * <ul>
   * <li>Property : "bdb.log.iterator.read.size"</li>
   * <li>Default : 8192</li>
   * </ul>
   */
  public void setBdbLogIteratorReadSize(int bdbLogIteratorReadSize) {
    this.bdbLogIteratorReadSize = bdbLogIteratorReadSize;
  }

  public boolean getBdbFairLatches() {
    return bdbFairLatches;
  }

  /**
   * Controls whether BDB JE should use latches instead of synchronized blocks
   * <p/>
   * <ul>
   * <li>Property : "bdb.fair.latches"</li>
   * <li>Default : false</li>
   * </ul>
   */
  public void setBdbFairLatches(boolean bdbFairLatches) {
    this.bdbFairLatches = bdbFairLatches;
  }

  public boolean getBdbReadUncommitted() {
    return bdbReadUncommitted;
  }

  /**
   * If true, BDB JE get() will not be blocked by put()
   * <p/>
   * <ul>
   * <li>Property : "bdb.lock.read_uncommitted"</li>
   * <li>Default : true</li>
   * </ul>
   */
  public void setBdbReadUncommitted(boolean bdbReadUncommitted) {
    this.bdbReadUncommitted = bdbReadUncommitted;
  }

  public int getBdbCleanerMinUtilization() {
    return bdbCleanerMinUtilization;
  }

  /**
   * The cleaner will keep the total disk space utilization percentage above
   * this value.
   * <p/>
   * <ul>
   * <li>property: "bdb.cleaner.minUtilization"</li>
   * <li>default: 50</li>
   * <li>minimum: 0</li>
   * <li>maximum: 90</li>
   * </ul>
   */
  public final void setBdbCleanerMinUtilization(int minUtilization) {
    if (minUtilization < 0 || minUtilization > 90)
      throw new IllegalArgumentException("minUtilization should be between 0 and 90 (both inclusive)");
    this.bdbCleanerMinUtilization = minUtilization;
  }

  public int getBdbBtreeFanout() {
    return this.bdbBtreeFanout;
  }

  /**
   * The btree node fanout. Given by "". default: 512
   * <p/>
   * <ul>
   * <li>property: "bdb.btree.fanout"</li>
   * <li>default: 512</li>
   * </ul>
   */
  public void setBdbBtreeFanout(int bdbBtreeFanout) {
    this.bdbBtreeFanout = bdbBtreeFanout;
  }

  /**
   * Exposes BDB JE EnvironmentConfig.TREE_MAX_DELTA.
   * <p/>
   * <ul>
   * <li>Property : "bdb.max.delta"</li>
   * <li>Default : 100</li>
   * </ul>
   */
  public void setBdbMaxDelta(int maxDelta) {
    this.bdbMaxDelta = maxDelta;
  }

  public int getBdbMaxDelta() {
    return this.bdbMaxDelta;
  }

  /**
   * Exposes BDB JE EnvironmentConfig.TREE_BIN_DELTA.
   * <p/>
   * <ul>
   * <li>Property : "bdb.bin.delta"</li>
   * <li>Default : 75</li>
   * </ul>
   */
  public void setBdbBinDelta(int binDelta) {
    this.bdbBinDelta = binDelta;
  }

  public int getBdbBinDelta() {
    return this.bdbBinDelta;
  }

  public boolean getBdbCleanerFetchObsoleteSize() {
    return bdbCleanerFetchObsoleteSize;
  }

  /**
   * If true, Cleaner also fetches the old value to determine the size during
   * an update/delete to compute file utilization. Without this, BDB will auto
   * compute utilization based on heuristics.. (which may or may not work,
   * depending on your use case)
   * <p/>
   * <ul>
   * <li>property: "bdb.cleaner.fetch.obsolete.size"</li>
   * <li>default : true</li>
   * </ul>
   */
  public final void setBdbCleanerFetchObsoleteSize(boolean bdbCleanerFetchObsoleteSize) {
    this.bdbCleanerFetchObsoleteSize = bdbCleanerFetchObsoleteSize;
  }

  public boolean getBdbCleanerAdjustUtilization() {
    return bdbCleanerAdjustUtilization;
  }

  /**
   * If true, Cleaner does not perform any predictive adjustment of the
   * internally computed utilization values
   * <p/>
   * <ul>
   * <li>property: "bdb.cleaner.adjust.utilization"</li>
   * <li>default : false</li>
   * </ul>
   */
  public final void setBdbCleanerAdjustUtilization(boolean bdbCleanerAdjustUtilization) {
    this.bdbCleanerAdjustUtilization = bdbCleanerAdjustUtilization;
  }

  public boolean getBdbRecoveryForceCheckpoint() {
    return bdbRecoveryForceCheckpoint;
  }

  /**
   * When this parameter is set to true, the last .jdb file restored from
   * snapshot will not be modified when opening the Environment, and a new
   * .jdb file will be created and become the end-of-log file. If using
   * incremental backup, this parameter must be true.
   * <p/>
   * <ul>
   * <li>property: "bdb.recovery.force.checkpoint"</li>
   * <li>default : false</li>
   * </ul>
   */
  public final void setBdbRecoveryForceCheckpoint(boolean bdbRecoveryForceCheckpoint) {
    this.bdbRecoveryForceCheckpoint = bdbRecoveryForceCheckpoint;
  }

  public boolean getBdbCleanerLazyMigration() {
    return bdbCleanerLazyMigration;
  }

  /**
   * If true, Cleaner offloads some work to application threads, to keep up
   * with the write rate. Side effect is that data is staged on the JVM till
   * it is flushed down by Checkpointer, hence not GC friendly (Will cause
   * promotions). Use if you have lots of spare RAM but running low on
   * threads/IOPS
   * <p/>
   * <ul>
   * <li>property: "bdb.cleaner.lazy.migration"</li>
   * <li>default : false</li>
   * </ul>
   */
  public final void setBdbCleanerLazyMigration(boolean bdbCleanerLazyMigration) {
    this.bdbCleanerLazyMigration = bdbCleanerLazyMigration;
  }

  public boolean getBdbCacheModeEvictLN() {
    return bdbCacheModeEvictLN;
  }

  /**
   * If true, BDB will not cache data in the JVM. This is very Java GC
   * friendly, and brings a lot of predictability in performance, by greatly
   * reducing constant CMS activity
   * <p/>
   * <ul>
   * <li>Property : "bdb.cache.evictln"</li>
   * <li>Default : true</li>
   * </ul>
   */
  public void setBdbCacheModeEvictLN(boolean bdbCacheModeEvictLN) {
    this.bdbCacheModeEvictLN = bdbCacheModeEvictLN;
  }

  public boolean getBdbMinimizeScanImpact() {
    return bdbMinimizeScanImpact;
  }

  /**
   * If true, attempts are made to minimize impact to BDB cache during scan
   * jobs
   * <p/>
   * <ul>
   * <li>Property : "bdb.minimize.scan.impact"</li>
   * <li>Default : true</li>
   * </ul>
   */
  public void setBdbMinimizeScanImpact(boolean bdbMinimizeScanImpact) {
    this.bdbMinimizeScanImpact = bdbMinimizeScanImpact;
  }

  public boolean isBdbWriteTransactionsEnabled() {
    return bdbWriteTransactions;
  }

  /**
   * Controls persistence mode for BDB JE Transaction. By default, we rely on
   * the checkpointer to flush the writes
   * <p/>
   * <ul>
   * <li>Property : "bdb.write.transactions"</li>
   * <li>Default : false</li>
   * </ul>
   */
  public void setBdbWriteTransactions(boolean bdbWriteTransactions) {
    this.bdbWriteTransactions = bdbWriteTransactions;
  }

  /**
   * If true, use separate BDB JE environment per store
   * <p/>
   * <ul>
   * <li>Property : "bdb.one.env.per.store"</li>
   * <li>Default : false</li>
   * </ul>
   */
  public void setBdbOneEnvPerStore(boolean bdbOneEnvPerStore) {
    this.bdbOneEnvPerStore = bdbOneEnvPerStore;
  }

  public boolean isBdbOneEnvPerStore() {
    return bdbOneEnvPerStore;
  }

  public boolean getBdbPrefixKeysWithPartitionId() {
    return bdbPrefixKeysWithPartitionId;
  }

  /**
   * If true, keys will be prefixed by the partition Id on disk. This can
   * dramatically speed up rebalancing, restore operations, at the cost of 2
   * bytes of extra storage per key
   * <p/>
   * <ul>
   * <li>Property : "bdb.prefix.keys.with.partitionid"</li>
   * <li>Default : true</li>
   * </ul>
   */
  public void setBdbPrefixKeysWithPartitionId(boolean bdbPrefixKeysWithPartitionId) {
    this.bdbPrefixKeysWithPartitionId = bdbPrefixKeysWithPartitionId;
  }

  public long getBdbCheckpointBytes() {
    return this.bdbCheckpointBytes;
  }

  /**
   * Checkpointer is woken up and a checkpoint is written once this many bytes
   * have been logged
   * <p/>
   * <ul>
   * <li>Property : "bdb.checkpoint.interval.bytes"</li>
   * <li>Default : 200MB</li>
   * </ul>
   */
  public void setBdbCheckpointBytes(long bdbCheckpointBytes) {
    this.bdbCheckpointBytes = bdbCheckpointBytes;
  }

  public boolean getBdbCheckpointerOffForBatchWrites() {
    return this.bdbCheckpointerOffForBatchWrites;
  }

  /**
   * BDB JE Checkpointer will be turned off during batch writes. This helps
   * save redundant writing of index updates, as we do say large streaming
   * updates
   * <p/>
   * <ul>
   * <li>Property : "bdb.checkpointer.off.batch.writes"</li>
   * <li>Default : false</li>
   * </ul>
   */
  public void setBdbCheckpointerOffForBatchWrites(boolean bdbCheckpointerOffForBulkWrites) {
    this.bdbCheckpointerOffForBatchWrites = bdbCheckpointerOffForBulkWrites;
  }

  public long getBdbCheckpointMs() {
    return this.bdbCheckpointMs;
  }

  /**
   * BDB JE Checkpointer wakes up whenever this time period elapses
   * <p/>
   * <ul>
   * <li>Property : "bdb.checkpoint.interval.ms"</li>
   * <li>Default : 30s or 30000 ms</li>
   * </ul>
   */
  public void setBdbCheckpointMs(long bdbCheckpointMs) {
    this.bdbCheckpointMs = bdbCheckpointMs;
  }

  public long getBdbStatsCacheTtlMs() {
    return this.bdbStatsCacheTtlMs;
  }

  /**
   * Interval to reuse environment stats fetched from BDB. Once the interval
   * expires, a fresh call will be made
   * <p/>
   * <ul>
   * <li>Property : "bdb.stats.cache.ttl.ms"</li>
   * <li>Default : 5s</li>
   * </ul>
   */
  public void setBdbStatsCacheTtlMs(long statsCacheTtlMs) {
    this.bdbStatsCacheTtlMs = statsCacheTtlMs;
  }

  public long getBdbMinimumSharedCache() {
    return this.bdbMinimumSharedCache;
  }

  /**
   * When using partitioned caches, this parameter controls the minimum amount
   * of memory reserved for the global pool. Any memory-footprint reservation
   * that will break this guarantee will fail.
   * <p/>
   * <ul>
   * <li>Property : "bdb.minimum.shared.cache"</li>
   * <li>Default : 0</li>
   * </ul>
   */
  public void setBdbMinimumSharedCache(long minimumSharedCache) {
    this.bdbMinimumSharedCache = minimumSharedCache;
  }

  public boolean isBdbLevelBasedEviction() {
    return bdbLevelBasedEviction;
  }

  /**
   * Controls if BDB JE cache eviction happens based on LRU or by BTree level.
   * <p/>
   * <ul>
   * <li>Property : "bdb.evict.by.level"</li>
   * <li>Default : false</li>
   * </ul>
   */
  public void setBdbLevelBasedEviction(boolean bdbLevelBasedEviction) {
    this.bdbLevelBasedEviction = bdbLevelBasedEviction;
  }

  /**
   * Controls if BDB database enables key-prefixing.
   * With key-prefixing, B+ tree index size could be reduced, but it will introduce some overhead,
   * such as which could cause higher cpu usage, memory pressure, when the common prefix changes
   * inside an IN/BIN during ingestion.
   * @return
   */
  public boolean isBdbDatabaseKeyPrefixing() {
    return bdbDatabaseKeyPrefixing;
  }

  public void setBdbDatabaseKeyPrefixing(boolean bdbDatabaseKeyPrefixing) {
    this.bdbDatabaseKeyPrefixing = bdbDatabaseKeyPrefixing;
  }

  /**
   * Controls if BDB enforces taking checkpoint after dropping a partition;
   * With checkpoint enforcement, log files will be cleaned up after dropping a BDB storage partition.
   * @return
   */
  public boolean isBdbDroppedDbCleanUpEnabled() {
    return bdbDroppedDbCleanUpEnabled;
  }

  public void setBdbDroppedDbCleanUpEnabled(boolean bdbDroppedDbCleanUpEnabled) {
    this.bdbDroppedDbCleanUpEnabled = bdbDroppedDbCleanUpEnabled;
  }
}
