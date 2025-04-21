package com.linkedin.davinci.store.rocksdb;

import static com.linkedin.davinci.store.AbstractStorageEngine.METADATA_PARTITION_ID;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.store.AbstractStorageIterator;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.MemoryLimitExhaustedException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ByteBufferGetStatus;
import org.rocksdb.Cache;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.EnvOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.MemoryUsageType;
import org.rocksdb.MemoryUtil;
import org.rocksdb.Options;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.SstFileManager;
import org.rocksdb.SstFileWriter;
import org.rocksdb.Statistics;
import org.rocksdb.Status;
import org.rocksdb.WriteOptions;


/**
 * In {@link RocksDBStoragePartition}, it assumes the update(insert/delete) will happen sequentially.
 * If the batch push is bytewise-sorted by key, this class is leveraging {@link SstFileWriter} to
 * generate the SST file directly and ingest all the generated SST files into the RocksDB database
 * at the end of the push.
 *
 * If the ingestion is unsorted, this class is using the regular RocksDB interface to support update
 * operations.
 */
@NotThreadSafe
public class RocksDBStoragePartition extends AbstractStoragePartition {
  private static final Logger LOGGER = LogManager.getLogger(RocksDBStoragePartition.class);
  private static final String ROCKSDB_ERROR_MESSAGE_FOR_RUNNING_OUT_OF_SPACE_QUOTA = "Max allowed space was reached";
  protected static final ReadOptions READ_OPTIONS_DEFAULT = new ReadOptions();
  static final byte[] REPLICATION_METADATA_COLUMN_FAMILY = "timestamp_metadata".getBytes();

  private static final FlushOptions WAIT_FOR_FLUSH_OPTIONS = new FlushOptions().setWaitForFlush(true);

  /**
   * Here RocksDB disables WAL, but relies on the 'flush', which will be invoked through {@link #sync()}
   * to avoid data loss during recovery.
   */
  protected final WriteOptions writeOptions;
  private final String fullPathForTempSSTFileDir;
  private final String fullPathForPartitionDBSnapshot;

  private final EnvOptions envOptions;

  protected final String replicaId;
  protected final String storeName;
  protected final String storeNameAndVersion;
  protected final boolean blobTransferEnabled;
  protected final int partitionId;
  private final String fullPathForPartitionDB;

  /**
   * If the internal RocksDB handler has been closed or in the middle of closing, any other RocksDB operations
   * will crash.
   * We will use {@link #isClosed} to indicate whether the current RocksDB is closed or not.
   */
  private boolean isClosed = false;
  /**
   * Since all the modification functions are synchronized, we don't need any other synchronization for the update path
   * to guard RocksDB closing behavior.
   * The following {@link #readCloseRWLock} is only used to guard {@link #get} since we don't want to synchronize get requests.
   */
  protected final ReentrantReadWriteLock readCloseRWLock = new ReentrantReadWriteLock();

  /**
   * The passed in {@link Options} instance.
   * For now, the RocksDB version being used right now doesn't support shared block cache unless
   * all the RocksDB databases reuse the same {@link Options} instance, which is not great.
   *
   * Once the latest version: https://github.com/facebook/rocksdb/releases/tag/v5.12.2 is available
   * in maven repo, we could setup a separate {@link Options} for each RocksDB database to specify
   * customized config, such as:
   * 1. Various block size;
   * 2. Read-only for batch-only store;
   *
   */
  private final Options options;
  protected RocksDB rocksDB;
  private final RocksDBServerConfig rocksDBServerConfig;
  private final RocksDBStorageEngineFactory factory;
  private final RocksDBThrottler rocksDBThrottler;
  /**
   * Whether the input is sorted or not. <br>
   * deferredWrite = sortedInput => ingested via batch push which is sorted in VPJ, can use {@link RocksDBSstFileWriter} to ingest
   *                                the input data to RocksDB <br>
   * !deferredWrite = !sortedInput => can not use RocksDBSstFileWriter for ingestion
   */
  protected final boolean deferredWrite;

  /**
   * Whether the database is read only or not.
   */
  protected final boolean readOnly;
  protected final boolean writeOnly;
  protected final boolean readWriteLeaderForDefaultCF;
  protected final boolean readWriteLeaderForRMDCF;

  private final Optional<Statistics> aggStatistics;
  private final RocksDBMemoryStats rocksDBMemoryStats;

  private Optional<Supplier<byte[]>> expectedChecksumSupplier;

  /**
   * Column Family is the concept in RocksDB to create isolation between different value for the same key. All KVs are
   * stored in `DEFAULT` column family, if no column family is specified.
   * If we stores replication metadata in the RocksDB, we stored it in a separated column family. We will insert all the
   * column family descriptors into columnFamilyDescriptors and pass it to RocksDB when opening the store, and it will
   * fill the columnFamilyHandles with handles which will be used when we want to put/get/delete
   * from different RocksDB column families.
   */
  protected final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
  protected final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
  private RocksDBSstFileWriter rocksDBSstFileWriter = null;

  protected RocksDBStoragePartition(
      StoragePartitionConfig storagePartitionConfig,
      RocksDBStorageEngineFactory factory,
      String dbDir,
      RocksDBMemoryStats rocksDBMemoryStats,
      RocksDBThrottler rocksDbThrottler,
      RocksDBServerConfig rocksDBServerConfig,
      List<byte[]> columnFamilyNameList,
      VeniceStoreVersionConfig storeConfig) {
    super(storagePartitionConfig.getPartitionId());
    this.factory = factory;
    this.rocksDBServerConfig = rocksDBServerConfig;
    this.storeNameAndVersion = storagePartitionConfig.getStoreName();
    this.storeName = Version.parseStoreFromVersionTopic(storeNameAndVersion);
    this.partitionId = storagePartitionConfig.getPartitionId();
    this.replicaId = Utils.getReplicaId(storagePartitionConfig.getStoreName(), partitionId);
    this.aggStatistics = factory.getAggStatistics();
    this.blobTransferEnabled = storeConfig.isBlobTransferEnabled();

    // If writing to offset metadata partition METADATA_PARTITION_ID enable WAL write to sync up offset on server
    // restart,
    Options options = getStoreOptions(storagePartitionConfig, false);

    // If writing to offset metadata partition METADATA_PARTITION_ID enable WAL write to sync up offset on server
    // restart,
    // if WAL is disabled then all ingestion progress made would be lost in case of non-graceful shutdown of server.
    this.writeOptions = new WriteOptions().setDisableWAL(this.partitionId != METADATA_PARTITION_ID);

    // For multiple column family enable atomic flush
    if (columnFamilyNameList.size() > 1 && rocksDBServerConfig.isAtomicFlushEnabled()) {
      options.setAtomicFlush(true);
    }

    if (options.tableFormatConfig() instanceof PlainTableConfig) {
      this.deferredWrite = false;
    } else {
      this.deferredWrite = storagePartitionConfig.isDeferredWrite();
    }
    this.readOnly = storagePartitionConfig.isReadOnly();
    this.writeOnly = storagePartitionConfig.isWriteOnlyConfig();
    this.readWriteLeaderForDefaultCF = storagePartitionConfig.isReadWriteLeaderForDefaultCF();
    this.readWriteLeaderForRMDCF = storagePartitionConfig.isReadWriteLeaderForRMDCF();
    this.fullPathForPartitionDB = RocksDBUtils.composePartitionDbDir(dbDir, storeNameAndVersion, partitionId);
    this.options = options;
    /**
     * TODO: check whether we should tune any config with {@link EnvOptions}.
     */
    this.envOptions = new EnvOptions();
    // Direct write is not efficient when there are a lot of ongoing pushes
    this.envOptions.setUseDirectWrites(false);
    this.rocksDBMemoryStats = rocksDBMemoryStats;
    this.expectedChecksumSupplier = Optional.empty();
    this.rocksDBThrottler = rocksDbThrottler;
    this.fullPathForTempSSTFileDir = RocksDBUtils.composeTempSSTFileDir(dbDir, storeNameAndVersion, partitionId);
    this.fullPathForPartitionDBSnapshot =
        blobTransferEnabled ? RocksDBUtils.composeSnapshotDir(dbDir, storeNameAndVersion, partitionId) : null;

    if (deferredWrite) {
      this.rocksDBSstFileWriter = new RocksDBSstFileWriter(
          storeNameAndVersion,
          partitionId,
          dbDir,
          envOptions,
          options,
          fullPathForTempSSTFileDir,
          false,
          rocksDBServerConfig);
    }

    /**
     * There are some possible optimization opportunities for column families. For example, optimizeForSmallDb() option
     * may be applied if we are sure replicationMetadata column family is smaller in size.
     */
    ColumnFamilyOptions columnFamilyOptions;
    for (byte[] name: columnFamilyNameList) {
      if (name == REPLICATION_METADATA_COLUMN_FAMILY && !rocksDBServerConfig.isRocksDBPlainTableFormatEnabled()) {
        columnFamilyOptions = new ColumnFamilyOptions(getStoreOptions(storagePartitionConfig, true));
      } else {
        columnFamilyOptions = new ColumnFamilyOptions(options);
      }
      columnFamilyDescriptors.add(new ColumnFamilyDescriptor(name, columnFamilyOptions));
    }
    /**
     * This new open(ReadOnly)WithColumnFamily API replace original open(ReadOnly) API to reduce code duplication.
     * In the default case, we will only open DEFAULT_COLUMN_FAMILY, which is what old API does internally.
     */
    Runnable dbOpenRunnable = () -> {
      try {
        if (this.readOnly) {
          this.rocksDB = rocksDbThrottler
              .openReadOnly(options, fullPathForPartitionDB, columnFamilyDescriptors, columnFamilyHandleList);
        } else {
          this.rocksDB =
              rocksDbThrottler.open(options, fullPathForPartitionDB, columnFamilyDescriptors, columnFamilyHandleList);
        }
      } catch (RocksDBException | InterruptedException e) {
        throw new VeniceException("Failed to open RocksDB for replica: " + replicaId, e);
      }
    };
    if (factory.enforceMemoryLimit(storeName)) {
      /**
       * We need to put a lock when calculating the memory usage since multiple threads can open different databases concurrently.
       *
       * {@link SstFileManager} doesn't check the size limit when opening up an existing database,
       * so this function will do the check manually when opening up any new database.
       */
      synchronized (factory) {
        checkMemoryLimit(factory.getMemoryLimit(), factory.getSstFileManagerForMemoryLimiter(), fullPathForPartitionDB);
        dbOpenRunnable.run();
      }
    } else {
      dbOpenRunnable.run();
    }
    registerDBStats();
    LOGGER.info(
        "Opened RocksDB: {} for replica: {} in {} and {} mode",
        fullPathForPartitionDB,
        replicaId,
        this.readOnly ? "read-only" : "read-write",
        this.deferredWrite ? "deferred write" : "non-deferred write");
  }

  public RocksDBStoragePartition(
      StoragePartitionConfig storagePartitionConfig,
      RocksDBStorageEngineFactory factory,
      String dbDir,
      RocksDBMemoryStats rocksDBMemoryStats,
      RocksDBThrottler rocksDbThrottler,
      RocksDBServerConfig rocksDBServerConfig,
      VeniceStoreVersionConfig storeConfig) {
    // If not specified, RocksDB inserts values into DEFAULT_COLUMN_FAMILY.
    this(
        storagePartitionConfig,
        factory,
        dbDir,
        rocksDBMemoryStats,
        rocksDbThrottler,
        rocksDBServerConfig,
        Collections.singletonList(RocksDB.DEFAULT_COLUMN_FAMILY),
        storeConfig);
  }

  private void checkMemoryLimit(long memoryLimit, SstFileManager sstFileManager, String dbPath) {
    if (memoryLimit < 0) {
      return;
    }

    /**
     * Check whether current {@link sstFileManager} is already tracking the sst files in the db path.
     * Here are the reasons:
     * 1. Database close won't remove the tracking info from SSTFileManager.
     * 2. During ingestion, Venice could reopen the database (close and open).
     */
    File storeDbDir = new File(dbPath);
    if (storeDbDir.exists()) {
      Map<String, Long> trackedSSTFiles = sstFileManager.getTrackedFiles();
      File[] sstFiles = storeDbDir.listFiles((dir, name) -> name.endsWith(".sst"));
      if (sstFiles == null) {
        return;
      }
      boolean alreadyTracked = false;
      long storeSize = 0;
      for (File sstFile: sstFiles) {
        if (trackedSSTFiles.containsKey(sstFile.getAbsolutePath())) {
          alreadyTracked = true;
        } else if (alreadyTracked) {
          throw new VeniceException("SSTFileManager tracking files is missing sst file: " + sstFile.getAbsolutePath());
        }
        storeSize += sstFile.length();
      }
      if (alreadyTracked) {
        return;
      }
      long currentSSTFileUsage = sstFileManager.getTotalSize();
      if (currentSSTFileUsage + storeSize >= memoryLimit) {
        throw new MemoryLimitExhaustedException(
            "Failed to open up RocksDB for replica: " + replicaId + ", memory limit: " + memoryLimit
                + " and the current memory usage: " + currentSSTFileUsage + ", new store size: " + storeSize);
      }
    }
  }

  protected void makeSureRocksDBIsStillOpen() {
    if (isClosed) {
      throw new VeniceException(
          "RocksDB has been closed for replica: " + replicaId + ", partition id: " + partitionId
              + ", any further operation is disallowed");
    }
  }

  protected EnvOptions getEnvOptions() {
    return envOptions;
  }

  protected Boolean getBlobTransferEnabled() {
    return blobTransferEnabled;
  }

  protected Options getStoreOptions(StoragePartitionConfig storagePartitionConfig, boolean isRMD) {
    Options options = new Options();

    options.setEnv(factory.getEnv());
    options.setRateLimiter(factory.getRateLimiter());
    if (factory.enforceMemoryLimit(storeName)) {
      options.setSstFileManager(factory.getSstFileManagerForMemoryLimiter());
    } else {
      options.setSstFileManager(factory.getSstFileManager());
    }
    options.setWriteBufferManager(factory.getWriteBufferManager());

    options.setCreateIfMissing(true);
    options.setCompressionType(rocksDBServerConfig.getRocksDBOptionsCompressionType());
    options.setCompactionStyle(rocksDBServerConfig.getRocksDBOptionsCompactionStyle());
    options.setBytesPerSync(rocksDBServerConfig.getRocksDBBytesPerSync());
    options.setUseDirectReads(rocksDBServerConfig.getRocksDBUseDirectReads());
    options.setMaxOpenFiles(rocksDBServerConfig.getMaxOpenFiles());
    options.setTargetFileSizeBase(rocksDBServerConfig.getTargetFileSizeInBytes());
    options.setMaxFileOpeningThreads(rocksDBServerConfig.getMaxFileOpeningThreads());
    options.setMinWriteBufferNumberToMerge(rocksDBServerConfig.getRocksDBMinWriteBufferNumberToMerge());

    /**
     * Disable the stat dump threads, which will create excessive threads, which will eventually crash
     * storage node.
     */
    options.setStatsDumpPeriodSec(0);
    options.setStatsPersistPeriodSec(0);

    options.setKeepLogFileNum(rocksDBServerConfig.getMaxLogFileNum());
    options.setMaxLogFileSize(rocksDBServerConfig.getMaxLogFileSize());

    aggStatistics.ifPresent(options::setStatistics);

    if (rocksDBServerConfig.isRocksDBPlainTableFormatEnabled()) {
      PlainTableConfig tableConfig = new PlainTableConfig();
      tableConfig.setStoreIndexInFile(rocksDBServerConfig.isRocksDBStoreIndexInFile());
      tableConfig.setHugePageTlbSize(rocksDBServerConfig.getRocksDBHugePageTlbSize());
      tableConfig.setBloomBitsPerKey(rocksDBServerConfig.getRocksDBBloomBitsPerKey());
      options.setTableFormatConfig(tableConfig);
      options.setAllowMmapReads(true);
      options.useCappedPrefixExtractor(rocksDBServerConfig.getCappedPrefixExtractorLength());
    } else {
      // Cache index and bloom filter in block cache
      // and share the same cache across all the RocksDB databases
      BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
      tableConfig.setBlockSize(rocksDBServerConfig.getRocksDBSSTFileBlockSizeInBytes());
      tableConfig.setBlockCache(factory.getSharedCache(isRMD));
      tableConfig.setCacheIndexAndFilterBlocks(rocksDBServerConfig.isRocksDBSetCacheIndexAndFilterBlocks());
      tableConfig.setFormatVersion(rocksDBServerConfig.getBlockBaseFormatVersion());
      options.setTableFormatConfig(tableConfig);

      /**
       * Only enable blob files for block-based format.
       */
      if (rocksDBServerConfig.isBlobFilesEnabled()) {
        options.setEnableBlobFiles(true);
        options.setEnableBlobGarbageCollection(true);
        options.setMinBlobSize(rocksDBServerConfig.getMinBlobSizeInBytes());
        options.setBlobFileSize(rocksDBServerConfig.getBlobFileSizeInBytes());
        options.setBlobGarbageCollectionAgeCutoff(rocksDBServerConfig.getBlobGarbageCollectionAgeCutOff());
        options.setBlobGarbageCollectionForceThreshold(rocksDBServerConfig.getBlobGarbageCollectionForceThreshold());
        options.setBlobFileStartingLevel(rocksDBServerConfig.getBlobFileStartingLevel());
      }
    }

    if (storagePartitionConfig.isWriteOnlyConfig()) {
      options
          .setLevel0FileNumCompactionTrigger(rocksDBServerConfig.getLevel0FileNumCompactionTriggerWriteOnlyVersion());
      options.setLevel0SlowdownWritesTrigger(rocksDBServerConfig.getLevel0SlowdownWritesTriggerWriteOnlyVersion());
      options.setLevel0StopWritesTrigger(rocksDBServerConfig.getLevel0StopWritesTriggerWriteOnlyVersion());
    } else {
      options.setLevel0FileNumCompactionTrigger(rocksDBServerConfig.getLevel0FileNumCompactionTrigger());
      options.setLevel0SlowdownWritesTrigger(rocksDBServerConfig.getLevel0SlowdownWritesTrigger());
      options.setLevel0StopWritesTrigger(rocksDBServerConfig.getLevel0StopWritesTrigger());
    }
    if (rocksDBServerConfig.isLevel0CompactionTuningForReadWriteLeaderEnabled()) {
      /**
       * Read-write leader tuning has higher priority than the above strategy.
       */
      if (!isRMD && storagePartitionConfig.isReadWriteLeaderForDefaultCF()
          || isRMD && storagePartitionConfig.isReadWriteLeaderForRMDCF()) {
        options.setLevel0FileNumCompactionTrigger(
            rocksDBServerConfig.getLevel0FileNumCompactionTriggerForReadWriteLeader());
        options.setLevel0SlowdownWritesTrigger(rocksDBServerConfig.getLevel0SlowdownWritesTriggerForReadWriteLeader());
        options.setLevel0StopWritesTrigger(rocksDBServerConfig.getLevel0StopWritesTriggerForReadWriteLeader());
      }
    }

    // Memtable options
    options.setWriteBufferSize(rocksDBServerConfig.getRocksDBMemtableSizeInBytes());
    options.setMaxWriteBufferNumber(rocksDBServerConfig.getRocksDBMaxMemtableCount());
    options.setMaxTotalWalSize(rocksDBServerConfig.getRocksDBMaxTotalWalSizeInBytes());
    options.setMaxBytesForLevelBase(rocksDBServerConfig.getRocksDBMaxBytesForLevelBase());
    options.setMemtableHugePageSize(rocksDBServerConfig.getMemTableHugePageSize());

    options.setCreateMissingColumnFamilies(true); // This config allows to create new column family automatically.
    return options;
  }

  protected List<ColumnFamilyHandle> getColumnFamilyHandleList() {
    return columnFamilyHandleList;
  }

  public long getRmdByteUsage() {
    return 0;
  }

  @Override
  public boolean checkDatabaseIntegrity(Map<String, String> checkpointedInfo) {
    makeSureRocksDBIsStillOpen();
    if (!deferredWrite) {
      LOGGER.info("checkDatabaseIntegrity will do nothing since 'deferredWrite' is disabled");
      return true;
    }
    return rocksDBSstFileWriter.checkDatabaseIntegrity(checkpointedInfo);
  }

  @Override
  public synchronized void beginBatchWrite(
      Map<String, String> checkpointedInfo,
      Optional<Supplier<byte[]>> expectedChecksumSupplier) {
    makeSureRocksDBIsStillOpen();
    if (!deferredWrite) {
      LOGGER.info("'beginBatchWrite' will do nothing since 'deferredWrite' is disabled");
      return;
    }
    rocksDBSstFileWriter.open(checkpointedInfo, expectedChecksumSupplier);
  }

  @Override
  public synchronized void endBatchWrite() {
    makeSureRocksDBIsStillOpen();
    if (!deferredWrite) {
      LOGGER.info("'endBatchWrite' will do nothing since 'deferredWrite' is disabled");
      return;
    }
    /**
     * Sync all the SST files before ingestion.
     */
    sync();
    /**
     * Ingest all the generated sst files into RocksDB database.
     *
     * Note: this function should be invoked after {@link #sync()} to make sure
     * the last SST file written is finished.
     */
    rocksDBSstFileWriter.ingestSSTFiles(rocksDB, columnFamilyHandleList);
  }

  @Override
  public synchronized void createSnapshot() {
    if (blobTransferEnabled) {
      createSnapshot(rocksDB, fullPathForPartitionDBSnapshot);
    }
  }

  private void checkAndThrowMemoryLimitException(RocksDBException e) {
    if (e.getMessage().contains(ROCKSDB_ERROR_MESSAGE_FOR_RUNNING_OUT_OF_SPACE_QUOTA)) {
      throw new MemoryLimitExhaustedException(
          storeNameAndVersion,
          partitionId,
          factory.getSstFileManagerForMemoryLimiter().getTotalSize());
    }
  }

  @Override
  public synchronized void put(byte[] key, byte[] value) {
    put(key, ByteBuffer.wrap(value));
  }

  @Override
  public synchronized void put(byte[] key, ByteBuffer valueBuffer) {
    makeSureRocksDBIsStillOpen();
    if (readOnly) {
      throw new VeniceException(
          "Cannot make writes while database is opened in read-only mode for replica: " + replicaId);
    }
    try {
      if (deferredWrite) {
        rocksDBSstFileWriter.put(key, valueBuffer);
      } else {
        rocksDB.put(
            writeOptions,
            key,
            0,
            key.length,
            valueBuffer.array(),
            valueBuffer.position(),
            valueBuffer.remaining());
      }
    } catch (RocksDBException e) {
      checkAndThrowMemoryLimitException(e);
      throw new VeniceException("Failed to store the key/value pair in the RocksDB: " + replicaId, e);
    }
  }

  @Override
  public <K, V> void put(K key, V value) {
    throw new UnsupportedOperationException("Method not implemented!!");
  }

  @Override
  public byte[] get(byte[] key) {
    readCloseRWLock.readLock().lock();
    try {
      makeSureRocksDBIsStillOpen();
      return rocksDB.get(key);
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to get value from RocksDB: " + replicaId, e);
    } finally {
      readCloseRWLock.readLock().unlock();
    }
  }

  @Override
  public ByteBuffer get(byte[] key, ByteBuffer valueToBePopulated) {
    readCloseRWLock.readLock().lock();
    try {
      makeSureRocksDBIsStillOpen();
      int size = rocksDB.get(key, valueToBePopulated.array());
      if (size == RocksDB.NOT_FOUND) {
        return null;
      } else if (size > valueToBePopulated.capacity()) {
        LOGGER.warn(
            "Reallocating a new ByteBuffer of size {}, previous size was {}",
            size,
            valueToBePopulated.capacity());
        valueToBePopulated = ByteBuffer.allocate(size);
        size = rocksDB.get(key, valueToBePopulated.array());
      }
      valueToBePopulated.position(0);
      valueToBePopulated.limit(size);
      return valueToBePopulated;
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to get value from RocksDB: " + replicaId, e);
    } finally {
      readCloseRWLock.readLock().unlock();
    }
  }

  @Override
  public <K, V> V get(K key) {
    throw new UnsupportedOperationException("Method not implemented!!");
  }

  @Override
  public byte[] get(ByteBuffer keyBuffer) {
    readCloseRWLock.readLock().lock();
    try {
      makeSureRocksDBIsStillOpen();
      return rocksDB.get(keyBuffer.array(), keyBuffer.position(), keyBuffer.remaining());
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to get value from RocksDB: " + replicaId, e);
    } finally {
      readCloseRWLock.readLock().unlock();
    }
  }

  public List<byte[]> multiGet(List<byte[]> keys) {
    readCloseRWLock.readLock().lock();
    try {
      makeSureRocksDBIsStillOpen();
      return rocksDB.multiGetAsList(keys);
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to get value from RocksDB: " + replicaId, e);
    } finally {
      readCloseRWLock.readLock().unlock();
    }
  }

  public List<ByteBuffer> multiGet(List<ByteBuffer> keys, List<ByteBuffer> values) {
    readCloseRWLock.readLock().lock();

    try {
      makeSureRocksDBIsStillOpen();
      List<ByteBufferGetStatus> statusList = rocksDB.multiGetByteBuffers(keys, values);
      int keyCnt = keys.size();
      int statusCnt = statusList.size();
      int valueCnt = values.size();
      if (keyCnt != statusCnt) {
        throw new VeniceException(
            "RocksDB: " + replicaId + " returns inconsistent number of statuses, key count: " + keys.size()
                + ", but returns: " + statusCnt);
      }
      if (keyCnt != valueCnt) {
        throw new VeniceException(
            "RocksDB: " + replicaId + " returns inconsistent number of results, key count: " + keys.size()
                + ", but returns: " + valueCnt);
      }

      List<ByteBuffer> resultList = new ArrayList<>(keys.size());
      Iterator<ByteBufferGetStatus> statusIter = statusList.iterator();
      Iterator<ByteBuffer> keyIter = keys.iterator();
      ListIterator<ByteBuffer> valueIter = values.listIterator();
      while (keyIter.hasNext()) {
        ByteBuffer key = keyIter.next();
        ByteBufferGetStatus bbStatus = statusIter.next();
        Status.Code statusCode = bbStatus.status.getCode();
        ByteBuffer value = valueIter.next();
        if (statusCode.equals(Status.Code.Ok)) {
          // Need to check whether the result is complete or not by comparing length
          if (value.remaining() == bbStatus.requiredSize) {
            // good
            resultList.add(value);
          } else {
            // Need to look it up again since the passed buffer is too small
            byte[] newValue;
            if (key.isDirect()) {
              byte[] keyBytes = new byte[key.remaining()];
              key.mark();
              key.get(keyBytes, 0, key.remaining());
              key.reset();
              newValue = get(keyBytes);
            } else {
              newValue = get(key);
            }
            ByteBuffer newValueBuffer = ByteBuffer.allocateDirect(newValue.length);
            newValueBuffer.put(newValue);
            newValueBuffer.flip();
            resultList.add(newValueBuffer);
            valueIter.set(newValueBuffer);
          }
        } else if (statusCode.equals(Status.Code.NotFound)) {
          resultList.add(null);
        } else {
          throw new VeniceException("Received unexpected code: " + statusCode + " from RocksDB: " + replicaId);
        }
      }

      return resultList;
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to get value from RocksDB: " + replicaId, e);
    } finally {
      readCloseRWLock.readLock().unlock();
    }
  }

  @Override
  public void getByKeyPrefix(byte[] keyPrefix, BytesStreamingCallback callback) {
    if (keyPrefix != null && rocksDBServerConfig.isRocksDBPlainTableFormatEnabled()) {
      throw new VeniceException("Get by key prefix is not supported with RocksDB PlainTable Format.");
    }

    readCloseRWLock.readLock().lock();
    try {
      makeSureRocksDBIsStillOpen();

      try (ReadOptions readOptions = getReadOptionsForIteration(keyPrefix);
          RocksIterator iterator = rocksDB.newIterator(readOptions)) {
        if (keyPrefix == null) {
          iterator.seekToFirst();
        } else {
          iterator.seek(keyPrefix);
        }
        while (iterator.isValid()) {
          callback.onRecordReceived(iterator.key(), iterator.value());
          iterator.next();
        }
      }
    } finally {
      readCloseRWLock.readLock().unlock();
      callback.onCompletion();
    }
  }

  public synchronized boolean validateBatchIngestion() {
    if (!deferredWrite) {
      return true;
    }
    return rocksDBSstFileWriter.validateBatchIngestion();
  }

  private ReadOptions getReadOptionsForIteration(byte[] keyPrefix) {
    if (keyPrefix == null) {
      return new ReadOptions();
    } else {
      return new ReadOptions().setIterateUpperBound(getPrefixIterationUpperBound(keyPrefix));
    }
  }

  private Slice getPrefixIterationUpperBound(byte[] prefix) {
    byte[] upperBound = getIncrementedByteArray(Arrays.copyOf(prefix, prefix.length), prefix.length - 1);
    return upperBound == null ? null : new Slice(upperBound);
  }

  private byte[] getIncrementedByteArray(byte[] array, int indexToIncrement) {
    byte maxUnsignedByte = (byte) 255;
    if (array[indexToIncrement] != maxUnsignedByte) {
      array[indexToIncrement]++;
      return array;
    } else if (indexToIncrement > 0) {
      array[indexToIncrement] = 0;
      return getIncrementedByteArray(array, --indexToIncrement);
    } else {
      return null;
    }
  }

  @Override
  public synchronized void delete(byte[] key) {
    makeSureRocksDBIsStillOpen();
    if (readOnly) {
      throw new VeniceException(
          "Cannot make deletion while database is opened in read-only mode for replica: " + replicaId);
    }
    try {
      if (deferredWrite) {
        throw new VeniceException("Deletion is unexpected in 'deferredWrite' mode");
      } else {
        rocksDB.delete(key);
      }
    } catch (RocksDBException e) {
      checkAndThrowMemoryLimitException(e);
      throw new VeniceException("Failed to delete entry from RocksDB: " + replicaId, e);
    }
  }

  @Override
  public synchronized Map<String, String> sync() {
    makeSureRocksDBIsStillOpen();
    if (!deferredWrite) {
      LOGGER.debug("Flush memtable to disk for RocksDB: {}", replicaId);

      if (this.readOnly) {
        /**
         * Update the log level to be debug since in some cases (a sync could be triggered after adjusting the storage
         * engine to be ready-only even there is no write in between), this log is causing confusion.
         *
         * And this log is not so important because of the following reasons:
         * 1. If there is data being writen before 'sync', the write will be rejected with proper exceptional message.
         * 2. If there is no data being writen before sync, the 'sync' will do nothing.
         */
        LOGGER.debug("Unexpected sync in RocksDB read-only mode");
      } else {
        try {
          // Since Venice RocksDB database disables WAL, flush will be triggered for every 'sync' to
          // avoid data loss during crash recovery
          rocksDB.flush(WAIT_FOR_FLUSH_OPTIONS, columnFamilyHandleList);
        } catch (RocksDBException e) {
          checkAndThrowMemoryLimitException(e);
          throw new VeniceException("Failed to flush memtable to disk for RocksDB: " + replicaId, e);
        }
      }
      return Collections.emptyMap();
    }
    return rocksDBSstFileWriter.sync();
  }

  public void deleteFilesInDirectory(String fullPath) {
    if (fullPath == null || fullPath.isEmpty()) {
      return;
    }

    File dir = new File(fullPath);
    if (dir.exists()) {
      // Remove the files inside
      Arrays.stream(dir.list()).forEach(file -> {
        if (!(new File(fullPath, file).delete())) {
          LOGGER.warn("Failed to remove file: {} in dir: {}", file, fullPath);
        }
      });
    }
  }

  private void deleteDirectory(String fullPath) {
    // Remove the files inside the directory
    deleteFilesInDirectory(fullPath);
    // Remove the directory
    File dir = new File(fullPath);
    if (dir.exists()) {
      if (!dir.delete()) {
        LOGGER.warn("Failed to remove dir: {}", fullPath);
      }
    }
  }

  @Override
  public synchronized void drop() {
    close();
    try {
      Options storeOptions = getStoreOptions(new StoragePartitionConfig(storeNameAndVersion, partitionId), false);
      RocksDB.destroyDB(fullPathForPartitionDB, storeOptions);
      storeOptions.close();
    } catch (RocksDBException e) {
      LOGGER.error("Failed to destroy DB for replica: {}", replicaId);
    }
    /**
     * To avoid resource leaking, we will clean up all the database files anyway.
     */
    // Remove extra SST files first
    deleteFilesInDirectory(fullPathForTempSSTFileDir);
    // remove snapshots files
    deleteFilesInDirectory(fullPathForPartitionDBSnapshot);
    // Remove partition directory
    deleteDirectory(fullPathForPartitionDB);
    LOGGER.info("RocksDB for replica:{} was dropped.", replicaId);
  }

  @Override
  public synchronized void close() {
    if (isClosed) {
      return;
    }
    long startTimeInMs = System.currentTimeMillis();
    /**
     * The following operations are used to free up memory.
     */
    deRegisterDBStats();
    readCloseRWLock.writeLock().lock();
    try {
      rocksDB.close();
    } finally {
      isClosed = true;
      readCloseRWLock.writeLock().unlock();
    }
    if (envOptions != null) {
      envOptions.close();
    }
    if (deferredWrite) {
      rocksDBSstFileWriter.close();
    }
    options.close();
    if (writeOptions != null) {
      writeOptions.close();
    }
    LOGGER.info(
        "RocksDB close for replica: {} took {} ms.",
        replicaId,
        LatencyUtils.getElapsedTimeFromMsToMs(startTimeInMs));
  }

  /**
   * Reopen the underlying RocksDB database, and this operation will unload the data cached in memory.
   */
  @Override
  public synchronized void reopen() {
    readCloseRWLock.writeLock().lock();
    try {
      long startTimeInMs = System.currentTimeMillis();
      rocksDB.close();
      LOGGER.info(
          "RocksDB close for replica: {} took {} ms.",
          replicaId,
          LatencyUtils.getElapsedTimeFromMsToMs(startTimeInMs));
      if (this.readOnly) {
        this.rocksDB = rocksDBThrottler
            .openReadOnly(options, fullPathForPartitionDB, columnFamilyDescriptors, columnFamilyHandleList);
      } else {
        this.rocksDB =
            rocksDBThrottler.open(options, fullPathForPartitionDB, columnFamilyDescriptors, columnFamilyHandleList);
      }
      LOGGER.info("Reopened RocksDB for replica: {}", replicaId);
    } catch (Exception e) {
      throw new VeniceException("Failed to reopen RocksDB for replica: " + replicaId);
    } finally {
      readCloseRWLock.writeLock().unlock();
    }
  }

  private void registerDBStats() {
    if (rocksDBMemoryStats != null) {
      rocksDBMemoryStats.registerPartition(RocksDBUtils.getPartitionDbName(storeNameAndVersion, partitionId), this);
    }
  }

  private void deRegisterDBStats() {
    if (rocksDBMemoryStats != null) {
      rocksDBMemoryStats.deregisterPartition(RocksDBUtils.getPartitionDbName(storeNameAndVersion, partitionId));
    }
  }

  public long getRocksDBStatValue(String statName) {
    readCloseRWLock.readLock().lock();
    try {
      makeSureRocksDBIsStillOpen();
      return rocksDB.getLongProperty(statName);
    } catch (RocksDBException e) {
      throw new VeniceException(
          "Failed to get property value from RocksDB: " + replicaId + " for property: " + statName,
          e);
    } finally {
      readCloseRWLock.readLock().unlock();
    }
  }

  public Map<MemoryUsageType, Long> getApproximateMemoryUsageByType(final Set<Cache> caches) {
    readCloseRWLock.readLock().lock();
    try {
      makeSureRocksDBIsStillOpen();
      return MemoryUtil.getApproximateMemoryUsageByType(Arrays.asList(rocksDB), caches);
    } finally {
      readCloseRWLock.readLock().unlock();
    }
  }

  /**
   * Check {@link AbstractStoragePartition#verifyConfig(StoragePartitionConfig)}.
   *
   * @param partitionConfig
   * @return
   */
  @Override
  public boolean verifyConfig(StoragePartitionConfig partitionConfig) {
    if (readOnly != partitionConfig.isReadOnly()) {
      return false;
    }
    if (writeOnly != partitionConfig.isWriteOnlyConfig()) {
      return false;
    }
    if (rocksDBServerConfig.isLevel0CompactionTuningForReadWriteLeaderEnabled()) {
      if (readWriteLeaderForDefaultCF != partitionConfig.isReadWriteLeaderForDefaultCF()) {
        return false;
      }
      if (readWriteLeaderForRMDCF != partitionConfig.isReadWriteLeaderForRMDCF()) {
        return false;
      }
    }

    if (options.tableFormatConfig() instanceof BlockBasedTableConfig
        && deferredWrite != partitionConfig.isDeferredWrite()) {
      return false;
    }

    return true;
  }

  @Override
  public long getPartitionSizeInBytes() {
    readCloseRWLock.readLock().lock();
    try {
      makeSureRocksDBIsStillOpen();
      return getRocksDBStatValue("rocksdb.live-sst-files-size") + getRocksDBStatValue("rocksdb.live-blob-file-size");
    } finally {
      readCloseRWLock.readLock().unlock();
    }
  }

  protected Options getOptions() {
    return options;
  }

  // Visible for testing
  public String getFullPathForTempSSTFileDir() {
    return fullPathForTempSSTFileDir;
  }

  // Visible for testing
  public RocksDBSstFileWriter getRocksDBSstFileWriter() {
    return rocksDBSstFileWriter;
  }

  @Override
  public AbstractStorageIterator getIterator() {
    return new RocksDBStorageIterator(rocksDB.newIterator());
  }

  /**
   * util method to create a snapshot
   * It will check the snapshot directory and delete it if it exists, then generate a new snapshot
   */
  public static void createSnapshot(RocksDB rocksDB, String fullPathForPartitionDBSnapshot) {
    LOGGER.info("Creating snapshot in directory: {}", fullPathForPartitionDBSnapshot);

    // clean up the snapshot directory if it exists
    File partitionSnapshotDir = new File(fullPathForPartitionDBSnapshot);
    if (partitionSnapshotDir.exists()) {
      LOGGER.info("Snapshot directory already exists, deleting old snapshots at {}", fullPathForPartitionDBSnapshot);
      try {
        FileUtils.deleteDirectory(partitionSnapshotDir);
      } catch (IOException e) {
        throw new VeniceException(
            "Failed to delete the existing snapshot directory: " + fullPathForPartitionDBSnapshot,
            e);
      }
    }

    try {
      LOGGER.info("Start creating snapshots in directory: {}", fullPathForPartitionDBSnapshot);

      Checkpoint checkpoint = Checkpoint.create(rocksDB);
      checkpoint.createCheckpoint(fullPathForPartitionDBSnapshot);

      LOGGER.info("Finished creating snapshots in directory: {}", fullPathForPartitionDBSnapshot);
    } catch (RocksDBException e) {
      throw new VeniceException(
          "Received exception during RocksDB's snapshot creation in directory " + fullPathForPartitionDBSnapshot,
          e);
    }
  }
}
