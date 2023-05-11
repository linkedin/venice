package com.linkedin.davinci.store.rocksdb;

import static com.linkedin.davinci.store.AbstractStorageEngine.METADATA_PARTITION_ID;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.LatencyUtils;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import org.rocksdb.Cache;
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
import org.rocksdb.SstFileWriter;
import org.rocksdb.Statistics;
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
  protected static final ReadOptions READ_OPTIONS_DEFAULT = new ReadOptions();
  static final byte[] REPLICATION_METADATA_COLUMN_FAMILY = "timestamp_metadata".getBytes();

  private static final FlushOptions WAIT_FOR_FLUSH_OPTIONS = new FlushOptions().setWaitForFlush(true);

  /**
   * Here RocksDB disables WAL, but relies on the 'flush', which will be invoked through {@link #sync()}
   * to avoid data loss during recovery.
   */
  protected final WriteOptions writeOptions;
  private final String fullPathForTempSSTFileDir;

  private final EnvOptions envOptions;

  protected final String storeName;
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
   * Whether the input is sorted or not.
   */
  protected final boolean deferredWrite;

  /**
   * Whether the database is read only or not.
   */
  protected final boolean readOnly;
  protected final boolean writeOnly;
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
      List<byte[]> columnFamilyNameList) {
    super(storagePartitionConfig.getPartitionId());
    this.factory = factory;
    this.rocksDBServerConfig = rocksDBServerConfig;
    // Create the folder for storage partition if it doesn't exist
    this.storeName = storagePartitionConfig.getStoreName();
    this.partitionId = storagePartitionConfig.getPartitionId();
    this.aggStatistics = factory.getAggStatistics();

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
    this.fullPathForPartitionDB = RocksDBUtils.composePartitionDbDir(dbDir, storeName, partitionId);
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
    this.fullPathForTempSSTFileDir = RocksDBUtils.composeTempSSTFileDir(dbDir, storeName, partitionId);
    if (deferredWrite) {
      this.rocksDBSstFileWriter = new RocksDBSstFileWriter(
          storeName,
          partitionId,
          dbDir,
          envOptions,
          options,
          fullPathForTempSSTFileDir,
          false,
          rocksDBServerConfig);
    }

    try {
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
      if (this.readOnly) {
        this.rocksDB = rocksDbThrottler
            .openReadOnly(options, fullPathForPartitionDB, columnFamilyDescriptors, columnFamilyHandleList);
      } else {
        this.rocksDB =
            rocksDbThrottler.open(options, fullPathForPartitionDB, columnFamilyDescriptors, columnFamilyHandleList);
      }
    } catch (RocksDBException | InterruptedException e) {
      throw new VeniceException("Failed to open RocksDB for store: " + storeName + ", partition id: " + partitionId, e);
    }

    registerDBStats();
    LOGGER.info(
        "Opened RocksDB for store: {}, partition: {}, in {} and {} mode",
        storeName,
        partitionId,
        this.readOnly ? "read-only" : "read-write",
        this.deferredWrite ? "deferred write" : "non-deferred write");
  }

  public RocksDBStoragePartition(
      StoragePartitionConfig storagePartitionConfig,
      RocksDBStorageEngineFactory factory,
      String dbDir,
      RocksDBMemoryStats rocksDBMemoryStats,
      RocksDBThrottler rocksDbThrottler,
      RocksDBServerConfig rocksDBServerConfig) {
    // If not specified, RocksDB inserts values into DEFAULT_COLUMN_FAMILY.
    this(
        storagePartitionConfig,
        factory,
        dbDir,
        rocksDBMemoryStats,
        rocksDbThrottler,
        rocksDBServerConfig,
        Collections.singletonList(RocksDB.DEFAULT_COLUMN_FAMILY));
  }

  protected void makeSureRocksDBIsStillOpen() {
    if (isClosed) {
      throw new VeniceException(
          "RocksDB has been closed for store: " + storeName + ", partition id: " + partitionId
              + ", any further operation is disallowed");
    }
  }

  protected EnvOptions getEnvOptions() {
    return envOptions;
  }

  private Options getStoreOptions(StoragePartitionConfig storagePartitionConfig, boolean isRMD) {
    Options options = new Options();

    options.setEnv(factory.getEnv());
    options.setRateLimiter(factory.getRateLimiter());
    options.setSstFileManager(factory.getSstFileManager());
    options.setWriteBufferManager(factory.getWriteBufferManager());

    options.setCreateIfMissing(true);
    options.setCompressionType(rocksDBServerConfig.getRocksDBOptionsCompressionType());
    options.setCompactionStyle(rocksDBServerConfig.getRocksDBOptionsCompactionStyle());
    options.setBytesPerSync(rocksDBServerConfig.getRocksDBBytesPerSync());
    options.setUseDirectReads(rocksDBServerConfig.getRocksDBUseDirectReads());
    options.setMaxOpenFiles(rocksDBServerConfig.getMaxOpenFiles());
    options.setTargetFileSizeBase(rocksDBServerConfig.getTargetFileSizeInBytes());
    options.setMaxFileOpeningThreads(rocksDBServerConfig.getMaxFileOpeningThreads());

    /**
     * Disable the stat dump threads, which will create excessive threads, which will eventually crash
     * storage node.
     */
    options.setStatsDumpPeriodSec(0);
    options.setStatsPersistPeriodSec(0);

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

      // TODO Consider Adding "cache_index_and_filter_blocks_with_high_priority" to allow for preservation of indexes in
      // memory.
      // https://github.com/facebook/rocksdb/wiki/Block-Cache#caching-index-and-filter-blocks
      // https://github.com/facebook/rocksdb/wiki/Block-Cache#lru-cache

      tableConfig.setBlockCacheCompressedSize(rocksDBServerConfig.getRocksDBBlockCacheCompressedSizeInBytes());
      tableConfig.setFormatVersion(rocksDBServerConfig.getBlockBaseFormatVersion());
      options.setTableFormatConfig(tableConfig);
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
  public synchronized void put(byte[] key, byte[] value) {
    put(key, ByteBuffer.wrap(value));
  }

  @Override
  public synchronized void put(byte[] key, ByteBuffer valueBuffer) {
    makeSureRocksDBIsStillOpen();
    if (readOnly) {
      throw new VeniceException(
          "Cannot make writes while partition is opened in read-only mode" + ", partition=" + storeName + "_"
              + partitionId);
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
      throw new VeniceException(
          "Failed to put key/value pair to store: " + storeName + ", partition id: " + partitionId,
          e);
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
      throw new VeniceException("Failed to get value from store: " + storeName + ", partition id: " + partitionId, e);
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
      throw new VeniceException("Failed to get value from store: " + storeName + ", partition id: " + partitionId, e);
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
      throw new VeniceException("Failed to get value from store: " + storeName + ", partition id: " + partitionId, e);
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
          "Cannot make deletion while partition is opened in read-only mode" + ", partition=" + storeName + "_"
              + partitionId);
    }
    try {
      if (deferredWrite) {
        throw new VeniceException("Deletion is unexpected in 'deferredWrite' mode");
      } else {
        rocksDB.delete(key);
      }
    } catch (RocksDBException e) {
      throw new VeniceException(
          "Failed to delete entry from store: " + storeName + ", partition id: " + partitionId,
          e);
    }
  }

  @Override
  public synchronized Map<String, String> sync() {
    makeSureRocksDBIsStillOpen();
    if (!deferredWrite) {
      LOGGER.debug("Flush memtable to disk for store: {}, partition id: {}", storeName, partitionId);

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
          // Since Venice RocksDB database disables WAL, flush will be triggered for every 'sync' to avoid data loss
          // during
          // crash recovery
          rocksDB.flush(WAIT_FOR_FLUSH_OPTIONS);
        } catch (RocksDBException e) {
          throw new VeniceException(
              "Failed to flush memtable to disk for store: " + storeName + ", partition id: " + partitionId,
              e);
        }
      }
      return Collections.emptyMap();
    }

    return rocksDBSstFileWriter.sync();
  }

  public void deleteFilesInDirectory(String fullPath) {
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
      Options storeOptions = getStoreOptions(new StoragePartitionConfig(storeName, partitionId), false);
      RocksDB.destroyDB(fullPathForPartitionDB, storeOptions);
      storeOptions.close();
    } catch (RocksDBException e) {
      LOGGER.error("Failed to destroy DB for store: {}, partition: {}", storeName, partitionId);
    }
    /**
     * To avoid resource leaking, we will clean up all the database files anyway.
     */
    // Remove extra SST files first
    deleteFilesInDirectory(fullPathForTempSSTFileDir);
    // Remove partition directory
    deleteDirectory(fullPathForPartitionDB);
    LOGGER.info("RocksDB for store: {}, partition: {} was dropped.", storeName, partitionId);
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
        "RocksDB close for store: {}, partition {} took {} ms.",
        storeName,
        partitionId,
        LatencyUtils.getElapsedTimeInMs(startTimeInMs));
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
          "RocksDB close for store: {}, partition {} took {} ms.",
          storeName,
          partitionId,
          LatencyUtils.getElapsedTimeInMs(startTimeInMs));

      if (this.readOnly) {
        this.rocksDB = rocksDBThrottler
            .openReadOnly(options, fullPathForPartitionDB, columnFamilyDescriptors, columnFamilyHandleList);
      } else {
        this.rocksDB =
            rocksDBThrottler.open(options, fullPathForPartitionDB, columnFamilyDescriptors, columnFamilyHandleList);
      }
      LOGGER.info("Reopened RocksDB for store: {}, partition: {}", storeName, partitionId);
    } catch (Exception e) {
      throw new VeniceException("Failed to reopen RocksDB for store: " + storeName + " partition: " + partitionId);
    } finally {
      readCloseRWLock.writeLock().unlock();
    }
  }

  private void registerDBStats() {
    if (rocksDBMemoryStats != null) {
      rocksDBMemoryStats.registerPartition(RocksDBUtils.getPartitionDbName(storeName, partitionId), this);
    }
  }

  private void deRegisterDBStats() {
    if (rocksDBMemoryStats != null) {
      rocksDBMemoryStats.deregisterPartition(RocksDBUtils.getPartitionDbName(storeName, partitionId));
    }
  }

  public long getRocksDBStatValue(String statName) {
    readCloseRWLock.readLock().lock();
    try {
      makeSureRocksDBIsStillOpen();
      return rocksDB.getLongProperty(statName);
    } catch (RocksDBException e) {
      throw new VeniceException(
          "Failed to get property value from store: " + storeName + ", partition id: " + partitionId + " for property: "
              + statName,
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
    if (options.tableFormatConfig() instanceof PlainTableConfig) {
      return readOnly == partitionConfig.isReadOnly() && writeOnly == partitionConfig.isWriteOnlyConfig();
    }
    return deferredWrite == partitionConfig.isDeferredWrite() && readOnly == partitionConfig.isReadOnly()
        && writeOnly == partitionConfig.isWriteOnlyConfig();
  }

  /**
   * This method calculates the file size by adding all subdirectories size
   * @return the partition db size in bytes
   */
  @Override
  public long getPartitionSizeInBytes() {
    File partitionDbDir = new File(fullPathForPartitionDB);
    if (partitionDbDir.exists()) {
      /**
       * {@link FileUtils#sizeOf(File)} will throw {@link IllegalArgumentException} if the file/dir doesn't exist.
       */
      return FileUtils.sizeOf(partitionDbDir);
    } else {
      return 0;
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
}
