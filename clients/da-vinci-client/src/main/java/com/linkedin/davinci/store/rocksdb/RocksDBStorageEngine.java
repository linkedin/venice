package com.linkedin.davinci.store.rocksdb;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.SstFileManager;


public class RocksDBStorageEngine extends AbstractStorageEngine<RocksDBStoragePartition> {
  private static final Logger LOGGER = LogManager.getLogger(RocksDBStorageEngine.class);

  public static final String SERVER_CONFIG_FILE_NAME = "rocksdbConfig";

  private final String rocksDbPath;
  private final String storeDbPath;
  private final RocksDBMemoryStats memoryStats;
  private final RocksDBThrottler rocksDbThrottler;
  private final RocksDBServerConfig rocksDBServerConfig;
  private final RocksDBStorageEngineFactory factory;
  private final VeniceStoreVersionConfig storeConfig;
  private final boolean replicationMetadataEnabled;

  /**
   * The cached value will be refreshed by {@link #getStoreSizeInBytes()}.
   */
  private long cachedDiskUsage = 0;
  /**
   * The cached value will be refreshed by {@link #getRMDSizeInBytes()}.
   */
  private long cachedRMDDiskUsage = 0;

  public RocksDBStorageEngine(
      VeniceStoreVersionConfig storeConfig,
      RocksDBStorageEngineFactory factory,
      String rocksDbPath,
      RocksDBMemoryStats rocksDBMemoryStats,
      RocksDBThrottler rocksDbThrottler,
      RocksDBServerConfig rocksDBServerConfig,
      InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
      boolean replicationMetadataEnabled) {
    super(storeConfig.getStoreVersionName(), storeVersionStateSerializer, partitionStateSerializer);
    this.storeConfig = storeConfig;
    this.rocksDbPath = rocksDbPath;
    this.memoryStats = rocksDBMemoryStats;
    this.rocksDbThrottler = rocksDbThrottler;
    this.rocksDBServerConfig = rocksDBServerConfig;
    this.factory = factory;
    this.replicationMetadataEnabled = replicationMetadataEnabled;

    // Create store folder if it doesn't exist
    storeDbPath = RocksDBUtils.composeStoreDbDir(this.rocksDbPath, getStoreName());
    File storeDbDir = new File(storeDbPath);
    if (!storeDbDir.exists()) {
      storeDbDir.mkdirs();
      LOGGER.info("Created RocksDb dir for store: {}", getStoreName());
    } else {
      if (storeConfig.isRocksDbStorageEngineConfigCheckEnabled()) {
        // We only validate it when re-opening the storage engine.
        if (hasConflictPersistedStoreEngineConfig()) {
          try {
            LOGGER.info("Removing store directory: {}", storeDbDir.getAbsolutePath());
            FileUtils.deleteDirectory(storeDbDir);
          } catch (IOException e) {
            throw new VeniceException("Encounter IO exception when removing RocksDB engine folder.", e);
          }
          storeDbDir.mkdirs();
        }
      }
    }

    // restoreStoragePartitions will create metadata partition if not exist.
    restoreStoragePartitions(storeConfig.isRestoreMetadataPartition(), storeConfig.isRestoreDataPartitions());

    if (storeConfig.isRestoreMetadataPartition()) {
      // Persist RocksDB table format option used in building the storage engine.
      persistStoreEngineConfig();
    }
  }

  @Override
  public PersistenceType getType() {
    return PersistenceType.ROCKS_DB;
  }

  @Override
  protected Set<Integer> getPersistedPartitionIds() {
    File storeDbDir = new File(storeDbPath);
    if (!storeDbDir.exists()) {
      LOGGER.info("Store dir: {} doesn't exist", storeDbPath);
      return Collections.emptySet();
    }
    if (!storeDbDir.isDirectory()) {
      throw new VeniceException("Store dir: " + storeDbPath + " is not a directory!!!");
    }
    String[] partitionDbNames = storeDbDir.list();
    HashSet<Integer> partitionIdSet = new HashSet<>();
    if (partitionDbNames != null) {
      for (String partitionDbName: partitionDbNames) {
        partitionIdSet.add(RocksDBUtils.parsePartitionIdFromPartitionDbName(partitionDbName));
      }
    }
    return partitionIdSet;
  }

  @Override
  public RocksDBStoragePartition createStoragePartition(StoragePartitionConfig storagePartitionConfig) {
    // Metadata partition should not enable replication metadata column family.
    if (storagePartitionConfig.getPartitionId() == METADATA_PARTITION_ID || !replicationMetadataEnabled) {
      return new RocksDBStoragePartition(
          storagePartitionConfig,
          factory,
          rocksDbPath,
          memoryStats,
          rocksDbThrottler,
          rocksDBServerConfig);
    } else {
      return new ReplicationMetadataRocksDBStoragePartition(
          storagePartitionConfig,
          factory,
          rocksDbPath,
          memoryStats,
          rocksDbThrottler,
          rocksDBServerConfig);
    }
  }

  @Override
  public void drop() {
    super.drop();

    // Whoever is in control of the metadata partition should be responsible of dropping the storage engine folder.
    if (storeConfig.isRestoreMetadataPartition()) {
      // Remove store db dir
      File storeDbDir = new File(storeDbPath);
      if (storeDbDir.exists()) {
        LOGGER.info("Started removing database dir: {} for store: {}", storeDbPath, getStoreName());
        if (!storeDbDir.delete()) {
          LOGGER.warn("Failed to remove dir: {}.", storeDbDir);
        } else {
          LOGGER.info("Finished removing database dir: {} for store {}", storeDbPath, getStoreName());
        }
      }
    }
  }

  @Override
  public long getRMDSizeInBytes() {
    Set<Integer> partitionIds = super.getPartitionIds();
    long diskUsage = 0;
    for (int i: partitionIds) {
      AbstractStoragePartition partition;
      try {
        partition = super.getPartitionOrThrow(i);
      } catch (VeniceException e) {
        LOGGER.warn("Could not find partition {} for store {}", i, super.getStoreName());
        continue;
      }
      diskUsage += partition.getRmdByteUsage();
    }
    cachedRMDDiskUsage = diskUsage;

    return diskUsage;
  }

  public long getCachedRMDSizeInBytes() {
    return cachedRMDDiskUsage;
  }

  @Override
  public long getStoreSizeInBytes() {
    File storeDbDir = new File(storeDbPath);
    if (storeDbDir.exists()) {
      /**
       * {@link FileUtils#sizeOf(File)} will throw {@link IllegalArgumentException} if the file/dir doesn't exist.
       */
      cachedDiskUsage = FileUtils.sizeOf(storeDbDir);
    } else {
      cachedDiskUsage = 0;
    }
    return cachedDiskUsage;
  }

  @Override
  public long getCachedStoreSizeInBytes() {
    return cachedDiskUsage;
  }

  private boolean hasConflictPersistedStoreEngineConfig() {
    String configPath = getRocksDbEngineConfigPath();
    File storeEngineConfig = new File(configPath);
    if (storeEngineConfig.exists()) {
      LOGGER.info("RocksDB storage engine config found at {}", configPath);
      try {
        VeniceProperties persistedStorageEngineConfig = Utils.parseProperties(storeEngineConfig);
        LOGGER.info("Found storage engine configs: {}", persistedStorageEngineConfig.toString(true));
        boolean usePlainTableFormat = persistedStorageEngineConfig.getBoolean(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, true);
        if (usePlainTableFormat != rocksDBServerConfig.isRocksDBPlainTableFormatEnabled()) {
          String existingTableFormat = usePlainTableFormat ? "PlainTable" : "BlockBasedTable";
          String newTableFormat =
              rocksDBServerConfig.isRocksDBPlainTableFormatEnabled() ? "PlainTable" : "BlockBasedTable";
          LOGGER.warn(
              "Tried to open an existing {} RocksDB format engine with table format option: {}. Will remove the content and recreate the folder.",
              existingTableFormat,
              newTableFormat);
          return true;
        }
      } catch (IOException e) {
        throw new VeniceException("Encounter IO exception when validating RocksDB engine configs.", e);
      }
    } else {
      // If no existing config is found, we will by default skip the checking as not enough information is given to
      // enforce the check.
      LOGGER.warn("RocksDB storage engine config not found for store {} skipping the validation.", getStoreName());
    }
    return false;
  }

  private void persistStoreEngineConfig() {
    String configPath = getRocksDbEngineConfigPath();
    File storeEngineConfig = new File(configPath);
    if (storeEngineConfig.exists()) {
      LOGGER.warn("RocksDB engine already exists, will skip persisting config.");
      return;
    }
    try {
      storeConfig.getPersistStorageEngineConfig().storeFlattened(storeEngineConfig);
    } catch (IOException e) {
      throw new VeniceException("Unable to persist store engine config.", e);
    }
  }

  private String getRocksDbEngineConfigPath() {
    return RocksDBUtils.composePartitionDbDir(rocksDbPath, getStoreName(), METADATA_PARTITION_ID) + "/"
        + SERVER_CONFIG_FILE_NAME;
  }

  @Override
  public boolean hasMemorySpaceLeft() {
    SstFileManager sstFileManager = factory.getSstFileManager();
    if (sstFileManager.isMaxAllowedSpaceReached() || sstFileManager.isMaxAllowedSpaceReachedIncludingCompactions()) {
      return false;
    }
    long currentUsage = sstFileManager.getTotalSize();
    if (factory.getMemoryLimit() - currentUsage >= 2 * factory.getMemtableSize()) {
      return true;
    }
    return false;
  }
}
