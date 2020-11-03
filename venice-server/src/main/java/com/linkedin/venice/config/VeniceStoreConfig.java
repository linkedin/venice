package com.linkedin.venice.config;

import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;

import static com.linkedin.venice.store.rocksdb.RocksDBServerConfig.*;


/**
 * class that maintains all properties that are not specific to a venice server and cluster.
 * Includes individual store properties and other properties that can be overwritten.
 */
public class VeniceStoreConfig extends VeniceServerConfig {

  private String storeName;
  /**
   * This config indicates the persistence type being used in local node.
   * It is possible to have different persistence types in stores within the same node since storage engine type
   * of all the stores couldn't be switched at one time.
   */
  private Optional<PersistenceType> storePersistenceType = Optional.empty();

  private final VeniceProperties persistStorageEngineConfigs;
  // TODO: Store level bdb configuration, need to create StoreStorageConfig abstract class and extend from that

  private boolean restoreDataPartitions = true;
  private boolean restoreMetadataPartition = true;

  public VeniceStoreConfig(String storeName, VeniceProperties storeProperties)
    throws ConfigurationException {
    super(storeProperties);
    this.storeName = storeName;

    // Stores all storage engine configs that are needed to be persisted to disk.
    this.persistStorageEngineConfigs = new PropertyBuilder()
        .put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, getRocksDBServerConfig().isRocksDBPlainTableFormatEnabled())
        .build();
  }

  public VeniceStoreConfig(String storeName, VeniceProperties storeProperties,
      PersistenceType storePersistenceType)
      throws ConfigurationException {
    this(storeName, storeProperties);
    this.storePersistenceType = Optional.of(storePersistenceType);
  }

  public String getStoreName() {
    return storeName;
  }

  public PersistenceType getStorePersistenceType() {
    if (!storePersistenceType.isPresent()) {
      throw new VeniceException("The persistence type of store: " + storeName + " is still unknown, something wrong happened");
    }
    return storePersistenceType.get();
  }

  public boolean isRestoreDataPartitions() {
    return restoreDataPartitions;
  }

  public void setRestoreDataPartitions(boolean restoreDataPartitions) {
    this.restoreDataPartitions = restoreDataPartitions;
  }

  public boolean isRestoreMetadataPartition() {
    return restoreMetadataPartition;
  }

  public void setRestoreMetadataPartition(boolean restoreMetadataPartition) {
    this.restoreMetadataPartition = restoreMetadataPartition;
  }

  /**
   * For some store, the persistence type may not be known when constructing {@link VeniceStoreConfig}, such as
   * in `VeniceStateModelFactory#createNewStateModel`, when Helix wants to create a new state model for some store,
   * it doesn't know the persistence type since it is possible that this store is an existing store, so the persistence
   * type is decided by the on-disk type, and it is a new store, it will be decided by storage node config.
   *
   * @return true, it means the storage persistence type is decided, and stay immutable during the lifetime of this store
   * in the same node.
   */
  public boolean isStorePersistenceTypeKnown() {
    return storePersistenceType.isPresent();
  }

  /**
   * This reason to create a `setter` for {@link #storePersistenceType} since it is unknown sometimes
   * when initializing {@link VeniceStoreConfig}, such as in {@link com.linkedin.venice.helix.VeniceStateModelFactory#createNewStateModel},
   * The new state model could be created for a brand-new store or an existing store.
   * @param storePersistenceType
   */
  public void setStorePersistenceType(PersistenceType storePersistenceType) {
    if (this.storePersistenceType.isPresent()) {
      if (this.storePersistenceType.get() == storePersistenceType) {
        // nothing changes.
        return;
      }
      throw new VeniceException("Store persistence type is not mutable, and the previous type: " +
          this.storePersistenceType.get() + " and new wanted type: " + storePersistenceType);
    }

    this.storePersistenceType = Optional.of(storePersistenceType);
  }

  public VeniceProperties getPersistStorageEngineConfig() {
    return persistStorageEngineConfigs;
  }
}
