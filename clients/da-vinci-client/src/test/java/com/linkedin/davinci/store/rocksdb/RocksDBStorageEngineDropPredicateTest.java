package com.linkedin.davinci.store.rocksdb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.StorageEngineAccessor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for {@link RocksDBStorageEngine#shouldDropPartitionOnRestoreFailure(int, Throwable)}.
 * Confirms that only Status.Code.Corruption and Status.Code.IOError with a "No such file or directory" status
 * trigger a drop. Environmental failures (NoSpace, lock contention, generic IOError, non-RocksDBException) must not.
 */
public class RocksDBStorageEngineDropPredicateTest {
  private static final int PARTITION_ID = 0;
  private StorageService storageService;
  private VeniceStoreVersionConfig storeConfig;
  private RocksDBStorageEngine engine;
  private final ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
  private static final String storeName = Utils.getUniqueString("rocksdb_drop_predicate_test");
  private static final int versionNumber = 0;
  private static final String topicName = Version.composeKafkaTopic(storeName, versionNumber);

  @BeforeClass
  public void setUp() {
    Version mockVersion = mock(Version.class);
    when(mockVersion.isActiveActiveReplicationEnabled()).thenReturn(false);
    Store mockStore = mock(Store.class);
    when(mockStore.getVersion(versionNumber)).thenReturn(mockVersion);
    when(mockReadOnlyStoreRepository.getStoreOrThrow(storeName)).thenReturn(mockStore);

    VeniceProperties serverProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    storageService = new StorageService(
        AbstractStorageEngineTest.getVeniceConfigLoader(serverProps),
        mock(AggVersionedStorageEngineStats.class),
        null,
        AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        mockReadOnlyStoreRepository);
    storeConfig = new VeniceStoreVersionConfig(topicName, serverProps, PersistenceType.ROCKS_DB);
    engine = (RocksDBStorageEngine) StorageEngineAccessor
        .getInnerStorageEngine(storageService.openStoreForNewPartition(storeConfig, PARTITION_ID, () -> null));
  }

  @AfterClass
  public void cleanUp() throws Exception {
    storageService.dropStorePartition(storeConfig, PARTITION_ID);
    storageService.stop();
  }

  @Test
  public void testDropOnCorruption() {
    RocksDBException rocksDBException =
        new RocksDBException("simulated", new Status(Status.Code.Corruption, Status.SubCode.None, "boom"));
    Throwable wrapped = new VeniceException("Failed to open RocksDB for replica: 0", rocksDBException);
    Assert.assertTrue(engine.shouldDropPartitionOnRestoreFailure(PARTITION_ID, wrapped));
  }

  @Test
  public void testDropOnIOErrorNoSuchFile() {
    RocksDBException rocksDBException = new RocksDBException(
        "IO error: No such file or directory: /path/to/CURRENT",
        new Status(Status.Code.IOError, Status.SubCode.None, "No such file or directory"));
    Throwable wrapped = new VeniceException("Failed to open RocksDB for replica: 0", rocksDBException);
    Assert.assertTrue(engine.shouldDropPartitionOnRestoreFailure(PARTITION_ID, wrapped));
  }

  @Test
  public void testDropOnIOErrorNoSuchFileViaMessageOnly() {
    RocksDBException rocksDBException = new RocksDBException("IO error: No such file or directory: /path/to/CURRENT");
    Throwable wrapped = new VeniceException("Failed to open RocksDB for replica: 0", rocksDBException);
    // Without a Status, the predicate has no Code to inspect, so this must not drop. Confirms we
    // never drop on message text alone — Status.Code.IOError gating is required.
    Assert.assertFalse(engine.shouldDropPartitionOnRestoreFailure(PARTITION_ID, wrapped));
  }

  @Test
  public void testNoDropOnIOErrorNoSpace() {
    RocksDBException rocksDBException =
        new RocksDBException("disk full", new Status(Status.Code.IOError, Status.SubCode.NoSpace, "ENOSPC"));
    Throwable wrapped = new VeniceException("Failed to open RocksDB for replica: 0", rocksDBException);
    Assert.assertFalse(engine.shouldDropPartitionOnRestoreFailure(PARTITION_ID, wrapped));
  }

  @Test
  public void testNoDropOnGenericIOError() {
    RocksDBException rocksDBException =
        new RocksDBException("io", new Status(Status.Code.IOError, Status.SubCode.None, "EIO"));
    Throwable wrapped = new VeniceException("Failed to open RocksDB for replica: 0", rocksDBException);
    Assert.assertFalse(engine.shouldDropPartitionOnRestoreFailure(PARTITION_ID, wrapped));
  }

  @Test
  public void testNoDropOnLockBusy() {
    RocksDBException rocksDBException =
        new RocksDBException("locked", new Status(Status.Code.Busy, Status.SubCode.LockLimit, "lock held"));
    Throwable wrapped = new VeniceException("Failed to open RocksDB for replica: 0", rocksDBException);
    Assert.assertFalse(engine.shouldDropPartitionOnRestoreFailure(PARTITION_ID, wrapped));
  }

  @Test
  public void testNoDropOnNonRocksDBException() {
    Throwable nonRocks = new RuntimeException("unrelated boom");
    Assert.assertFalse(engine.shouldDropPartitionOnRestoreFailure(PARTITION_ID, nonRocks));
  }

  @Test
  public void testFindsRocksDBExceptionThroughCauseChain() {
    RocksDBException rocksDBException =
        new RocksDBException("nested", new Status(Status.Code.Corruption, Status.SubCode.None, "boom"));
    Throwable level2 = new VeniceException("Failed to open RocksDB for replica: 0", rocksDBException);
    Throwable level1 = new RuntimeException("outer wrapper", level2);
    Assert.assertTrue(engine.shouldDropPartitionOnRestoreFailure(PARTITION_ID, level1));
  }
}
