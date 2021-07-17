package com.linkedin.davinci.store.rocksdb;

import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TimestampMetadataRocksDBStorageEngineTest extends AbstractStorageEngineTest {
  private static final int PARTITION_ID = 0;

  private final String storeName = TestUtils.getUniqueString("rocksdb_store_test");
  private final ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
  private final int versionNumber = 0;
  private final String topicName = Version.composeKafkaTopic(storeName, versionNumber);

  private StorageService storageService;
  private VeniceStoreConfig storeConfig;

  @Override
  public void createStorageEngineForTest() {
    Version mockVersion = mock(Version.class);
    when(mockVersion.isActiveActiveReplicationEnabled()).thenReturn(true);
    Store mockStore = mock(Store.class);
    when(mockStore.getVersion(versionNumber)).thenReturn(Optional.of(mockVersion));
    when(mockReadOnlyStoreRepository.hasStore(storeName)).thenReturn(true);
    when(mockReadOnlyStoreRepository.getStoreOrThrow(storeName)).thenReturn(mockStore);

    VeniceProperties serverProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    storageService = new StorageService(
        AbstractStorageEngineTest.getVeniceConfigLoader(serverProps),
        mock(AggVersionedStorageEngineStats.class),
        null,
        AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        mockReadOnlyStoreRepository);
    storeConfig = new VeniceStoreConfig(topicName, serverProps, PersistenceType.ROCKS_DB);
    testStoreEngine = storageService.openStoreForNewPartition(storeConfig , PARTITION_ID);
    createStoreForTest();
  }

  @BeforeClass
  public void setup() {
    createStorageEngineForTest();
  }

  @AfterClass
  public void tearDown() throws Exception {
    storageService.dropStorePartition(storeConfig , PARTITION_ID);
    storageService.stop();
  }
  @Test
  public void testUseTimestampMetadataRocksDBStoragePartition() {
    // Verify that data partition is created as TSMD-RocksDB Partition.
    Assert.assertTrue(testStoreEngine.getPartitionOrThrow(PARTITION_ID) instanceof TimestampMetadataRocksDBStoragePartition);
    // Verify that metadata partition is not create as TSMD-RocksDB Partition
    Assert.assertFalse(testStoreEngine.getMetadataPartition() instanceof TimestampMetadataRocksDBStoragePartition);
  }
}
