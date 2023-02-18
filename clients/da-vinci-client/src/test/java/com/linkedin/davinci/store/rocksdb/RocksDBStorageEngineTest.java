package com.linkedin.davinci.store.rocksdb;

import static com.linkedin.davinci.store.AbstractStorageEngine.METADATA_PARTITION_ID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RocksDBStorageEngineTest extends AbstractStorageEngineTest {
  private static final int PARTITION_ID = 0;
  private StorageService storageService;
  private VeniceStoreVersionConfig storeConfig;
  private static final String storeName = Utils.getUniqueString("rocksdb_store_test");
  private final ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
  private static final int versionNumber = 0;
  private static final String topicName = Version.composeKafkaTopic(storeName, versionNumber);

  @Override
  public void createStorageEngineForTest() {
    Version mockVersion = mock(Version.class);
    when(mockVersion.isActiveActiveReplicationEnabled()).thenReturn(false);
    Store mockStore = mock(Store.class);
    when(mockStore.getVersion(versionNumber)).thenReturn(Optional.of(mockVersion));
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
    testStoreEngine = storageService.openStoreForNewPartition(storeConfig, PARTITION_ID, () -> null);
    createStoreForTest();
  }

  @BeforeClass
  public void setUp() {
    createStorageEngineForTest();
  }

  @AfterClass
  public void cleanUp() throws Exception {
    storageService.dropStorePartition(storeConfig, PARTITION_ID);
    storageService.stop();
  }

  @Test
  public void testGetAndPut() {
    super.testGetAndPut();
  }

  @Test
  public void testGetByKeyPrefixManyKeys() {
    super.testGetByKeyPrefixManyKeys();
  }

  @Test
  public void testGetByKeyPrefixMaxSignedByte() {
    super.testGetByKeyPrefixMaxSignedByte();
  }

  @Test
  public void testGetByKeyPrefixMaxUnsignedByte() {
    super.testGetByKeyPrefixMaxUnsignedByte();
  }

  @Test
  public void testGetByKeyPrefixByteOverflow() {
    super.testGetByKeyPrefixByteOverflow();
  }

  @Test
  public void testGetAndPutPartitionOffset() {
    AbstractStorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine) testStorageEngine;
    OffsetRecord offsetRecord = new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    offsetRecord.setCheckpointLocalVersionTopicOffset(666L);
    rocksDBStorageEngine.putPartitionOffset(PARTITION_ID, offsetRecord);
    Assert.assertEquals(rocksDBStorageEngine.getPartitionOffset(PARTITION_ID).get().getLocalVersionTopicOffset(), 666L);
    rocksDBStorageEngine.clearPartitionOffset(PARTITION_ID);
    Assert.assertEquals(rocksDBStorageEngine.getPartitionOffset(PARTITION_ID).isPresent(), false);
  }

  @Test
  public void testGetAndPutStoreVersionState() {
    AbstractStorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine) testStorageEngine;

    // Create a StoreVersionState record
    StoreVersionState storeVersionStateRecord = new StoreVersionState();
    storeVersionStateRecord.sorted = true;

    rocksDBStorageEngine.putStoreVersionState(storeVersionStateRecord);
    Assert.assertEquals(rocksDBStorageEngine.getStoreVersionState(), storeVersionStateRecord);

    // If no store version state is present in this metadata partition, null should be returned.
    rocksDBStorageEngine.clearStoreVersionState();
    Assert.assertNull(rocksDBStorageEngine.getStoreVersionState());
  }

  @Test
  public void testIllegalPartitionIdInGetAndPutPartitionOffset() {
    AbstractStorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine) testStorageEngine;

    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> rocksDBStorageEngine
            .putPartitionOffset(-1, new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer())));

    Assert.assertThrows(IllegalArgumentException.class, () -> rocksDBStorageEngine.getPartitionOffset(-1));
  }

  @Test
  public void testDelete() {
    super.testDelete();
  }

  @Test
  public void testUpdate() {
    super.testUpdate();
  }

  @Test
  public void testGetInvalidKeys() {
    super.testGetInvalidKeys();
  }

  @Test
  public void testPartitioning() throws Exception {
    super.testPartitioning();
  }

  @Test
  public void testAddingAPartitionTwice() throws Exception {
    super.testAddingAPartitionTwice();
  }

  @Test
  public void testRemovingPartitionTwice() throws Exception {
    super.testRemovingPartitionTwice();
  }

  @Test
  public void testOperationsOnNonExistingPartition() throws Exception {
    super.testOperationsOnNonExistingPartition();
  }

  @Test
  public void testGetPersistedPartitionIds() {
    AbstractStorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine) testStorageEngine;
    Set<Integer> persistedPartitionIds = rocksDBStorageEngine.getPersistedPartitionIds();
    Assert.assertEquals(persistedPartitionIds.size(), 2);
    Assert.assertTrue(persistedPartitionIds.contains(PARTITION_ID));
    Assert.assertTrue(persistedPartitionIds.contains(METADATA_PARTITION_ID));
  }

  @Test
  public void testRocksDBStoragePartitionType() {
    // Verify that data partition is created as regular RocksDB partition, not a RMD-RocksDB Partition.
    Assert.assertFalse(
        testStoreEngine.getPartitionOrThrow(PARTITION_ID) instanceof ReplicationMetadataRocksDBStoragePartition);
    Assert.assertTrue(testStoreEngine.getPartitionOrThrow(PARTITION_ID) instanceof RocksDBStoragePartition);
    // Verify that metadata partition is created as regular RocksDB partition, not a RMD-RocksDB Partition.
    Assert.assertFalse(testStoreEngine.getMetadataPartition() instanceof ReplicationMetadataRocksDBStoragePartition);
    Assert.assertTrue(testStoreEngine.getMetadataPartition() instanceof RocksDBStoragePartition);
  }
}
