package com.linkedin.davinci.store.rocksdb;

import com.linkedin.davinci.config.VeniceClusterConfig;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static com.linkedin.davinci.store.AbstractStorageEngine.*;
import static org.mockito.Mockito.*;


public class RocksDBStorageEngineTest extends AbstractStorageEngineTest {
  private static final int PARTITION_ID = 0;

  private String storeName;
  private StorageService storageService;
  private VeniceStoreConfig storeConfig;
  private VeniceClusterConfig clusterConfig;

  @Override
  public void createStorageEngineForTest() {
    storeName = TestUtils.getUniqueString("rocksdb_store_test");
    VeniceProperties serverProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    storageService = new StorageService(
        AbstractStorageEngineTest.getVeniceConfigLoader(serverProps),
        mock(AggVersionedStorageEngineStats.class),
        null,
        AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        mock(ReadOnlyStoreRepository.class));
    clusterConfig = new VeniceClusterConfig(serverProps);
    storeConfig = new VeniceStoreConfig(storeName, serverProps, PersistenceType.ROCKS_DB);
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
  public void testGetAndPut() {
    super.testGetAndPut();
  }

  @Test
  public void testGetByKeyPrefixManyKeys(){
    super.testGetByKeyPrefixManyKeys();
  }

  @Test
  public void testGetByKeyPrefixMaxSignedByte(){
    super.testGetByKeyPrefixMaxSignedByte();
  }

  @Test
  public void testGetByKeyPrefixMaxUnsignedByte(){
    super.testGetByKeyPrefixMaxUnsignedByte();
  }

  @Test
  public void testGetByKeyPrefixByteOverflow(){
    super.testGetByKeyPrefixByteOverflow();
  }

  @Test
  public void testGetAndPutPartitionOffset() {
    AbstractStorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine)testStorageEngine;
    OffsetRecord offsetRecord = new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    offsetRecord.setOffset(666L);
    rocksDBStorageEngine.putPartitionOffset(PARTITION_ID, offsetRecord);
    Assert.assertEquals(rocksDBStorageEngine.getPartitionOffset(PARTITION_ID).get().getOffset(), 666L);
    rocksDBStorageEngine.clearPartitionOffset(PARTITION_ID);
    Assert.assertEquals(rocksDBStorageEngine.getPartitionOffset(PARTITION_ID).isPresent(), false);
  }

  @Test
  public void testGetAndPutStoreVersionState() {
    AbstractStorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine)testStorageEngine;

    // Create a StoreVersionState record
    StoreVersionState storeVersionStateRecord = new StoreVersionState();
    storeVersionStateRecord.sorted = true;

    rocksDBStorageEngine.putStoreVersionState(storeVersionStateRecord);
    Assert.assertEquals(rocksDBStorageEngine.getStoreVersionState().get(), storeVersionStateRecord);

    // If no store version state is present in this metadata partition, Optional.empty() should be returned.
    rocksDBStorageEngine.clearStoreVersionState();
    Assert.assertEquals(rocksDBStorageEngine.getStoreVersionState(), Optional.empty());
  }

  @Test
  public void testIllegalPartitionIdInGetAndPutPartitionOffset() {
    AbstractStorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine)testStorageEngine;

    Assert.assertThrows(IllegalArgumentException.class,
        () -> rocksDBStorageEngine.putPartitionOffset(-1, new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer())));

    Assert.assertThrows(IllegalArgumentException.class,
        () -> rocksDBStorageEngine.getPartitionOffset(-1));
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
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine)testStorageEngine;
    Set<Integer> persistedPartitionIds = rocksDBStorageEngine.getPersistedPartitionIds();
    Assert.assertEquals(persistedPartitionIds.size(), 2);
    Assert.assertTrue(persistedPartitionIds.contains(PARTITION_ID));
    Assert.assertTrue(persistedPartitionIds.contains(METADATA_PARTITION_ID));
  }
}
