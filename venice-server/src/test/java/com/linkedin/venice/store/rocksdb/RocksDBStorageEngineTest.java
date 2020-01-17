package com.linkedin.venice.store.rocksdb;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.stats.AggVersionedBdbStorageEngineStats;
import com.linkedin.venice.stats.AggVersionedStorageEngineStats;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.AbstractStorageEngineTest;
import com.linkedin.venice.store.StoragePartitionConfig;
import com.linkedin.venice.utils.RandomGenUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.math.BigInteger;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class RocksDBStorageEngineTest extends AbstractStorageEngineTest {
  private StorageService service;
  private VeniceStoreConfig storeConfig;

  private String storeName;
  private static final int PARTITION_ID = 0;
  private static final int STORING_METADATA_PARTITION_ID = 1;

  @Override
  public void createStorageEngineForTest() {
    storeName = TestUtils.getUniqueString("rocksdb_store_test");
    VeniceProperties serverProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceConfigLoader configLoader = AbstractStorageEngineTest.getVeniceConfigLoader(serverProperties);
    service = new StorageService(configLoader, s -> s.toString(), mock(AggVersionedBdbStorageEngineStats.class),
        mock(AggVersionedStorageEngineStats.class));
    storeConfig = new VeniceStoreConfig(storeName, serverProperties, PersistenceType.ROCKS_DB);
    testStoreEngine = service.openStoreForNewPartition(storeConfig , PARTITION_ID);
    createStoreForTest();
  }

  @BeforeClass
  public void setup() {
    createStorageEngineForTest();
  }

  @AfterClass
  public void tearDown() {
    if(service != null && storeConfig != null) {
      service.dropStorePartition(storeConfig , PARTITION_ID);
    }
  }

  @Test
  public void testGetAndPut() {
    super.testGetAndPut();
  }

  @Test
  public void testGetAndPutPartitionOffset() {
    AbstractStorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine)testStorageEngine;
    OffsetRecord offsetRecord = new OffsetRecord();
    offsetRecord.setOffset(666L);
    StoragePartitionConfig storagePartitionConfig = new StoragePartitionConfig(storeName, STORING_METADATA_PARTITION_ID);
    // storingMetadata is set to false by default, so we need to change it to true
    storagePartitionConfig.setStoringMetadata(true);
    testStorageEngine.addStoragePartition(storagePartitionConfig);
    rocksDBStorageEngine.putPartitionOffset(STORING_METADATA_PARTITION_ID, offsetRecord);
    Assert.assertEquals(rocksDBStorageEngine.getPartitionOffset(STORING_METADATA_PARTITION_ID).getOffset(), 666L);

    // test and make sure rocksDBStorageEngine can store and retrieve data
    // with the same key but different column family correctly
    byte[] key = BigInteger.valueOf(STORING_METADATA_PARTITION_ID).toByteArray();
    byte[] value = RandomGenUtils.getRandomBytes(20);
    rocksDBStorageEngine.put(STORING_METADATA_PARTITION_ID, key, value);
    Assert.assertEquals(rocksDBStorageEngine.get(STORING_METADATA_PARTITION_ID, key), value);
    Assert.assertEquals(rocksDBStorageEngine.getPartitionOffset(STORING_METADATA_PARTITION_ID).getOffset(), 666L);

    testStorageEngine.dropPartition(STORING_METADATA_PARTITION_ID);
  }

  @Test
  public void testIllegalPartitionIdInGetAndPutPartitionOffset() {
    AbstractStorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine)testStorageEngine;

    Assert.assertThrows(IllegalArgumentException.class,
        () -> rocksDBStorageEngine.putPartitionOffset(-1, new OffsetRecord()));

    Assert.assertThrows(IllegalArgumentException.class,
        () -> rocksDBStorageEngine.getPartitionOffset(-1));
  }

  @Test
  public void testIllegalGetAndPutPartitionOffsetWhenNotStoringMetadata() {
    AbstractStorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine)testStorageEngine;

    // storingMetadata is set to false by default, so we should not call get/putPartitionOffset
    Assert.assertThrows(VeniceException.class,
        () -> rocksDBStorageEngine.getPartitionOffset(PARTITION_ID));
    Assert.assertThrows(VeniceException.class,
        () -> rocksDBStorageEngine.putPartitionOffset(PARTITION_ID, new OffsetRecord()));
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
  public void testPutNullKey() {
    super.testPutNullKey();
  }

  @Test
  public void testPartitioning()
      throws Exception {
    super.testPartitioning();
  }

  @Test
  public void testAddingAPartitionTwice()
      throws Exception {
    super.testAddingAPartitionTwice();
  }

  @Test
  public void testRemovingPartitionTwice()
      throws Exception {
    super.testRemovingPartitionTwice();
  }

  @Test
  public void testOperationsOnNonExistingPartition()
      throws Exception {
    super.testOperationsOnNonExistingPartition();
  }

  @Test
  public void testGetPersistedPartitionIds() {
    AbstractStorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine)testStorageEngine;
    Set<Integer> persistedPartitionIds = rocksDBStorageEngine.getPersistedPartitionIds();
    Assert.assertEquals(persistedPartitionIds.size(), 1);
    Assert.assertTrue(persistedPartitionIds.contains(PARTITION_ID));
  }
}
