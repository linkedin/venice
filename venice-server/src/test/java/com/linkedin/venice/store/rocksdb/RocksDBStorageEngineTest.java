package com.linkedin.venice.store.rocksdb;

import com.linkedin.venice.config.VeniceClusterConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.stats.AggVersionedBdbStorageEngineStats;
import com.linkedin.venice.stats.AggVersionedStorageEngineStats;
import com.linkedin.venice.storage.BdbStorageMetadataService;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.AbstractStorageEngineTest;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;
import java.util.Set;

import static com.linkedin.venice.store.AbstractStorageEngine.*;
import static org.mockito.Mockito.*;


public class RocksDBStorageEngineTest extends AbstractStorageEngineTest {
  private static final int PARTITION_ID = 0;

  private StorageService storageService;
  private VeniceStoreConfig storeConfig;
  private VeniceClusterConfig clusterConfig;
  private String storeName;

  @Override
  public void createStorageEngineForTest() {
    storeName = TestUtils.getUniqueString("rocksdb_store_test");
    VeniceProperties serverProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceConfigLoader configLoader = AbstractStorageEngineTest.getVeniceConfigLoader(serverProps);
    storageService = new StorageService(
        configLoader,
        s -> {},
        mock(AggVersionedBdbStorageEngineStats.class),
        mock(AggVersionedStorageEngineStats.class),
        null);
    storeConfig = new VeniceStoreConfig(storeName, serverProps, PersistenceType.ROCKS_DB);
    testStoreEngine = storageService.openStoreForNewPartition(storeConfig , PARTITION_ID);
    VeniceProperties clusterProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.IN_MEMORY, 1000L);
    clusterConfig = new VeniceClusterConfig(clusterProps);
    createStoreForTest();
  }

  @BeforeClass
  public void setup() {
    createStorageEngineForTest();
  }

  @AfterClass
  public void tearDown() throws Exception {
    if(storageService != null && storeConfig != null) {
      storageService.dropStorePartition(storeConfig , PARTITION_ID);
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
        () -> rocksDBStorageEngine.putPartitionOffset(-1, new OffsetRecord()));

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
  public void testPutNullKey() {
    super.testPutNullKey();
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

  private BdbStorageMetadataService getOffsetManager(VeniceClusterConfig clusterConfig) {
    try {
      FileUtils.deleteDirectory(new File(clusterConfig.getOffsetDatabasePath()));
    } catch (Exception e) {
      Assert.fail("Unable to cleanup BDB directory", e);
    }
    BdbStorageMetadataService metadataService = new BdbStorageMetadataService(clusterConfig);
    metadataService.start();
    return metadataService;
  }

  @Test
  public void testBDBToRocksDBOffsetBootStrap() throws Exception {
    BdbStorageMetadataService metadataService = getOffsetManager(clusterConfig);
    try {
      AbstractStorageEngine testStorageEngine = getTestStoreEngine();
      String topic = testStorageEngine.getName();
      OffsetRecord expectedRecord = null, actualRecord = null;

      for (int j = 0; j < 2; j++) {
        expectedRecord = TestUtils.getOffsetRecord(j);
        metadataService.put(testStorageEngine.getName(), PARTITION_ID, expectedRecord);
      }
      StoreVersionState storeVersionState = new StoreVersionState();
      storeVersionState.sorted = true;
      metadataService.put(topic, storeVersionState);
      actualRecord = metadataService.getLastOffset(topic, PARTITION_ID);
      storeVersionState = metadataService.getStoreVersionState(topic).get();
      testStoreEngine.bootStrapAndValidateOffsetRecordsFromBDB(metadataService);

      Optional<OffsetRecord> record = testStoreEngine.getPartitionOffset(PARTITION_ID);
      Assert.assertEquals(record.get(), actualRecord);

      Optional<StoreVersionState> actualState = testStoreEngine.getStoreVersionState();
      Assert.assertEquals(actualState.get(), storeVersionState);

      // after validation BDB should be empty
      Assert.assertFalse(metadataService.getStoreVersionState(topic).isPresent());

      // calling it again will not bootstrap and validate again.
      storeVersionState.sorted = false;
      metadataService.put(topic, storeVersionState);
      testStoreEngine.bootStrapAndValidateOffsetRecordsFromBDB(metadataService);
    } finally {
      metadataService.stop();
    }
  }
}
