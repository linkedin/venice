package com.linkedin.venice.storage;

import com.linkedin.venice.config.VeniceClusterConfig;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.store.AbstractStorageEngineTest;
import com.linkedin.venice.utils.RandomGenUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class BdbStorageMetadataServiceTest {

  private VeniceClusterConfig clusterConfig;
  // BDB allowed minimum wakeup interval is 1 second
  private Long flushIntervalMs = 1001L;
  private BdbStorageMetadataService storageMetadataService;

  private int partitionId = 3;

  private BdbStorageMetadataService getOffsetManager(VeniceClusterConfig clusterConfig) throws Exception {
    BdbStorageMetadataService offsetManager = new BdbStorageMetadataService(clusterConfig);
    offsetManager.start();
    return offsetManager;
  }

  @BeforeClass
  private void init() throws Exception {

    VeniceProperties clusterProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.IN_MEMORY,
        flushIntervalMs);

    clusterConfig = new VeniceClusterConfig(clusterProps);
    storageMetadataService = getOffsetManager(clusterConfig);
  }

  @Test
  public void testFreshnessAfterRestart() throws Exception {
    final String TOPIC = TestUtils.getUniqueString("topic");
    OffsetRecord expectedRecord = null, actualRecord =  null;

    for(int i = 0; i < 10; i ++){
      //Write 10 times randomly to the offset Store and verify the read.
      for(int j = 0; j < 10; j ++) {
        long lastOffset = RandomGenUtils.getRandomIntWithIn(Integer.MAX_VALUE);

        expectedRecord = TestUtils.getOffsetRecord(lastOffset);
        storageMetadataService.put(TOPIC, partitionId, expectedRecord);
        actualRecord = storageMetadataService.getLastOffset(TOPIC, partitionId);
        Assert.assertEquals(expectedRecord, actualRecord, "Offset Manager returned different record");
      }

      storageMetadataService.stop();

      try {
        storageMetadataService.getLastOffset(TOPIC, partitionId);
        Assert.fail("stopped offset manager should throw IllegalStateException");
      } catch(IllegalStateException ex) {
        //Expected
      }

      storageMetadataService = getOffsetManager(clusterConfig);
      actualRecord = storageMetadataService.getLastOffset(TOPIC, partitionId);
      Assert.assertEquals(expectedRecord, actualRecord, "Offset Manager does not persist across restarts");

    }
  }

  @Test
  public void testCRUDforOffsetRecord() {
    final String TOPIC = TestUtils.getUniqueString("topic");
    OffsetRecord actualRecord = storageMetadataService.getLastOffset(TOPIC, partitionId);
    Assert.assertEquals(new OffsetRecord(), actualRecord, "NonExistentTopic should return non existent offset");

    // Create
    long offset = RandomGenUtils.getRandomIntWithIn(Integer.MAX_VALUE);
    OffsetRecord expectedRecord = TestUtils.getOffsetRecord(offset);
    storageMetadataService.put(TOPIC, partitionId, expectedRecord);

    try {
      storageMetadataService.put(TOPIC, -1, expectedRecord);
      Assert.fail("The StorageMetadataService should refuse to store OffsetRecords with partitionId < 0");
    } catch (IllegalArgumentException e) {
      // Expected
    }

    //Read
    actualRecord = storageMetadataService.getLastOffset(TOPIC, partitionId);
    Assert.assertEquals(expectedRecord , actualRecord, "Offset Manager returned different record");

    // Update
    offset = RandomGenUtils.getRandomIntWithIn(Integer.MAX_VALUE);
    expectedRecord = TestUtils.getOffsetRecord(offset);
    storageMetadataService.put(TOPIC, partitionId, expectedRecord);

    actualRecord = storageMetadataService.getLastOffset(TOPIC, partitionId);
    Assert.assertEquals(expectedRecord, actualRecord, "Offset Manager returned different record");

    //Delete
    storageMetadataService.clearOffset(TOPIC , partitionId);
    actualRecord = storageMetadataService.getLastOffset(TOPIC, partitionId);
    Assert.assertEquals(new OffsetRecord() ,actualRecord , "cleared offset should return non existent offset" );
  }

  @Test
  public void testIllegalPartitionIdInOffsetManager() {
    final String TOPIC = TestUtils.getUniqueString("topic");
    try {
      storageMetadataService.put(TOPIC, -1, new OffsetRecord());
      Assert.fail("The StorageMetadataService should refuse to store OffsetRecords with partitionId < 0");
    } catch (IllegalArgumentException e) {
      // Expected
    }

    try {
      storageMetadataService.getLastOffset(TOPIC, -1);
      Assert.fail("The StorageMetadataService should refuse to retrieve OffsetRecords with partitionId < 0");
    } catch (IllegalArgumentException e) {
      // Expected
    }

    try {
      storageMetadataService.clearOffset(TOPIC, -1);
      Assert.fail("The StorageMetadataService should refuse to delete OffsetRecords with partitionId < 0");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testCRUDforStoreVersionState() {
    final String TOPIC = TestUtils.getUniqueString("topic");
    StoreVersionState actualRecord = storageMetadataService.getStoreVersionState(TOPIC);
    Assert.assertEquals(null, actualRecord, "NonExistentTopic should return a null StoreVersionState");

    // Create
    StoreVersionState expectedRecord = new StoreVersionState();
    expectedRecord.sorted = true;
    storageMetadataService.put(TOPIC, expectedRecord);

    //Read
    actualRecord = storageMetadataService.getStoreVersionState(TOPIC);
    Assert.assertEquals(actualRecord, expectedRecord, "StorageMetadataService returned a different record");

    // Update
    expectedRecord = new StoreVersionState();
    expectedRecord.sorted = false;
    storageMetadataService.put(TOPIC, expectedRecord);

    actualRecord = storageMetadataService.getStoreVersionState(TOPIC);
    Assert.assertEquals(actualRecord, expectedRecord, "StorageMetadataService returned a different record");

    //Delete
    storageMetadataService.clearStoreVersionState(TOPIC);
    actualRecord = storageMetadataService.getStoreVersionState(TOPIC);
    Assert.assertEquals(actualRecord, null, "cleared topic should return null");
  }

}
