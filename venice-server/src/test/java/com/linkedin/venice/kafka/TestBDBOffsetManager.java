package com.linkedin.venice.kafka;

import com.linkedin.venice.config.VeniceClusterConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.offsets.BdbOffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.storage.AbstractStorageEngineTest;
import com.linkedin.venice.utils.RandomGenUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestBDBOffsetManager {

  private VeniceClusterConfig clusterConfig;
  // BDB allowed minimum wakeup interval is 1 second
  private Long flushIntervalMs = 1001L;
  private BdbOffsetManager offsetManager;

  private int partitionId = 3;

  private BdbOffsetManager getOffsetManager(VeniceClusterConfig clusterConfig) throws Exception {
    BdbOffsetManager offsetManager = new BdbOffsetManager(clusterConfig);
    offsetManager.start();
    return offsetManager;
  }

  @BeforeClass
  private void init() throws Exception {

    VeniceProperties clusterProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.IN_MEMORY,
        flushIntervalMs);

    clusterConfig = new VeniceClusterConfig(clusterProps);
    offsetManager = getOffsetManager(clusterConfig);
  }

  @Test
  public void testFreshnessAfterRestart()
      throws Exception {

    String topicName = "test_topic";
    OffsetRecord expectedRecord = null, actualRecord =  null;

    for(int i = 0; i < 10; i ++){
      //Write 10 times randomly to the offset Store and verify the read.
      for(int j = 0; j < 10; j ++) {
        long lastOffset = RandomGenUtils.getRandomIntWithIn(Integer.MAX_VALUE);

        expectedRecord = TestUtils.getOffsetRecord(lastOffset);
        offsetManager.recordOffset(topicName, partitionId, expectedRecord);
        actualRecord = offsetManager.getLastOffset(topicName, partitionId);
        Assert.assertEquals(expectedRecord, actualRecord, "Offset Manager returned different record");
      }

      offsetManager.stop();

      try {
        offsetManager.getLastOffset(topicName, partitionId);
        Assert.fail("stopped offset manager should throw IllegalStateException");
      } catch(IllegalStateException ex) {
        //Expected
      }

      offsetManager = getOffsetManager(clusterConfig);
      actualRecord = offsetManager.getLastOffset(topicName, partitionId);
      Assert.assertEquals(expectedRecord, actualRecord, "Offset Manager does not persist across restarts");

    }
  }

  @Test
  public void testCRUD() {
    final String NON_EXISTENT_OFFSET_TOPIC =  "NonExistentOffsetTopic";
    OffsetRecord actualRecord = offsetManager.getLastOffset(NON_EXISTENT_OFFSET_TOPIC, partitionId);
    Assert.assertEquals(new OffsetRecord() ,actualRecord , "NonExistentTopic should return non existent offset" );

    // Create
    long offset = RandomGenUtils.getRandomIntWithIn(Integer.MAX_VALUE);
    OffsetRecord expectedRecord = TestUtils.getOffsetRecord(offset);
    offsetManager.recordOffset(NON_EXISTENT_OFFSET_TOPIC, partitionId, expectedRecord);

    //Read
    actualRecord = offsetManager.getLastOffset(NON_EXISTENT_OFFSET_TOPIC, partitionId);
    Assert.assertEquals(expectedRecord , actualRecord, "Offset Manager returned different record");

    // Update
    offset = RandomGenUtils.getRandomIntWithIn(Integer.MAX_VALUE);
    expectedRecord = TestUtils.getOffsetRecord(offset);
    offsetManager.recordOffset(NON_EXISTENT_OFFSET_TOPIC, partitionId, expectedRecord);

    actualRecord = offsetManager.getLastOffset(NON_EXISTENT_OFFSET_TOPIC, partitionId);
    Assert.assertEquals(expectedRecord, actualRecord, "Offset Manager returned different record");

    //Delete
    offsetManager.clearOffset(NON_EXISTENT_OFFSET_TOPIC , partitionId);
    actualRecord = offsetManager.getLastOffset(NON_EXISTENT_OFFSET_TOPIC, partitionId);
    Assert.assertEquals(new OffsetRecord() ,actualRecord , "cleared offset should return non existent offset" );
  }
}
