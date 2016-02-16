package com.linkedin.venice.kafka;

import com.linkedin.venice.utils.RandomGenUtils;
import com.linkedin.venice.config.VeniceClusterConfig;
import com.linkedin.venice.offsets.BdbOffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.utils.Props;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestBDBOffsetManager {

  private VeniceClusterConfig clusterConfig;
  private Long flushIntervalMs = 4000L;
  private BdbOffsetManager offsetManager;
  private String topicName = "test_topic";
  private int partitionId = 3;

  private BdbOffsetManager getOffsetManager(VeniceClusterConfig clusterConfig) throws Exception {
    BdbOffsetManager offsetManager = new BdbOffsetManager(clusterConfig);
    offsetManager.start();
    return offsetManager;
  }

  @BeforeClass
  private void init() throws Exception {

    Props clusterProps = new Props();
    clusterProps.put(VeniceConfigService.CLUSTER_NAME, "test_offset_manager");
    clusterProps.put(VeniceConfigService.ENABLE_KAFKA_CONSUMER_OFFSET_MANAGEMENT, "true");
    clusterProps.put(VeniceConfigService.OFFSET_MANAGER_TYPE, "bdb");
    clusterProps.put(VeniceConfigService.OFFSET_MANAGER_FLUSH_INTERVAL_MS, flushIntervalMs);
    clusterProps.put(VeniceConfigService.HELIX_ENABLED, "false");
    clusterProps.put(VeniceConfigService.ZOOKEEPER_ADDRESS, "localhost:2181");
    clusterConfig = new VeniceClusterConfig(clusterProps);
    offsetManager = getOffsetManager(clusterConfig);
  }

  @Test
  public void testFreshnessAfterRestart()
      throws Exception {
    /**
     * 1. Start a thread/loop that constantly produces to the same topic,partition for more than flushIntervalMs time
     * 2. Note the last update and stop the thread/loop and the offset manager.
     * 3. Get the last record from offsetManager
     * 4. Match the time stamp between the one fetched and the one noted down in step 2. The time difference should not
     * be greater than flushIntervalMs
     */

    long start = System.currentTimeMillis();
    long end = start + (flushIntervalMs * 5);
    long lastOffset = -1;
    long lastOffsetTimeStamp = 0L;

    while (System.currentTimeMillis() < end) {
      lastOffset = RandomGenUtils.getRandomIntInRange(0, 9999);
      lastOffsetTimeStamp = System.currentTimeMillis();

      OffsetRecord record = new OffsetRecord(lastOffset, lastOffsetTimeStamp);

      offsetManager.recordOffset(topicName, partitionId, record);
      Thread.sleep(100);
    }
    offsetManager.stop();
    offsetManager = getOffsetManager(clusterConfig);
    OffsetRecord record = offsetManager.getLastOffset(topicName, partitionId);
    long timeGap = lastOffsetTimeStamp - record.getEventTimeEpochMs();
    if (timeGap < 0 && timeGap > flushIntervalMs) {
      Assert.fail(
          "The last offset fetched from OffsetManager: " + record.getEventTimeEpochMs() + ", is staler (by " + timeGap
              + "ms) than the last emitted offset:  " + lastOffset);
    }
  }
}
