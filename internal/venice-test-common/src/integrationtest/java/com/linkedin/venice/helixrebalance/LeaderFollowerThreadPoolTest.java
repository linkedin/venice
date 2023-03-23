package com.linkedin.venice.helixrebalance;

import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModelFactory;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LeaderFollowerThreadPoolTest {
  private static final Logger LOGGER = LogManager.getLogger(LeaderFollowerThreadPoolTest.class);
  private VeniceClusterWrapper cluster;
  private VeniceServerWrapper server0;
  private VeniceServerWrapper server1;
  int replicaFactor = 2;
  int partitionSize = 1000;
  int partitionNum = 1;
  private Lock lock = new ReentrantLock();
  private Condition condition = lock.newCondition();
  private boolean isBlockingTaskStarted = false;
  private String storeName;

  @BeforeMethod
  public void setUp() {
    int numOfController = 1;
    int numOfServers = 0;
    int numOfRouters = 1;
    cluster = ServiceFactory
        .getVeniceCluster(numOfController, numOfServers, numOfRouters, replicaFactor, partitionSize, false, false);
  }

  @AfterMethod
  public void cleanUp() {
    cluster.close();
  }

  /**
   * testLeaderFollowerDualThreadPool test does the following steps:
   *
   * 1.  Create a Venice cluster with 1 controller, 1 router, and 2 servers (dual pool strategy).
   * 2.  Create a new store and push data.
   * 3.  Wait for the push job to complete successfully.
   * 4.  Create a blocking task to block server_1's thread pool.
   * 5.  Create a new version and push data.
   * 6.  Wait for the push job to complete successfully.
   *
   * Notice that because the test is running blocking test for the thread pool.
   * It's better to destroy the testbed between each test runs.
   */

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testLeaderFollowerDualThreadPool() throws Exception {
    commonTestProcedures(true);

    // Start a new version push.
    String topicNameV2 = createVersionAndPushData(storeName);

    // New version can complete successfully (it will not complete without having a separate thread pool for future
    // version)
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        true,
        () -> Assert.assertEquals(
            cluster.getLeaderVeniceController()
                .getVeniceAdmin()
                .getOffLinePushStatus(cluster.getClusterName(), topicNameV2)
                .getExecutionStatus(),
            ExecutionStatus.COMPLETED));
  }

  /**
   * testLeaderFollowerSingleThreadPool test does the following steps:
   *
   * 1.  Create a Venice cluster with 1 controller, 1 router, and 2 servers (single thread pool).
   * 2.  Create a new store and push data.
   * 3.  Wait for the push job to complete successfully.
   * 4.  Create a blocking task to block server_1's thread pool.
   * 5.  Create another version and push data.
   * 6.  Assert that the second version cannot be completed and expect a Venice Error.
   */
  @Test(timeOut = 150 * Time.MS_PER_SECOND)
  public void testLeaderFollowerSingleThreadPool() throws Exception {
    commonTestProcedures(false);

    // Start a new version push and expect it to fail with exception.
    try {
      createVersionAndPushData(storeName);
      Assert.fail("new version creation should have failed.");
    } catch (AssertionError e) {
      Assert.assertTrue(e.getMessage().contains("does not have enough replicas"));
    }
  }

  private void commonTestProcedures(boolean isDualPoolEnabled) throws InterruptedException {
    setUpServers(isDualPoolEnabled);

    storeName =
        Utils.getUniqueString("testLeaderFollowerThreadPools_" + (isDualPoolEnabled ? "DualPool" : "SinglePool"));
    long storageQuota = (long) partitionNum * partitionSize;

    cluster.getNewStore(storeName);
    cluster.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));

    // Create a version.
    String topicNameV1 = createVersionAndPushData(storeName);

    // Wait until push is completed successfully.
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        true,
        () -> Assert.assertEquals(
            cluster.getLeaderVeniceController()
                .getVeniceAdmin()
                .getOffLinePushStatus(cluster.getClusterName(), topicNameV1)
                .getExecutionStatus(),
            ExecutionStatus.COMPLETED));

    // Running a blocking task to occupy all threads in thread pool for current && backup versions.
    ExecutorService leaderFollowerPool =
        server0.getVeniceServer().getHelixParticipationService().getLeaderFollowerHelixStateTransitionThreadPool();

    leaderFollowerPool.submit(() -> {
      LOGGER.info("blocking task is running...");
      lock.lock();
      try {
        isBlockingTaskStarted = true;
        this.condition.signal();
      } finally {
        lock.unlock();
      }

      try {
        // Blocking the thread pool long enough for the test.
        Thread.sleep(120 * Time.MS_PER_SECOND);
      } catch (InterruptedException e) {
        LOGGER.warn(e.getMessage());
      }
    });

    // Main thread waits until the thread in thread pool is executing (thus occupied all threads in pool).
    waitBlockingTaskStarted();
  }

  private void waitBlockingTaskStarted() throws InterruptedException {
    lock.lock();
    try {
      while (!isBlockingTaskStarted) {
        this.condition.await(120, TimeUnit.SECONDS);
      }
    } finally {
      lock.unlock();
    }
  }

  private String createVersionAndPushData(String storeName) {
    VersionCreationResponse response = cluster.getNewVersion(storeName);

    String topicName = response.getKafkaTopic();
    Assert.assertEquals(response.getReplicas(), replicaFactor);
    Assert.assertEquals(response.getPartitions(), partitionNum);

    try (VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName)) {
      veniceWriter.broadcastStartOfPush(new HashMap<>());
      veniceWriter.put("test", "test", 1);
      veniceWriter.broadcastEndOfPush(new HashMap<>());
    }
    return topicName;
  }

  private void setUpServers(boolean isDualPool) {
    // Intentionally minimize the thread for leader follower state transition.
    Properties extraProperties = new Properties();
    extraProperties.put(ConfigKeys.MAX_LEADER_FOLLOWER_STATE_TRANSITION_THREAD_NUMBER, 1);

    if (isDualPool) {
      // Set a reasonable size for future version thread pool.
      extraProperties.put(ConfigKeys.MAX_FUTURE_VERSION_LEADER_FOLLOWER_STATE_TRANSITION_THREAD_NUMBER, 3);
      extraProperties.put(
          ConfigKeys.LEADER_FOLLOWER_STATE_TRANSITION_THREAD_POOL_STRATEGY,
          LeaderFollowerPartitionStateModelFactory.LeaderFollowerThreadPoolStrategy.DUAL_POOL_STRATEGY.name());
    } else {
      extraProperties.put(
          ConfigKeys.LEADER_FOLLOWER_STATE_TRANSITION_THREAD_POOL_STRATEGY,
          LeaderFollowerPartitionStateModelFactory.LeaderFollowerThreadPoolStrategy.SINGLE_POOL_STRATEGY.name());
    }
    server0 = cluster.addVeniceServer(new Properties(), extraProperties);
    server1 = cluster.addVeniceServer(new Properties(), extraProperties);
  }
}
