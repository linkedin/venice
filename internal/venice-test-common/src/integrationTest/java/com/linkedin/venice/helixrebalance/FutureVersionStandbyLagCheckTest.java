package com.linkedin.venice.helixrebalance;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Integration tests covering the future-version standby lag check feature (best-effort progress-based leader
 * election): when a replica transitions OFFLINE -> STANDBY for a future version whose push is still in progress,
 * the state transition can optionally poll the local version-topic lag and wait for it to drop below a threshold
 * (instead of waiting for full ingestion completion), bounded by a timeout so the push job is never blocked
 * indefinitely.
 */
public class FutureVersionStandbyLagCheckTest {
  private VeniceClusterWrapper cluster;
  private final int replicaFactor = 2;
  private final int partitionSize = 1000;
  private final int partitionNum = 1;

  @BeforeMethod
  public void setUp() {
    Properties extraProperties = new Properties();
    extraProperties.put(ConfigKeys.DEFAULT_OFFLINE_PUSH_STRATEGY, OfflinePushStrategy.WAIT_ALL_REPLICAS.name());
    extraProperties.put(ConfigKeys.OFFLINE_JOB_START_TIMEOUT_MS, 30_000);
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(0)
        .numberOfRouters(1)
        .replicationFactor(replicaFactor)
        .partitionSize(partitionSize)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .extraProperties(extraProperties)
        .build();
    cluster = ServiceFactory.getVeniceCluster(options);
  }

  @AfterMethod
  public void cleanUp() {
    cluster.close();
  }

  /**
   * With the lag check enabled and a threshold/timeout that are easily satisfiable, a future version push should
   * still complete successfully: the OFFLINE -> STANDBY transition polls the local lag, observes it catch up
   * quickly (since the push is tiny), and proceeds to STANDBY without waiting for the whole timeout budget.
   */
  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testFutureVersionPushCompletesWhenLagCheckEnabledAndLagCatchesUp() throws Exception {
    setUpServers(true, 60, 1);
    String storeName = Utils.getUniqueString("testFutureVersionLagCheckEnabled");
    runPushAndVerifyCompletion(storeName);
    // Push a future version on top; it should also complete even with the lag check enabled.
    runPushAndVerifyCompletion(storeName);
  }

  /**
   * With the lag check enabled but a timeout of 0 seconds (i.e. the wait budget is immediately exhausted), the
   * OFFLINE -> STANDBY transition must fail open and proceed exactly like the feature being disabled, so the push
   * job still completes instead of hanging or failing.
   */
  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testFutureVersionPushCompletesWhenLagCheckTimesOutImmediately() throws Exception {
    setUpServers(true, 0, 1);
    String storeName = Utils.getUniqueString("testFutureVersionLagCheckTimeout");
    runPushAndVerifyCompletion(storeName);
    runPushAndVerifyCompletion(storeName);
  }

  /**
   * With the lag check disabled (default behavior), future version pushes complete as before. This acts as the
   * baseline/regression guard for the new feature.
   */
  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testFutureVersionPushCompletesWhenLagCheckDisabled() throws Exception {
    setUpServers(false, 0, 0);
    String storeName = Utils.getUniqueString("testFutureVersionLagCheckDisabled");
    runPushAndVerifyCompletion(storeName);
    runPushAndVerifyCompletion(storeName);
  }

  private void runPushAndVerifyCompletion(String storeName) {
    if (cluster.getLeaderVeniceController().getVeniceAdmin().getStore(cluster.getClusterName(), storeName) == null) {
      cluster.getNewStore(storeName);
      long storageQuota = (long) partitionNum * partitionSize;
      cluster.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));
    }

    String topicName = createVersionAndPushData(storeName);

    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        true,
        () -> Assert.assertEquals(
            cluster.getLeaderVeniceController()
                .getVeniceAdmin()
                .getOffLinePushStatus(cluster.getClusterName(), topicName)
                .getExecutionStatus(),
            ExecutionStatus.COMPLETED));
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

  private void setUpServers(boolean lagCheckEnabled, int timeoutMinutes, int pollIntervalMinutes) {
    Properties extraProperties = new Properties();
    extraProperties.put(ConfigKeys.SERVER_FUTURE_VERSION_STANDBY_LAG_CHECK_ENABLED, lagCheckEnabled);
    extraProperties.put(ConfigKeys.SERVER_FUTURE_VERSION_STANDBY_LAG_THRESHOLD, 0);
    extraProperties.put(ConfigKeys.SERVER_FUTURE_VERSION_STANDBY_LAG_CHECK_TIMEOUT_MINUTES, timeoutMinutes);
    extraProperties.put(ConfigKeys.SERVER_FUTURE_VERSION_STANDBY_LAG_CHECK_POLL_INTERVAL_MINUTES, pollIntervalMinutes);

    cluster.addVeniceServer(new Properties(), extraProperties);
    cluster.addVeniceServer(new Properties(), extraProperties);
  }
}
