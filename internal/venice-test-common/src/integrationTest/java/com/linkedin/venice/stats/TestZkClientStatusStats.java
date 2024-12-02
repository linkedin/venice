package com.linkedin.venice.stats;

import static org.apache.zookeeper.Watcher.Event.KeeperState;

import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.WatchedEvent;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestZkClientStatusStats {
  private MockTehutiReporter reporter;
  private ZkClientStatusStats zkStats;
  private String clientName = "test-client";

  @BeforeMethod
  public void setUp() {
    MetricsRepository metricsRepository = new MetricsRepository();
    reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);
    zkStats = new ZkClientStatusStats(metricsRepository, clientName);
  }

  @Test
  public void testStatsCanUpdateZkStatus() {
    long reconnectionLatency = 1;
    testState(0, 0, 0, Double.NaN, KeeperState.Unknown);
    changeAndTestState(1, 0, 0, Double.NaN, KeeperState.Disconnected);
    Utils.sleep(reconnectionLatency);
    changeAndTestState(1, 1, 0, reconnectionLatency, KeeperState.SyncConnected);
    changeAndTestState(1, 1, 1, reconnectionLatency, KeeperState.Expired);
  }

  @Test
  public void testZkClientCanUpdateStatus() {
    ZkServerWrapper zkServer = null;
    ZkClient zkClient = null;
    try {
      zkServer = ServiceFactory.getZkServer();
      zkClient = ZkClientFactory.newZkClient(zkServer.getAddress());
      long zkConnectionTimeout = 5000;
      Assert.assertTrue(
          zkClient.waitUntilConnected(zkConnectionTimeout, TimeUnit.MILLISECONDS),
          "ZK did not connect within " + zkConnectionTimeout + " ms.");
      zkClient.subscribeStateChanges(zkStats);

      testState(0, 0, 0, Double.NaN, KeeperState.Unknown);

      zkClient.process(new WatchedEvent(null, KeeperState.Disconnected, null));
      zkClient.process(new WatchedEvent(null, KeeperState.SyncConnected, null));
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        testState(1, 1, 0, 0, KeeperState.SyncConnected);
      });
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
      Utils.closeQuietlyWithErrorLogged(zkServer);
    }
  }

  private void changeAndTestState(
      double expectedDisconnectedCount,
      double expectedSyncConnectedCount,
      double expectedExpiredCount,
      double minimumExpectedReconnectionLatency,
      KeeperState newState) {
    zkStats.handleStateChanged(newState);
    testState(
        expectedDisconnectedCount,
        expectedSyncConnectedCount,
        expectedExpiredCount,
        minimumExpectedReconnectionLatency,
        newState);
  }

  private void testState(
      double expectedDisconnectedCount,
      double expectedSyncConnectedCount,
      double expectedExpiredCount,
      double minimumExpectedReconnectionLatency,
      KeeperState newState) {
    Assert.assertEquals(
        reporter.query("." + clientName + "--zk_client_Disconnected.Count").value(),
        expectedDisconnectedCount);
    Assert.assertEquals(
        reporter.query("." + clientName + "--zk_client_SyncConnected.Count").value(),
        expectedSyncConnectedCount);
    Assert.assertEquals(reporter.query("." + clientName + "--zk_client_Expired.Count").value(), expectedExpiredCount);
    Assert.assertEquals(
        reporter.query("." + clientName + "--zk_client_status.Gauge").value(),
        (double) newState.getIntValue());
    double reconnectionLatency = reporter.query("." + clientName + "--zk_client_reconnection_latency.Avg").value();
    if (Double.isNaN(minimumExpectedReconnectionLatency)) {
      Assert.assertEquals(reconnectionLatency, minimumExpectedReconnectionLatency);
    } else {
      Assert.assertTrue(
          reconnectionLatency >= minimumExpectedReconnectionLatency,
          "Reconnection latency expected to be at least " + minimumExpectedReconnectionLatency + " but was "
              + reconnectionLatency);
    }
  }
}
