package com.linkedin.venice.stats;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.TimeUnit;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.zookeeper.WatchedEvent;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.zookeeper.Watcher.Event.KeeperState;


public class TestZkClientStatusStats {
  private MockTehutiReporter reporter;
  private ZkClientStatusStats zkStats;
  private String clientName = "test-client";

  @BeforeMethod
  public void setup() {
    MetricsRepository metricsRepository = new MetricsRepository();
    reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);
    zkStats = new ZkClientStatusStats(metricsRepository, clientName);
  }

  @Test
  public void testStatsCanUpdateZkStatus() {
    Assert.assertEquals(reporter.query("." + clientName + "--zk_client_disconnection_times.Count").value(), 0d);
    Assert.assertEquals(reporter.query("." + clientName + "--zk_client_status.Gauge").value(), -1d);

    zkStats.handleStateChanged(KeeperState.Disconnected);
    Assert.assertEquals(reporter.query("." + clientName + "--zk_client_disconnection_times.Count").value(), 1d);
    Assert.assertEquals(reporter.query("." + clientName + "--zk_client_status.Gauge").value(), 0d);

    zkStats.handleStateChanged(KeeperState.SyncConnected);
    Assert.assertEquals(reporter.query("." + clientName + "--zk_client_status.Gauge").value(), 3d);
  }

  @Test
  public void testZkClientCanUpdateStatus() {
    ZkServerWrapper zkServer = ServiceFactory.getZkServer();
    ZkClient zkClient = new ZkClient(zkServer.getAddress());
    zkClient.waitUntilConnected();
    zkClient.subscribeStateChanges(zkStats);

    Assert.assertEquals(reporter.query("." + clientName + "--zk_client_disconnection_times.Count").value(), 0d);

    zkClient.process(new WatchedEvent(null, KeeperState.Disconnected, null));
    zkClient.process(new WatchedEvent(null, KeeperState.SyncConnected, null));
    TestUtils.waitForNonDeterministicAssertion(10000, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(reporter.query("." + clientName + "--zk_client_disconnection_times.Count").value(), 1d);
      Assert.assertEquals(reporter.query("." + clientName + "--zk_client_status.Gauge").value(), 3d);

    });

    zkClient.close();
    zkServer.close();
  }
}
