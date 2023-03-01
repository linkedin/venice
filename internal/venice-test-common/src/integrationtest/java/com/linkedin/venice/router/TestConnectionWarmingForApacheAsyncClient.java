package com.linkedin.venice.router;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.router.httpclient.StorageNodeClientType;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestConnectionWarmingForApacheAsyncClient {
  private VeniceClusterWrapper veniceCluster;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws VeniceClientException {
    Utils.thisIsLocalhost();
    veniceCluster = ServiceFactory.getVeniceCluster(1, 2, 0, 2, 100, true, false);

    Properties routerProperties = new Properties();
    routerProperties.put(ConfigKeys.ROUTER_STORAGE_NODE_CLIENT_TYPE, StorageNodeClientType.APACHE_HTTP_ASYNC_CLIENT);
    routerProperties.put(ConfigKeys.ROUTER_PER_NODE_CLIENT_ENABLED, true);
    routerProperties.put(ConfigKeys.ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_ENABLED, true);
    routerProperties.put(ConfigKeys.ROUTER_PER_NODE_CLIENT_THREAD_COUNT, 2);
    routerProperties.put(ConfigKeys.ROUTER_MAX_OUTGOING_CONNECTION_PER_ROUTE, 10);
    routerProperties.put(ConfigKeys.ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_LOW_WATER_MARK, 5);
    routerProperties.put(ConfigKeys.ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_SLEEP_INTERVAL_MS, 0);
    veniceCluster.addVeniceRouter(routerProperties);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testConnectionWarming() {
    List<VeniceRouterWrapper> routers = veniceCluster.getVeniceRouters();
    Assert.assertEquals(1, routers.size(), "There should be only one router in this cluster");
    MetricsRepository routerMetricsRepository = routers.get(0).getMetricsRepository();
    // Since there are two storage nodes, both of them should be fully warmed after start.
    Assert.assertEquals(
        20.0,
        routerMetricsRepository.getMetric(".connection_pool--total_max_connection_count.LambdaStat").value(),
        "The connections to two storage nodes should be fully warmed up");
    Assert.assertEquals(
        0.0,
        routerMetricsRepository.getMetric(".connection_pool--total_active_connection_count.LambdaStat").value(),
        "There shouldn't be any active connections since there is no traffic");
    Assert.assertEquals(
        20.0,
        routerMetricsRepository.getMetric(".connection_pool--total_idle_connection_count.LambdaStat").value(),
        "The idle connection count be equal to the max connection count");

    // Try to add a new server
    Properties serverFeatureProperties = new Properties();
    serverFeatureProperties.put(VeniceServerWrapper.SERVER_ENABLE_SSL, "true");
    veniceCluster.addVeniceServer(serverFeatureProperties, new Properties());
    // There should be 3 live instances now.
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          30.0,
          routerMetricsRepository.getMetric(".connection_pool--total_max_connection_count.LambdaStat").value(),
          "The connections to three storage nodes should be fully warmed up");
      Assert.assertEquals(
          30.0,
          routerMetricsRepository.getMetric(".connection_pool--total_idle_connection_count.LambdaStat").value(),
          "The idle connection count be equal to the max connection count");
    });
    Assert.assertEquals(
        0.0,
        routerMetricsRepository.getMetric(".connection_pool--total_active_connection_count.LambdaStat").value(),
        "There shouldn't be any active connections since there is no traffic");

    /**
     * Currently, {@link com.linkedin.venice.router.httpclient.ApacheHttpAsyncStorageNodeClient} doesn't handle cleaning up the corresponding client
     * if a storage node gets shutdown.
     */
  }
}
