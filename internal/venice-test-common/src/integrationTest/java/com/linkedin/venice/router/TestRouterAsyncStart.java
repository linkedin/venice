package com.linkedin.venice.router;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.router.httpclient.StorageNodeClientType;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestRouterAsyncStart {
  private VeniceClusterWrapper veniceCluster;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws VeniceClientException {
    Utils.thisIsLocalhost();
    veniceCluster = ServiceFactory.getVeniceCluster(1, 1, 0);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testConnectionWarmingFailureDuringSyncStart() {
    // Setup Venice server in a way that it will take a very long time to response (drop all requests)
    List<VeniceServerWrapper> servers = veniceCluster.getVeniceServers();
    Assert.assertEquals(servers.size(), 1, "There should be only one storage node in this cluster");
    VeniceServerWrapper serverWrapper = servers.get(0);
    serverWrapper.getVeniceServer().setRequestHandler((ChannelHandlerContext context, Object message) -> true);

    Properties routerProperties = new Properties();
    routerProperties.put(ConfigKeys.ROUTER_STORAGE_NODE_CLIENT_TYPE, StorageNodeClientType.APACHE_HTTP_ASYNC_CLIENT);
    routerProperties.put(ConfigKeys.ROUTER_PER_NODE_CLIENT_ENABLED, true);
    routerProperties.put(ConfigKeys.ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_ENABLED, true);
    routerProperties.put(ConfigKeys.ROUTER_PER_NODE_CLIENT_THREAD_COUNT, 2);
    routerProperties.put(ConfigKeys.ROUTER_MAX_OUTGOING_CONNECTION_PER_ROUTE, 10);
    routerProperties.put(ConfigKeys.ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_LOW_WATER_MARK, 5);
    routerProperties.put(ConfigKeys.ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_SLEEP_INTERVAL_MS, 0);
    routerProperties.put(ConfigKeys.ROUTER_ASYNC_START_ENABLED, false);

    // Venice Router should fail to start because of connection warming since storage node won't respond to any request
    Assert.assertThrows(() -> ServiceFactory.withMaxAttempt(1, () -> veniceCluster.addVeniceRouter(routerProperties)));
  }
}
