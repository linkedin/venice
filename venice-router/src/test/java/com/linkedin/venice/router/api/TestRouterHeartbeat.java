package com.linkedin.venice.router.api;

import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.venice.integration.utils.MockHttpServerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.utils.TestUtils;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestRouterHeartbeat {
  @Test
  public void heartBeatMarksUnreachableNodes()
      throws Exception {
    VeniceHostHealth healthMon = new VeniceHostHealth();
    Map<Instance, CloseableHttpAsyncClient> clientPool = new HashMap<>();
    CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom().setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())  //Supports connection re-use if able
        .setMaxConnPerRoute(2) // concurrent execute commands beyond this limit get queued internally by the client
        .setMaxConnTotal(2).build(); // testing shows that > 2 concurrent request increase failure rate, hence using connection pool.
    httpClient.start();
    Instance dummyInstance = new Instance("nodeId", "localhost", 58232);
    clientPool.put(dummyInstance, httpClient);

    Assert.assertTrue(healthMon.isHostHealthy(dummyInstance, "partition"));

    RouterHeartbeat heartbeat = new RouterHeartbeat(clientPool, healthMon, 10, TimeUnit.SECONDS, 1000);
    heartbeat.start();
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS,
        () -> Assert.assertFalse(healthMon.isHostHealthy(dummyInstance, "partition")));
    heartbeat.stop();
    httpClient.close();
  }

  @Test
  public void heartBeatKeepsGoodNodesHealthy()
      throws Exception {
    VeniceHostHealth healthMon = new VeniceHostHealth();
    Map<Instance, CloseableHttpAsyncClient> clientPool = new HashMap<>();
    CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom().setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())  //Supports connection re-use if able
        .setMaxConnPerRoute(2) // concurrent execute commands beyond this limit get queued internally by the client
        .setMaxConnTotal(2).build(); // testing shows that > 2 concurrent request increase failure rate, hence using connection pool.
    httpClient.start();

    MockHttpServerWrapper server = ServiceFactory.getMockHttpServer("storage-node", new ArrayList<D2Server>());
    FullHttpResponse goodHealthResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    server.addResponseForUri(QueryAction.HEALTH.toString().toLowerCase(), goodHealthResponse);
    Instance dummyInstance = new Instance("nodeId", "localhost", server.getPort());
    clientPool.put(dummyInstance, httpClient);
    RouterHeartbeat heartbeat = new RouterHeartbeat(clientPool, healthMon, 10, TimeUnit.SECONDS, 1000);
    heartbeat.start();
    Thread.sleep(20); /* Just to give the heartbeat time to do something */
    Assert.assertTrue(healthMon.isHostHealthy(dummyInstance, "partition"));
    heartbeat.stop();
    httpClient.close();
  }
}
