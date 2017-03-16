package com.linkedin.venice.router.api;

import com.linkedin.venice.integration.utils.MockHttpServerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.LiveInstance;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;



public class TestRouterHeartbeat {
  @Test
  public void heartBeatMarksUnreachableNodes()
      throws Exception {
    VeniceHostHealth healthMon = new VeniceHostHealth();

    // This is a fake instance that wont respond.  Nothing is runing on that port
    LiveInstance dummyInstance = new LiveInstance("localhost_58262");

    // The ZkHelixManager provides a List of LiveInstance objects, so we create a list for the mock
    List<LiveInstance> dummyList = new ArrayList<>();
    dummyList.add(dummyInstance);

    // Venice uses the Instance class instead of LiveInstance, so we create an Instance.
    Instance dummy = new Instance(dummyInstance.getId(), "localhost", 58262);

    // Create the mock manager and verify our dummy instance is healthy before starting the heartbeat
    ZKHelixManager mockManager = Mockito.mock(ZKHelixManager.class);
    Assert.assertTrue(healthMon.isHostHealthy(dummy, "partition"));
    RouterHeartbeat heartbeat = new RouterHeartbeat(mockManager, healthMon, 1, TimeUnit.SECONDS, 500, Optional.empty());
    heartbeat.start();
    Thread.sleep(200); // Let heartbeat initialize.  TODO: More deterministic solution

    // The heartbeat calls the manager to get live instances to query, and passes in a listener.  We capture the listener
    // and supply it with our list of LiveInstances (which has our dummy Instance)
    ArgumentCaptor<LiveInstanceChangeListener> listenerArgument = ArgumentCaptor.forClass(LiveInstanceChangeListener.class);
    Mockito.verify(mockManager).addLiveInstanceChangeListener(listenerArgument.capture());
    listenerArgument.getValue().onLiveInstanceChange(dummyList, new NotificationContext(mockManager));

    // Since the heartbeat is querying an instance that wont respond, we expect it to tell the health monitor that the
    // host is unhealthy.
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS,
        () -> Assert.assertFalse(healthMon.isHostHealthy(dummy, "partition")));
    heartbeat.stop();
  }

  @Test
  public void heartBeatKeepsGoodNodesHealthy() throws Exception {

    // We want to verify the heartbeat can get a response from a server, so we create a server that
    // responds to a health check.
    MockHttpServerWrapper server = ServiceFactory.getMockHttpServer("storage-node");
    FullHttpResponse goodHealthResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    server.addResponseForUri("/" + QueryAction.HEALTH.toString().toLowerCase(), goodHealthResponse);
    VeniceHostHealth healthMon = new VeniceHostHealth();

    // now our dummy instance lives at the port where a good health check will return
    int port = server.getPort();
    LiveInstance dummyInstance = new LiveInstance("localhost_" + port);
    List<LiveInstance> dummyList = new ArrayList<>();
    dummyList.add(dummyInstance);
    Instance dummy = new Instance(dummyInstance.getId(), "localhost", port);

    // Create our manager and start the heartbeat process.
    ZKHelixManager mockManager = Mockito.mock(ZKHelixManager.class);
    RouterHeartbeat heartbeat = new RouterHeartbeat(mockManager, healthMon, 100, TimeUnit.MILLISECONDS, 500, Optional.empty());
    heartbeat.start();
    Thread.sleep(200); // Let heartbeat initialize.  TODO: More deterministic solution

    // The heartbeat calls the manager to get live instances to query, and passes in a listener.  We capture the listener
    // and supply it with our list of LiveInstances (which has our dummy Instance)
    ArgumentCaptor<LiveInstanceChangeListener> listenerArgument = ArgumentCaptor.forClass(LiveInstanceChangeListener.class);
    Mockito.verify(mockManager).addLiveInstanceChangeListener(listenerArgument.capture());
    listenerArgument.getValue().onLiveInstanceChange(dummyList, new NotificationContext(mockManager));
    Utils.sleep(200); // Just to give the heartbeat time to do something



    // our instance should stay healthy since it responds to the health check.
    Assert.assertTrue(healthMon.isHostHealthy(dummy, "partition"));
    heartbeat.stop();
    server.close();

  }
}
