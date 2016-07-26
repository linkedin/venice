package com.linkedin.venice.helix;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.TestUtils;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class CachedResourceZKStateListenerTest {
  private ZkServerWrapper zkServer;
  private ZkClient zkClient;
  private static final int WAIT_TIME = 1000;

  @BeforeMethod
  public void setup() {
    zkServer = ServiceFactory.getZkServer();
    zkClient = new ZkClient(zkServer.getAddress(), WAIT_TIME);
  }

  @AfterMethod
  public void cleanup() {
    zkClient.close();
    zkServer.close();
  }

  @Test
  public void testReconnect()
      throws Exception {
    MockVeniceResource resource = new MockVeniceResource();
    CachedResourceZkStateListener listener = new CachedResourceZkStateListener(resource);
    zkClient.subscribeStateChanges(listener);
    // zkClient.close();
    //zkClient.connect(WAIT_TIME, zkClient);
    WatchedEvent disconnectEvent = new WatchedEvent(null, Watcher.Event.KeeperState.Disconnected, null);
    WatchedEvent connectEvent = new WatchedEvent(null, Watcher.Event.KeeperState.SyncConnected, null);
    // process disconnect event
    zkClient.process(disconnectEvent);
    // process connect event
    zkClient.process(connectEvent);
    TestUtils.waitForNonDeterministicCompletion(WAIT_TIME, TimeUnit.MILLISECONDS, new BooleanSupplier() {
      @Override
      public boolean getAsBoolean() {
        return resource.isRefreshed;
      }
    });
    Assert.assertFalse(listener.isDisconnected(), "Client should be reconnected");
    Assert.assertTrue(resource.isRefreshed, "Reconnected, resource should be refreshed.");
  }

  @Test
  public void testHandleStateChanged()
      throws Exception {
    MockVeniceResource resource = new MockVeniceResource();
    CachedResourceZkStateListener listener = new CachedResourceZkStateListener(resource);

    listener.handleStateChanged(Watcher.Event.KeeperState.SyncConnected);
    Assert.assertFalse(listener.isDisconnected(), "It's the first to connect, but not the reconnecting.");

    listener.handleStateChanged(Watcher.Event.KeeperState.Disconnected);
    Assert.assertTrue(listener.isDisconnected(), "Connection is disconnected.");

    listener.handleStateChanged(Watcher.Event.KeeperState.SyncConnected);
    Assert.assertFalse(listener.isDisconnected(), "After reconnecting, client is connected again.");
  }

  private static class MockVeniceResource implements VeniceResource {
    private volatile boolean isRefreshed;

    @Override
    public void refresh() {
      isRefreshed = true;
    }

    @Override
    public void clear() {
      isRefreshed = false;
    }
  }
}
