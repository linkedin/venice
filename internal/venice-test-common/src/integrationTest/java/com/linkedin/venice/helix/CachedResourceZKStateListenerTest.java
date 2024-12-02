package com.linkedin.venice.helix;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class CachedResourceZKStateListenerTest {
  private ZkServerWrapper zkServer;
  private ZkClient zkClient;
  private static final int WAIT_TIME = 10000;

  @BeforeMethod
  public void setUp() {
    zkServer = ServiceFactory.getZkServer();
    zkClient = ZkClientFactory.newZkClient(zkServer.getAddress());
  }

  @AfterMethod
  public void cleanUp() {
    zkClient.close();
    zkServer.close();
  }

  @Test
  public void testReconnectWithRefreshFailed() throws Exception {
    MockVeniceResourceWillThrowExceptionWhileRefreshing resource =
        new MockVeniceResourceWillThrowExceptionWhileRefreshing();
    int retryAttempts = 3;
    CachedResourceZkStateListener listener = new CachedResourceZkStateListener(resource, retryAttempts, 100);
    zkClient.subscribeStateChanges(listener);
    // zkClient.close();
    // zkClient.connect(WAIT_TIME, zkClient);
    WatchedEvent disconnectEvent = new WatchedEvent(null, Watcher.Event.KeeperState.Disconnected, null);
    WatchedEvent connectEvent = new WatchedEvent(null, Watcher.Event.KeeperState.SyncConnected, null);
    // process disconnect event
    zkClient.process(disconnectEvent);
    // process connect event
    zkClient.process(connectEvent);
    TestUtils.waitForNonDeterministicCompletion(WAIT_TIME, TimeUnit.MILLISECONDS, new BooleanSupplier() {
      @Override
      public boolean getAsBoolean() {
        return resource.refreshCount == retryAttempts;
      }
    });
    Assert.assertFalse(listener.isDisconnected(), "Client should be reconnected");
    Assert.assertFalse(resource.isRefreshed, "Reconnected, resource should not be refreshed correctly.");
    Assert.assertEquals(
        resource.refreshCount,
        retryAttempts,
        "Should retry +" + retryAttempts
            + " to avoid non-deterministic issue that zk could return partial result after getting reconnected. ");
  }

  @Test
  public void testReconnectWithRefresh() {
    MockVeniceResource resource = new MockVeniceResource();
    int retryAttempts = 3;
    CachedResourceZkStateListener listener = new CachedResourceZkStateListener(resource, retryAttempts, 100);
    zkClient.subscribeStateChanges(listener);
    WatchedEvent disconnectEvent = new WatchedEvent(null, Watcher.Event.KeeperState.Disconnected, null);
    WatchedEvent connectEvent = new WatchedEvent(null, Watcher.Event.KeeperState.SyncConnected, null);
    // process disconnect event
    zkClient.process(disconnectEvent);
    // process connect event
    zkClient.process(connectEvent);
    TestUtils.waitForNonDeterministicCompletion(WAIT_TIME, TimeUnit.MILLISECONDS, new BooleanSupplier() {
      @Override
      public boolean getAsBoolean() {
        return resource.isRefreshed && resource.refreshCount == 1;
      }
    });
    Assert.assertFalse(listener.isDisconnected(), "Client should be reconnected");
    Assert.assertTrue(resource.isRefreshed, "Reconnected, resource should be refreshed.");
    Assert.assertEquals(
        resource.refreshCount,
        1,
        "Should only refresh once. Because is refresh succeed, we should not keep retrying. ");
  }

  @Test
  public void testHandleStateChanged() throws Exception {
    MockVeniceResourceWillThrowExceptionWhileRefreshing resource =
        new MockVeniceResourceWillThrowExceptionWhileRefreshing();
    CachedResourceZkStateListener listener = new CachedResourceZkStateListener(resource, 1, 100);

    listener.handleStateChanged(Watcher.Event.KeeperState.SyncConnected);
    Assert.assertFalse(listener.isDisconnected(), "It's the first to connect, but not the reconnecting.");

    listener.handleStateChanged(Watcher.Event.KeeperState.Disconnected);
    Assert.assertTrue(listener.isDisconnected(), "Connection is disconnected.");

    listener.handleStateChanged(Watcher.Event.KeeperState.SyncConnected);
    Assert.assertFalse(listener.isDisconnected(), "After reconnecting, client is connected again.");
  }

  private static class MockVeniceResource implements VeniceResource {
    protected volatile boolean isRefreshed;
    protected int refreshCount = 0;

    @Override
    public void refresh() {
      isRefreshed = true;
      refreshCount++;
    }

    @Override
    public void clear() {
      isRefreshed = false;
    }
  }

  private static class MockVeniceResourceWillThrowExceptionWhileRefreshing extends MockVeniceResource {
    @Override
    public void refresh() {
      super.refresh();
      isRefreshed = false;
      throw new VeniceException();
    }
  }
}
