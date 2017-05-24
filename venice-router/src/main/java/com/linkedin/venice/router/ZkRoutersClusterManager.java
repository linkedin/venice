package com.linkedin.venice.router;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.HelixUtils;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.helix.manager.zk.ZkClient;


/**
 * Manage live routers through Zookeeper. Help each router to create a ZNode which reflects whether that router is
 * connected to ZK cluster or not and monitor all routers' ZNodes to get how many routers live right now.
 */
public class ZkRoutersClusterManager implements RoutersClusterManager, IZkChildListener {
  private static final String PREFIX_PATH = "/routers";
  private final String clusterName;
  private final ZkClient zkClient;
  private final String instanceId;
  private volatile int routerCount = 0;

  private final Set<RouterCountChangedListener> listeners;

  public ZkRoutersClusterManager(ZkClient zkClient, String clusterName, String instanceId) {
    this.zkClient = zkClient;
    this.clusterName = clusterName;
    listeners = new HashSet<>();
    this.instanceId = instanceId;
  }

  /**
   * Create a ephemeral ZNode for the give router. If parent path does not exist, it will help to create that as well.
   */
  @Override
  public void registerCurrentRouter() {
    // TODO we could add weight value for each router later, weight could be written into this znode.
    try {
      zkClient.createEphemeral(getRouterPath(instanceId));
      this.zkClient.subscribeChildChanges(getRouterRootPath(), this);
      changeRouterCount(zkClient.getChildren(getRouterRootPath()).size());
    } catch (ZkNoNodeException e) {
      // For a new cluster, the path for routers might not be created, try to create it.
      try {
        zkClient.createPersistent(getRouterRootPath(), true);
      } catch (ZkNodeExistsException ee) {
        //ignore, the path might already be created by other routers.
      }
      // Try to create ephemeral node for this router again.
      registerCurrentRouter();
    }
  }

  @Override
  public void unregisterCurrentRouter() {
    boolean result = zkClient.delete(getRouterPath(instanceId));
    if (!result) {
      throw new VeniceException(
          "Could not delete router: " + instanceId + " from zk. Path:" + getRouterPath(instanceId));
    }
    zkClient.unsubscribeChildChanges(getRouterPath(instanceId), this);
    changeRouterCount(zkClient.getChildren(getRouterRootPath()).size());
  }

  /**
   * Get how many routers live right now. It could be a delay because the number will only be update once the manger
   * received
   * the zk notification.
   */
  @Override
  public int getRoutersCount() {
    return this.routerCount;
  }

  @Override
  public void subscribeRouterCountChangedEvent(RouterCountChangedListener listener) {
    synchronized (listeners) {
      listeners.add(listener);
    }
  }

  @Override
  public void unSubscribeRouterCountChangedEvent(RouterCountChangedListener listener) {
    synchronized (listeners) {
      listeners.remove(listener);
    }
  }

  protected void changeRouterCount(int newRouterCount) {
    if (this.routerCount != newRouterCount) {
      this.routerCount = newRouterCount;
      triggerRouterCountChangedEvent(routerCount);
    }
  }

  protected void triggerRouterCountChangedEvent(int newRouterCount) {
    synchronized (listeners) {
      // As we don't expect there are lots of listeners and the handle logic should be simple enough in each of listener,
      // so we trigger them one by one in synchronize.
      for (RouterCountChangedListener listener : listeners) {
        listener.handleRouterCountChanged(newRouterCount);
      }
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds)
      throws Exception {
    changeRouterCount(currentChilds.size());
  }

  private String getRouterRootPath() {
    return HelixUtils.getHelixClusterZkPath(clusterName) + PREFIX_PATH;
  }

  private String getRouterPath(String instanceId) {
    return getRouterRootPath() + "/" + instanceId;
  }
}
