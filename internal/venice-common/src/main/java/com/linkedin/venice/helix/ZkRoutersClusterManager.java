package com.linkedin.venice.helix;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.RoutersClusterConfig;
import com.linkedin.venice.meta.RoutersClusterManager;
import com.linkedin.venice.utils.HelixUtils;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.helix.zookeeper.zkclient.exception.ZkNodeExistsException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;


/**
 * Manage live routers through Zookeeper. Help each router to create a ZNode which reflects whether that router is
 * connected to ZK cluster or not and monitor all routers' ZNodes to get how many routers live right now.
 */
public class ZkRoutersClusterManager
    implements RoutersClusterManager, IZkChildListener, IZkDataListener, VeniceResource, IZkStateListener {
  private static final Logger LOGGER = LogManager.getLogger(ZkRoutersClusterManager.class);
  private static final String PREFIX_PATH = "/routers";
  private final String clusterName;
  private final ZkClient zkClient;
  private volatile int liveRouterCount = 0;
  private final AtomicBoolean isConnected = new AtomicBoolean(true);
  private volatile RoutersClusterConfig routersClusterConfig;

  private final ZkBaseDataAccessor<RoutersClusterConfig> dataAccessor;

  private final Set<RouterCountChangedListener> routerCountListeners;

  private final Set<RouterClusterConfigChangedListener> configListeners;

  private final CachedResourceZkStateListener zkStateListener;

  private final int refreshAttemptsForZkReconnect;
  private final long refreshIntervalForZkReconnectInMs;

  public ZkRoutersClusterManager(
      ZkClient zkClient,
      HelixAdapterSerializer adaper,
      String clusterName,
      int refreshAttemptsForZkReconnect,
      long refreshIntervalForZkReconnectInMs) {
    this.zkClient = zkClient;
    this.clusterName = clusterName;
    routerCountListeners = new HashSet<>();
    configListeners = new HashSet<>();
    adaper.registerSerializer(getRouterRootPath(), new RouterClusterConfigJSONSerializer());
    zkClient.setZkSerializer(adaper);
    dataAccessor = new ZkBaseDataAccessor<>(zkClient);
    zkStateListener =
        new CachedResourceZkStateListener(this, refreshAttemptsForZkReconnect, refreshIntervalForZkReconnectInMs);
    this.refreshAttemptsForZkReconnect = refreshAttemptsForZkReconnect;
    this.refreshIntervalForZkReconnectInMs = refreshIntervalForZkReconnectInMs;
    isConnected.set(zkClient.getConnection().getZookeeperState().isAlive());
    this.zkClient.subscribeStateChanges(this);
  }

  @Override
  public void refresh() {
    LOGGER.info("Refresh started for cluster {}'s {}.", clusterName, getClass().getSimpleName());
    zkClient.subscribeDataChanges(getRouterRootPath(), this);
    zkClient.subscribeChildChanges(getRouterRootPath(), this);
    RoutersClusterConfig newRoutersClusterConfig = dataAccessor.get(getRouterRootPath(), null, AccessOption.PERSISTENT);
    // No config found in ZK, use the default config.
    if (newRoutersClusterConfig == null) {
      createRouterClusterConfig();
      newRoutersClusterConfig = new RoutersClusterConfig();
    }
    routersClusterConfig = newRoutersClusterConfig;
    zkClient.subscribeStateChanges(zkStateListener);
    // force a live router count update
    changeLiveRouterCount(zkClient.getChildren(getRouterRootPath()).size());
    LOGGER.info("Refresh finished for cluster {}'s {}.", clusterName, getClass().getSimpleName());
  }

  @Override
  public void clear() {
    zkClient.unsubscribeDataChanges(getRouterRootPath(), this);
    zkClient.unsubscribeChildChanges(getRouterRootPath(), this);
    routersClusterConfig = null;
    zkClient.unsubscribeStateChanges(zkStateListener);
  }

  /**
   * Create a ephemeral ZNode for the give router. If parent path does not exist, it will help to create that as well.
   */
  @Override
  public synchronized void registerRouter(String instanceId) {
    // TODO we could add weight value for each router later, weight could be written into this znode.
    try {
      zkClient.createEphemeral(getRouterPath(instanceId));
      LOGGER.info("Add router: {} into live routers.", instanceId);
      changeLiveRouterCount(zkClient.getChildren(getRouterRootPath()).size());
    } catch (ZkNoNodeException e) {
      // For a new cluster, the path for routers might not be created, try to create it.
      try {
        zkClient.createPersistent(getRouterRootPath(), true);
      } catch (ZkNodeExistsException ee) {
        // ignore, the path might already be created by other routers.
      }
      // Try to create ephemeral node for this router again.
      registerRouter(instanceId);
    }
  }

  @Override
  public synchronized void unregisterRouter(String instanceId) {
    try {
      // if this returns false, it means the router instance doesn't exist zk path
      boolean pathExistedPriorToDeletion = zkClient.delete(getRouterPath(instanceId));
      if (!pathExistedPriorToDeletion) {
        LOGGER.info("Attempted to delete a non-existent zk path: {}.", getRouterPath(instanceId));
      }
    } catch (Exception e) {
      // cannot delete this instance from zk path because of exceptions
      throw new VeniceException(
          "Error when deleting router " + instanceId + " from zk path " + getRouterPath(instanceId),
          e);
    }
    changeLiveRouterCount(zkClient.getChildren(getRouterRootPath()).size());
    LOGGER.info("Removed router {} from live routers temporarily.", instanceId);
  }

  /**
   * Get how many routers live right now. It could be a delay because the number will only be update once the manger
   * received
   * the zk notification.
   */
  @Override
  public int getLiveRoutersCount() {
    return this.liveRouterCount;
  }

  @Override
  public int getExpectedRoutersCount() {
    return routersClusterConfig.getExpectedRouterCount();
  }

  @Override
  public void updateExpectedRouterCount(int expectedNumber) {
    compareAndSetClusterConfig(currentData -> {
      validateExpectRouterCount(expectedNumber);
      currentData.setExpectedRouterCount(expectedNumber);
      return currentData;
    });
  }

  @Override
  public void subscribeRouterCountChangedEvent(RouterCountChangedListener listener) {
    synchronized (routerCountListeners) {
      routerCountListeners.add(listener);
    }
  }

  @Override
  public void unSubscribeRouterCountChangedEvent(RouterCountChangedListener listener) {
    synchronized (routerCountListeners) {
      routerCountListeners.remove(listener);
    }
  }

  @Override
  public void subscribeRouterClusterConfigChangedEvent(RouterClusterConfigChangedListener listener) {
    synchronized (configListeners) {
      configListeners.add(listener);
    }
  }

  @Override
  public void unSubscribeRouterClusterConfighangedEvent(RouterClusterConfigChangedListener listener) {
    synchronized (configListeners) {
      configListeners.remove(listener);
    }
  }

  @Override
  public boolean isThrottlingEnabled() {
    // If router's config could not be found, by default we think the throttling is enabled.
    return routersClusterConfig.isThrottlingEnabled();
  }

  @Override
  public boolean isMaxCapacityProtectionEnabled() {
    return routersClusterConfig.isMaxCapacityProtectionEnabled();
  }

  @Override
  public void enableThrottling(boolean enable) {
    compareAndSetClusterConfig(currentData -> {
      if (currentData == null) {
        currentData = new RoutersClusterConfig();
      }
      currentData.setThrottlingEnabled(enable);
      return currentData;
    });
    routersClusterConfig.setThrottlingEnabled(enable);
  }

  private void validateExpectRouterCount(int expectedRouterCount) {
    if (expectedRouterCount < 1) {
      throw new VeniceException(
          "Invalid value of expectedRouterCount: " + expectedRouterCount + ", should be larger than 0.");
    }
  }

  @Override
  public void enableMaxCapacityProtection(boolean enable) {
    compareAndSetClusterConfig(currentData -> {
      if (currentData == null) {
        currentData = new RoutersClusterConfig();
      }
      currentData.setMaxCapacityProtectionEnabled(enable);
      return currentData;
    });
    routersClusterConfig.setMaxCapacityProtectionEnabled(enable);
  }

  @Override
  public void createRouterClusterConfig() {
    /**
     * {@link ZkBaseDataAccessor#set} will try to create the ZNode if it doesn't exist.
     * If the ZNode already exists, it will just simply update the content.
     */
    boolean createConfigSuccess =
        dataAccessor.set(getRouterRootPath(), new RoutersClusterConfig(), AccessOption.PERSISTENT);
    if (!createConfigSuccess) {
      throw new VeniceException("Could not create router cluster config.");
    }
  }

  public RoutersClusterConfig getRoutersClusterConfig() {
    return routersClusterConfig.cloneRoutesClusterConfig();
  }

  protected void changeLiveRouterCount(int newRouterCount) {
    if (!isConnected.get()) {
      return;
    }
    if (this.liveRouterCount != newRouterCount) {
      this.liveRouterCount = newRouterCount;
      triggerRouterCountChangedEvent(liveRouterCount);
    }
  }

  protected void triggerRouterCountChangedEvent(int newRouterCount) {
    synchronized (routerCountListeners) {
      // As we don't expect there are lots of routerCountListeners and the handle logic should be simple enough in each
      // of listener,
      // so we trigger them one by one in synchronize.
      for (RouterCountChangedListener listener: routerCountListeners) {
        listener.handleRouterCountChanged(newRouterCount);
      }
    }
  }

  protected void triggerRouterClusterConfigChangedEvent(RoutersClusterConfig newConfig) {
    synchronized (configListeners) {
      for (RouterClusterConfigChangedListener listener: configListeners) {
        listener.handleRouterClusterConfigChanged(newConfig);
      }
    }
  }

  /**
   * Once a router instance is added/remove from cluster, its ephemeral zk node will be added/removed, then this
   * handler
   * will be executed to process the event.
   */
  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
    int oldLiveRouterCount = liveRouterCount;
    changeLiveRouterCount(currentChilds.size());
    LOGGER.info("Live router count has been changed from: {} to: {}.", oldLiveRouterCount, currentChilds.size());
  }

  /**
   * Once router cluster config is changed(Both cluster level or router level config is changed), its persistent zk
   * node's data will be changed, then this handler will be executed to process the event.
   * <p>
   * Make this method synchronized to avoid the conflict modification of routersClusterConfig.
   */
  @Override
  public synchronized void handleDataChange(String dataPath, Object data) throws Exception {
    if (data instanceof RoutersClusterConfig) {
      RoutersClusterConfig newConfig = (RoutersClusterConfig) data;
      if (routersClusterConfig == null || !routersClusterConfig.equals(newConfig)) {
        routersClusterConfig = newConfig;
        triggerRouterClusterConfigChangedEvent(routersClusterConfig);
        LOGGER.info("Router Cluster Config have been changed.");
      } else {
        LOGGER.info("Router Cluster Config have not been changed, ignore the data changed event.");
      }
    } else {
      if (data != null) {
        LOGGER.error("Invalid config type: {}.", data.getClass().getName());
      }
    }
  }

  @Override
  public synchronized void handleDataDeleted(String dataPath) throws Exception {
    // Cluster is deleted.
    clear();
  }

  protected final String getRouterRootPath() {
    return HelixUtils.getHelixClusterZkPath(clusterName) + PREFIX_PATH;
  }

  private String getRouterPath(String instanceId) {
    return getRouterRootPath() + "/" + instanceId;
  }

  private void compareAndSetClusterConfig(DataUpdater<RoutersClusterConfig> updater) {
    // Compare and update the config in ZK because it might be updated by other router/controller at the same time.
    HelixUtils.compareAndUpdate(dataAccessor, getRouterRootPath(), updater);
    // Read latest version from ZK to update local config. If we just do routerClusterConfig.addRouterConfig only,
    // this update might be overwritten by the change from other router/controller in a short time.
    // For example:
    // 1. router1 is registered
    // 2. router2 is trying to register.
    // 3. router2's registration succeed through our compare and update mechanism.
    // 4. router2 execute routerClusterConfig.addRouterConfig(router2)
    // 5. Data change event is coming for the router1's registration and overwrite the router2's registration info
    // 6. Data change event is coming for the router2's registration and update data to the correct version.
    // So after registration, read the latest version from zk is more safe here and the cost is pretty small because
    // registration is a rare operation.
    // Synchronized keyword protect routersClusterConfig from conflict updating.
    routersClusterConfig = dataAccessor.get(getRouterRootPath(), null, AccessOption.PERSISTENT);
    triggerRouterClusterConfigChangedEvent(routersClusterConfig);
  }

  @Override
  public void handleStateChanged(Watcher.Event.KeeperState keeperState) {
    if (keeperState.getIntValue() != 3 && keeperState.getIntValue() != 5 && keeperState.getIntValue() != 6) {
      // Looks like we're disconnected. Lets update our connection state and freeze our internal state
      LOGGER.warn("zkclient is disconnected and is in state: {}.", keeperState);
      this.isConnected.set(false);
    } else {
      // Now we are in a connected state. Unfreeze things and refresh data
      this.isConnected.set(true);
      LOGGER.info("zkclient is connected and is in state: {}.", keeperState);
      this.refresh();
    }
  }

  @Override
  public void handleNewSession(String s) throws Exception {
    // do nothing
  }

  @Override
  public void handleSessionEstablishmentError(Throwable throwable) throws Exception {
    // do nothing
  }
}
