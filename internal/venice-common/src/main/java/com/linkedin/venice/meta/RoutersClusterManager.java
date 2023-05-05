package com.linkedin.venice.meta;

/**
 * This interface define the operations used to manage all routers in a cluster. Each router could know how many
 * routers are still living through the implementation of this interface.
 */
public interface RoutersClusterManager {
  /**
   * Register the given router instance into the cluster.
   */
  void registerRouter(String instanceId);

  /**
   * Unregister the given router instance from the cluster.
   */
  void unregisterRouter(String instanceId);

  /**
   * Get how many routers are living right now in the cluster.
   */
  int getLiveRoutersCount();

  /**
   * Get the expected number of routers in the cluster.
   */
  int getExpectedRoutersCount();

  /**
   * Update the expected number of routers in the cluster.
   */
  void updateExpectedRouterCount(int expectedNumber);

  /**
   * Listen on the router count, get the notification once it's changed.
   */
  void subscribeRouterCountChangedEvent(RouterCountChangedListener listener);

  /**
   * Stop listening on the router count.
   */
  void unSubscribeRouterCountChangedEvent(RouterCountChangedListener listener);

  /**
   * Listen on the router cluster config, get the notification once the config is changed.
   *
   * @param listener
   */
  void subscribeRouterClusterConfigChangedEvent(RouterClusterConfigChangedListener listener);

  /**
   * Stop listening on the router cluster config.
   *
   * @param listener
   */
  void unSubscribeRouterClusterConfighangedEvent(RouterClusterConfigChangedListener listener);

  boolean isThrottlingEnabled();

  boolean isMaxCapacityProtectionEnabled();

  /**
   * Enable or disable read throttling feature on router. If this feature is disable, router will accept all read
   * request regardless of store's quota.
   */
  void enableThrottling(boolean enable);

  /**
   * Enable or disable max read capacity protection feature on router. If this feature is disabled, router will not
   * protected by pre-defined max read capacity which means the total quota one router assigned might exceed the max
   * capacity that router could serve.
   */
  void enableMaxCapacityProtection(boolean enable);

  void createRouterClusterConfig();

  interface RouterCountChangedListener {
    void handleRouterCountChanged(int newRouterCount);
  }

  interface RouterClusterConfigChangedListener {
    void handleRouterClusterConfigChanged(RoutersClusterConfig newConfig);
  }
}
