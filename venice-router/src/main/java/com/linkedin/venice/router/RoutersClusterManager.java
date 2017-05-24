package com.linkedin.venice.router;

/**
 * This interface define the operations used to manage all routers in a cluster. Each router could know how many
 * routers are still living through the implementation of this interface.
 */
public interface RoutersClusterManager {
  /**
   * Register the given router instance into the cluster.
   */
  void registerCurrentRouter();

  /**
   * Unregister the given router instance from the cluster.
   */
  void unregisterCurrentRouter();

  /**
   * Get how many routers are living right now in the cluster.
   */
  int getRoutersCount();

  /**
   * Listen on the router count, get the notification once it's changed.
   */
  void subscribeRouterCountChangedEvent(RouterCountChangedListener listener);
  /**
   * Stop listening on the router count.
   */
  void unSubscribeRouterCountChangedEvent(RouterCountChangedListener listener);

  interface RouterCountChangedListener {
    void handleRouterCountChanged(int newRouterCount);
  }
}
