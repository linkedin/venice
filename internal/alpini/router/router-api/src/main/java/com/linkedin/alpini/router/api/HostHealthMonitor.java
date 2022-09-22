package com.linkedin.alpini.router.api;

import javax.annotation.Nonnull;


/**
 * This is an interface for monitoring the healthiness of storage nodes. If a storage host is not responsive,
 * the router may re-route requests to other hosts according to the routing policy.
 */
public interface HostHealthMonitor<H> {
  /**
   * To check if a host is healthy/responsive.
   *
   * @param hostName the host name, including the service port
   * @return the host is healthy or not
   */
  boolean isHostHealthy(@Nonnull H hostName, @Nonnull String partitionName);
}
