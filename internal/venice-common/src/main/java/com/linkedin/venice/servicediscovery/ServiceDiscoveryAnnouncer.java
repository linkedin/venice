package com.linkedin.venice.servicediscovery;

/**
 * This interface is used to announce the service to a Service Discovery system on startup and de-announce the service
 * on graceful shutdown. The Service Discovery system should gracefully handle de-announcing in case of crash-failures.
 */
public interface ServiceDiscoveryAnnouncer {
  /**
   * Register the instance with the service discovery system.
   */
  void register();

  /**
   * Unregister the instance from the service discovery system.
   */
  void unregister();
}
