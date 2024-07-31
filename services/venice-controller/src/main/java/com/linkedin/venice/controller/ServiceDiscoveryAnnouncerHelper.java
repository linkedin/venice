package com.linkedin.venice.controller;

import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ServiceDiscoveryAnnouncerHelper {
  private static final Logger LOGGER = LogManager.getLogger(ServiceDiscoveryAnnouncerHelper.class);

  /**
   * Registers each service discovery announcer in {@code serviceDiscoveryAnnouncers}.
   * If registration fails, the service discovery announcer is added to a retry queue and registration will be retried in a separate thread.
   */
  public static void registerServiceDiscoveryAnnouncers(
      List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers,
      BlockingQueue<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncerRetryQueue) {
    serviceDiscoveryAnnouncers.forEach(serviceDiscoveryAnnouncer -> {
      try {
        serviceDiscoveryAnnouncer.register();
        LOGGER.info("Registered to service discovery: {}", serviceDiscoveryAnnouncer);
      } catch (Exception e) {
        LOGGER.error("Failed to register to service discovery: {}", serviceDiscoveryAnnouncer, e);
        serviceDiscoveryAnnouncerRetryQueue.add(serviceDiscoveryAnnouncer);
      }
    });
  }

  /**
   * Unregisters each service discovery announcer in {@code serviceDiscoveryAnnouncers}.
   * One reason that a service discovery announcer may fail to unregister is if it was never successfully registered.
   * @return a map of service discovery announcers to log messages (for testing purposes)
   */
  public static Map<ServiceDiscoveryAnnouncer, String> unregisterServiceDiscoveryAnnouncers(
      List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers) {
    Map<ServiceDiscoveryAnnouncer, String> map = new HashMap<>();
    serviceDiscoveryAnnouncers.forEach(serviceDiscoveryAnnouncer -> {
      try {
        serviceDiscoveryAnnouncer.unregister();
        LOGGER.info("Unregistered from service discovery: {}", serviceDiscoveryAnnouncer);
        map.put(serviceDiscoveryAnnouncer, "Unregistered from service discovery");
      } catch (Exception e) {
        LOGGER.error("Failed to unregister from service discovery: {}", serviceDiscoveryAnnouncer, e);
        map.put(serviceDiscoveryAnnouncer, "Failed to unregister from service discovery");
      }
    });
    return map;
  }
}
