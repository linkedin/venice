package com.linkedin.venice.servicediscovery;

import java.util.concurrent.BlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is responsible for retrying the registration of a {@link ServiceDiscoveryAnnouncer} in the case of registration failure.
 * It is a runnable task that is scheduled to register a service discovery announcer every
 * {@link ServiceDiscoveryAnnouncerRetryTask#serviceDiscoveryRegistrationRetryMS} milliseconds.
 */
public class ServiceDiscoveryAnnouncerRetryTask implements Runnable {
  private static final Logger LOGGER = LogManager.getLogger(ServiceDiscoveryAnnouncerRetryTask.class);
  private final BlockingQueue<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncerRetryQueue;
  private final long serviceDiscoveryRegistrationRetryMS;

  public ServiceDiscoveryAnnouncerRetryTask(
      BlockingQueue<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncerRetryQueue,
      long serviceDiscoveryRegistrationRetryMS) {
    this.serviceDiscoveryAnnouncerRetryQueue = serviceDiscoveryAnnouncerRetryQueue;
    this.serviceDiscoveryRegistrationRetryMS = serviceDiscoveryRegistrationRetryMS;
  }

  @Override
  public void run() {
    while (true) {
      ServiceDiscoveryAnnouncer announcer = null;
      try {
        announcer = serviceDiscoveryAnnouncerRetryQueue.take();
        announcer.register();
        LOGGER.info("Registered to service discovery: {}", announcer);
      } catch (InterruptedException e) {
        LOGGER.error("ServiceDiscoveryAnnouncerRetryTask interrupted", e);
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        LOGGER.error("Failed to register to service discovery: {}", announcer, e);
        if (announcer != null) {
          serviceDiscoveryAnnouncerRetryQueue.add(announcer);
        }
      }
      try {
        Thread.sleep(serviceDiscoveryRegistrationRetryMS);
      } catch (InterruptedException e) {
        LOGGER.error("ServiceDiscoveryAnnouncerRetryTask interrupted", e);
        Thread.currentThread().interrupt();
        break;
      }
    }
  }
}
