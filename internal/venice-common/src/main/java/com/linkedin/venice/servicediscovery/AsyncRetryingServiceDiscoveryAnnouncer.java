package com.linkedin.venice.servicediscovery;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is responsible for announcing and de-announcing a {@link ServiceDiscoveryAnnouncer} to a Service Discovery system
 * and taking care of failed registrations by retrying them asynchronously.
 */
public class AsyncRetryingServiceDiscoveryAnnouncer implements ServiceDiscoveryAnnouncer {
  private static final Logger LOGGER = LogManager.getLogger(AsyncRetryingServiceDiscoveryAnnouncer.class);
  private final List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers;
  private final Thread serviceDiscoveryAnnouncerRetryThread;
  private final BlockingQueue<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncerRetryQueue =
      new LinkedBlockingQueue<>();

  public AsyncRetryingServiceDiscoveryAnnouncer(
      List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers,
      long serviceDiscoveryRegistrationRetryMS) {
    this.serviceDiscoveryAnnouncers = serviceDiscoveryAnnouncers;
    ServiceDiscoveryAnnouncerRetryTask serviceDiscoveryAnnouncerRetryTask = new ServiceDiscoveryAnnouncerRetryTask(
        serviceDiscoveryAnnouncerRetryQueue,
        serviceDiscoveryRegistrationRetryMS);
    this.serviceDiscoveryAnnouncerRetryThread = new Thread(serviceDiscoveryAnnouncerRetryTask);
  }

  /**
   * Registers each {@link ServiceDiscoveryAnnouncer} in {@code serviceDiscoveryAnnouncers}.
   * If a service discovery announcer fails to register, it is added to the retry queue and registration will be retried asynchronously
   * in a separate thread.
   */
  @Override
  public void register() {
    serviceDiscoveryAnnouncers.forEach(serviceDiscoveryAnnouncer -> {
      try {
        serviceDiscoveryAnnouncer.register();
        LOGGER.info("Registered to service discovery: {}", serviceDiscoveryAnnouncer);
      } catch (Exception e) {
        LOGGER.error("Failed to register to service discovery: {}", serviceDiscoveryAnnouncer, e);
        serviceDiscoveryAnnouncerRetryQueue.add(serviceDiscoveryAnnouncer);
      }
    });
    if (!serviceDiscoveryAnnouncerRetryQueue.isEmpty()) {
      LOGGER.info("Starting service discovery announcer retry thread");
      serviceDiscoveryAnnouncerRetryThread.start();
    }
  }

  /**
   * Unregisters each {@link ServiceDiscoveryAnnouncer} in {@code serviceDiscoveryAnnouncers}.
   * One reason that a service discovery announcer may fail to unregister is if it was never successfully registered.
   */
  @Override
  public void unregister() {
    if (serviceDiscoveryAnnouncerRetryThread.isAlive()) {
      LOGGER.info("Stopping service discovery announcer retry thread");
      serviceDiscoveryAnnouncerRetryThread.interrupt();
    }
    serviceDiscoveryAnnouncers.forEach(serviceDiscoveryAnnouncer -> {
      try {
        serviceDiscoveryAnnouncer.unregister();
        LOGGER.info("Unregistered from service discovery: {}", serviceDiscoveryAnnouncer);
      } catch (Exception e) {
        LOGGER.error("Failed to unregister from service discovery: {}", serviceDiscoveryAnnouncer, e);
      }
    });
  }

  Thread getServiceDiscoveryAnnouncerRetryThread() {
    return serviceDiscoveryAnnouncerRetryThread;
  }

  BlockingQueue<ServiceDiscoveryAnnouncer> getServiceDiscoveryAnnouncerRetryQueue() {
    return serviceDiscoveryAnnouncerRetryQueue;
  }
}
