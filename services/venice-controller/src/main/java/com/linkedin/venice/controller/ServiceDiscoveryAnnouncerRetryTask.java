package com.linkedin.venice.controller;

import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import java.util.concurrent.BlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is responsible for retrying the registration of a {@link ServiceDiscoveryAnnouncer} in the case of registration failure.
 * It is a runnable task that is scheduled to register a {@link ServiceDiscoveryAnnouncer} every
 * {@link ServiceDiscoveryAnnouncerRetryTask#serviceDiscoveryRegistrationRetryMS} milliseconds.
 */
public class ServiceDiscoveryAnnouncerRetryTask implements Runnable {
  private static final Logger LOGGER = LogManager.getLogger(ServiceDiscoveryAnnouncerRetryTask.class);
  private final BlockingQueue<ServiceDiscoveryAnnouncer> retryQueue;
  private final long serviceDiscoveryRegistrationRetryMS;

  public ServiceDiscoveryAnnouncerRetryTask(
      BlockingQueue<ServiceDiscoveryAnnouncer> retryQueue,
      long serviceDiscoveryRegistrationRetryMS) {
    this.retryQueue = retryQueue;
    this.serviceDiscoveryRegistrationRetryMS = serviceDiscoveryRegistrationRetryMS;
  }

  @Override
  public void run() {
    while (true) {
      ServiceDiscoveryAnnouncer announcer = null;
      try {
        LOGGER.info("Thread for retry task sleeping for {} ms", serviceDiscoveryRegistrationRetryMS);
        Thread.sleep(serviceDiscoveryRegistrationRetryMS);
        LOGGER.info("Thread for retry task woke up");
        announcer = retryQueue.take();
        announcer.register();
        LOGGER.info("Registered to service discovery: {}", announcer);
      } catch (InterruptedException e) {
        LOGGER.error("ServiceDiscoveryAnnouncerRetryTask interrupted", e);
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        LOGGER.error("Failed to register to service discovery: {}", announcer, e);
        if (announcer != null) {
          retryQueue.add(announcer);
        }
      }
    }
  }
}
