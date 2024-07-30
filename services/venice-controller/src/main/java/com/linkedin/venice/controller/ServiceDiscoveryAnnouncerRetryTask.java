package com.linkedin.venice.controller;

import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import java.util.concurrent.BlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is responsible for retrying the registration of a {@link ServiceDiscoveryAnnouncer} in case of registration failure.
 */
public class ServiceDiscoveryAnnouncerRetryTask implements Runnable {
  private static final Logger LOGGER = LogManager.getLogger(ServiceDiscoveryAnnouncerRetryTask.class);
  private final BlockingQueue<ServiceDiscoveryAnnouncer> retryQueue;
  private final Long retryRegisterServiceDiscoveryAnnouncerMS = 30000L;

  public ServiceDiscoveryAnnouncerRetryTask(BlockingQueue<ServiceDiscoveryAnnouncer> retryQueue) {
    this.retryQueue = retryQueue;
  }

  @Override
  public void run() {
    while (true) {
      ServiceDiscoveryAnnouncer announcer = retryQueue.poll();
      if (announcer != null) {
        try {
          announcer.register();
          LOGGER.info("Registered to service discovery: {}", announcer);
        } catch (Exception e) {
          LOGGER.error("Failed to register to service discovery: {}", announcer, e);
          retryQueue.add(announcer);
        }
      }
      try {
        Thread.sleep(retryRegisterServiceDiscoveryAnnouncerMS);
      } catch (InterruptedException e) {
        LOGGER.error("ServiceDiscoveryAnnouncerRetryTask interrupted", e);
        Thread.currentThread().interrupt();
        break;
      }
    }
  }
}
