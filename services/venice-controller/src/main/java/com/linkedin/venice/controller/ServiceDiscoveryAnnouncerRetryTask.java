package com.linkedin.venice.controller;

import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import java.util.concurrent.BlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is responsible for retrying the registration of a {@link ServiceDiscoveryAnnouncer} in the case of registration failure.
 * It is a runnable task that is scheduled to register a {@link ServiceDiscoveryAnnouncer} every {@link ServiceDiscoveryAnnouncerRetryTask#retryRegisterServiceDiscoveryAnnouncerMS} milliseconds.
 */
public class ServiceDiscoveryAnnouncerRetryTask implements Runnable {
  private static final Logger LOGGER = LogManager.getLogger(ServiceDiscoveryAnnouncerRetryTask.class);
  private final BlockingQueue<ServiceDiscoveryAnnouncer> retryQueue;
  private final Long retryRegisterServiceDiscoveryAnnouncerMS;

  public ServiceDiscoveryAnnouncerRetryTask(
      BlockingQueue<ServiceDiscoveryAnnouncer> retryQueue,
      Long retryRegisterServiceDiscoveryAnnouncerMS) {
    this.retryQueue = retryQueue;
    this.retryRegisterServiceDiscoveryAnnouncerMS = retryRegisterServiceDiscoveryAnnouncerMS;
  }

  @Override
  public void run() {
    while (true) {
      ServiceDiscoveryAnnouncer announcer = null;
      try {
        LOGGER.info("Thread for retry task sleeping for {} ms", retryRegisterServiceDiscoveryAnnouncerMS);
        Thread.sleep(retryRegisterServiceDiscoveryAnnouncerMS);
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
