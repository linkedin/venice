package com.linkedin.venice.servicediscovery;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is responsible for retrying the registration of a {@link ServiceDiscoveryAnnouncer} in the case of registration failure.
 * It is a runnable task that is scheduled to register each service discovery announcer every
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

  /**
   * If the retry queue is empty, the task will wait until there is an available element in the queue.
   * Each service discovery announcer will retry registration every {@link ServiceDiscoveryAnnouncerRetryTask#serviceDiscoveryRegistrationRetryMS} milliseconds.
   */
  @Override
  public void run() {
    while (!serviceDiscoveryAnnouncerRetryQueue.isEmpty()) {
      try {
        ServiceDiscoveryAnnouncer availableAnnouncer = serviceDiscoveryAnnouncerRetryQueue.take();
        List<ServiceDiscoveryAnnouncer> announcerList = new ArrayList<>();
        announcerList.add(availableAnnouncer);
        serviceDiscoveryAnnouncerRetryQueue.drainTo(announcerList);
        for (ServiceDiscoveryAnnouncer announcer: announcerList) {
          try {
            announcer.register();
            LOGGER.info("Registered to service discovery: {}", announcer);
          } catch (Exception e) {
            LOGGER.error("Failed to register to service discovery: {}", announcer, e);
            serviceDiscoveryAnnouncerRetryQueue.put(announcer);
          }
        }
        Thread.sleep(serviceDiscoveryRegistrationRetryMS);
      } catch (InterruptedException e) {
        LOGGER.error("ServiceDiscoveryAnnouncerRetryTask interrupted", e);
        Thread.currentThread().interrupt();
        break;
      }
    }
  }
}
