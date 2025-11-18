package com.linkedin.venice.controller.multitaskscheduler;

import com.linkedin.venice.service.AbstractVeniceService;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 *  This service functions as an interface for scheduling various kind of tasks by loading multiple different task managers.
 *  Each task manager is assigned a java executor to enable scheduling and executing tasks in parallel.
 *
 *  The life cycle of this service matches the life cycle of {@link HelixVeniceClusterResources}, which means there is one
 *  multitask scheduler service per cluster. The multitask scheduler service is built when the controller is promoted
 *  to leader role for the cluster.
 *
 *  Currently, store migration manager is initialized and loaded inside within this service, it is responsible for managing
 *  the store migration process, and it is the only task manager currently loaded into this service. For each service per
 *  cluster, the store migration task is scheduled inside task scheduler service of the source cluster
 *
 */
public class MultiTaskSchedulerService extends AbstractVeniceService {
  private final StoreMigrationManager storeMigrationManager;
  private static final Logger LOGGER = LogManager.getLogger(MultiTaskSchedulerService.class);

  /**
   * Constructor to initialize the TaskSchedulerService with the task manager with pre-configured threadPoolSize and maxRetryAttempts.
   * @param threadPoolSize
   * @param maxRetryAttempts
   * @param taskIntervalInSecond
   * @param childFabricList
   */
  public MultiTaskSchedulerService(
      int threadPoolSize,
      int maxRetryAttempts,
      int taskIntervalInSecond,
      List<String> childFabricList) {
    this.storeMigrationManager = StoreMigrationManager
        .createStoreMigrationManager(threadPoolSize, maxRetryAttempts, taskIntervalInSecond, childFabricList);
  }

  @Override
  public boolean startInner() throws Exception {
    LOGGER.info("MultiTaskScheduler Service is initialized.");
    LOGGER.info("StoreMigrationManager has been loaded into MultiTaskScheduler Service.");
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    LOGGER.info("MultiTaskScheduler service starts shutting down: ");
    storeMigrationManager.shutdown();
  }

  public StoreMigrationManager getStoreMigrationManager() {
    return storeMigrationManager;
  }
}
