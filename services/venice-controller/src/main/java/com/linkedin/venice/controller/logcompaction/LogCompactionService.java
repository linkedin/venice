package com.linkedin.venice.controller.logcompaction;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.repush.RepushJobRequest;
import com.linkedin.venice.controllerapi.RepushJobResponse;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.LogContext;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This service is in charge of scheduling periodic log compaction & utilising {@link VeniceHelixAdmin} to trigger log compaction.
 *
 * The service runs in child controllers & initialised in {@link com.linkedin.venice.controller.VeniceController}.
 *
 * Workflow:
 * 1. schedules {@link LogCompactionTask} periodically to perform log compaction for all stores in the cluster
 * controlled by the {@link com.linkedin.venice.controller.VeniceController} instance that runs this LogCompactionService instance
 * 2. checks for stores that are ready for log compaction with function {@link VeniceHelixAdmin#getStoresForCompaction(String)}
 * 3. triggers compaction for each store with function {@link VeniceHelixAdmin#repushStore(RepushJobRequest)}
 *
 * See {@link CompactionManager} for the logic to determine if a store is ready for compaction
 */
public class LogCompactionService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(LogCompactionService.class);

  private static final int SCHEDULED_EXECUTOR_TIMEOUT_S = 60;
  public static final int PRE_EXECUTION_DELAY_MS = 0;

  private final Admin admin;
  private final String clusterName;
  private final VeniceControllerClusterConfig clusterConfigs;
  final ScheduledExecutorService executor;

  public LogCompactionService(Admin admin, String clusterName, VeniceControllerClusterConfig clusterConfigs) {
    this.admin = admin;
    this.clusterName = clusterName;

    this.clusterConfigs = clusterConfigs;

    executor = Executors.newScheduledThreadPool(clusterConfigs.getLogCompactionThreadCount());
  }

  @Override
  public boolean startInner() throws Exception {
    executor.scheduleAtFixedRate(
        new LogCompactionTask(clusterName),
        PRE_EXECUTION_DELAY_MS,
        clusterConfigs.getLogCompactionIntervalMS(),
        TimeUnit.MILLISECONDS);
    LOGGER.info("log compaction service is started");
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(SCHEDULED_EXECUTOR_TIMEOUT_S, TimeUnit.SECONDS)) {
        executor.shutdownNow();
        LOGGER.info("log compaction service executor shutdown timed out and is forcefully shutdown");
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      LOGGER.info("log compaction service interrupted");
    }
  }

  private class LogCompactionTask implements Runnable {
    private final String clusterName;

    private LogCompactionTask(String clusterName) {
      this.clusterName = clusterName;
    }

    @Override
    public void run() {
      LogContext.setStructuredLogContext(clusterConfigs.getLogContext());
      try {
        compactStoresInClusters();
      } catch (Throwable e) {
        LOGGER.error("Non-Exception Throwable caught", e);
      }
    }

    private void compactStoresInClusters() {
      for (StoreInfo storeInfo: admin.getStoresForCompaction(clusterName)) {
        try {
          RepushJobResponse response = admin
              .repushStore(new RepushJobRequest(clusterName, storeInfo.getName(), RepushJobRequest.SCHEDULED_TRIGGER));
          LOGGER.info(
              "log compaction triggered for cluster: {} store: {} | execution ID: {}",
              clusterName,
              response.getName(),
              response.getExecutionId());
        } catch (Exception e) {
          LOGGER.error(
              "Error checking if store is ready for log compaction for cluster: {} store: {}",
              clusterName,
              storeInfo.getName(),
              e);
          // TODO LC: add metrics for log compaction failures
        }
      }
    }
  }
}
