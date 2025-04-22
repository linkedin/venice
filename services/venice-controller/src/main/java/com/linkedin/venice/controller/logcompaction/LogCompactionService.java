package com.linkedin.venice.controller.logcompaction;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.repush.RepushJobRequest;
import com.linkedin.venice.controllerapi.RepushJobResponse;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.LogContext;
import java.util.Set;
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
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  final ScheduledExecutorService executor;

  public LogCompactionService(Admin admin, VeniceControllerMultiClusterConfig multiClusterConfigs) {
    this.admin = admin;
    this.multiClusterConfigs = multiClusterConfigs;

    executor = Executors.newScheduledThreadPool(multiClusterConfigs.getLogCompactionThreadCount());
  }

  @Override
  public boolean startInner() throws Exception {
    executor.scheduleAtFixedRate(
        new LogCompactionTask(multiClusterConfigs.getClusters()),
        PRE_EXECUTION_DELAY_MS,
        multiClusterConfigs.getLogCompactionIntervalMS(),
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
    private final Set<String> clusters;

    private LogCompactionTask(Set<String> clusters) {
      this.clusters = clusters;
    }

    @Override
    public void run() {
      LogContext.setStructuredLogContext(multiClusterConfigs.getLogContext());
      try {
        compactStoresInClusters();
      } catch (Throwable e) {
        LOGGER.error("Non-Exception Throwable caught", e);
      }
    }

    private void compactStoresInClusters() {
      for (String clusterName: clusters) {
        for (StoreInfo storeInfo: admin.getStoresForCompaction(clusterName)) {
          try {
            RepushJobResponse response =
                admin.repushStore(new RepushJobRequest(storeInfo.getName(), RepushJobRequest.SCHEDULED_TRIGGER));
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
}
