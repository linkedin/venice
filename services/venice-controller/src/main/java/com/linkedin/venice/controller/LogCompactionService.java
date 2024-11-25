package com.linkedin.venice.controller;

import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.service.AbstractVeniceService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class LogCompactionService extends AbstractVeniceService {
  private static final String SCHEDULED_TRIGGER = "Scheduled";
  private static final String MANUAL_TRIGGER = "Manual";

  private static final int SCHEDULED_EXECUTOR_TIMEOUT_S = 60;
  public static final int PRE_EXECUTION_DELAY_HR = 0;

  private final Admin admin;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  final ScheduledExecutorService executor;

  public LogCompactionService(Admin admin, VeniceControllerMultiClusterConfig multiClusterConfigs) {
    this.admin = admin;
    this.multiClusterConfigs = multiClusterConfigs;

    executor = Executors.newScheduledThreadPool(multiClusterConfigs.getScheduledLogCompactionThreadCount());
  }

  @Override
  public boolean startInner() throws Exception {
    executor.scheduleAtFixedRate(
        new LogCompactionTask(SCHEDULED_TRIGGER),
        PRE_EXECUTION_DELAY_HR,
        multiClusterConfigs.getScheduledLogCompactionIntervalHR(),
        TimeUnit.HOURS);
    return false;
  }

  @Override
  public void stopInner() throws Exception {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(SCHEDULED_EXECUTOR_TIMEOUT_S, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
    }
  }

  // TODO: LogCompactionTask
  private class LogCompactionTask implements Runnable {
    private final String triggerSource;

    private LogCompactionTask(String triggerSource) {
      this.triggerSource = triggerSource;
    }

    @Override
    public void run() {
      // admin.triggerRepush()
      for (String clusterName: multiClusterConfigs.getClusters()) {
        for (StoreInfo storeInfo: admin.getStoresForCompaction(clusterName)) {
          // TODO: response = RepushOrchestratorProvider.repush(storeInfo.getName();)
          // TODO: if response is not success, log error
        }
      }
    }
  }
}
