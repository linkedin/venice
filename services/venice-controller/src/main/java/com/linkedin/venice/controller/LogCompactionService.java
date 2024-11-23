package com.linkedin.venice.controller;

import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.service.AbstractVeniceService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


public class LogCompactionService extends AbstractVeniceService {
  private final Admin admin;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final ScheduledExecutorService executor;

  public LogCompactionService(Admin admin, VeniceControllerMultiClusterConfig multiClusterConfigs) {
    this.admin = admin;
    this.multiClusterConfigs = multiClusterConfigs;

    executor = Executors.newScheduledThreadPool(multiClusterConfigs.getScheduledLogCompactionThreadCount());
    // TODO: get schedulerThreadCount from MultiClusterConfigs
    // if no config, set to 1
    // TODO: initialise scheduler
  }

  @Override
  public boolean startInner() throws Exception {
    // TODO: start schedule
    return false;
  }

  @Override
  public void stopInner() throws Exception {
    // TODO: stop scheduler
  }

  // TODO: LogCompactionTask
  private class LogCompactionTask implements Runnable {
    private final String triggerSource; // TODO: enums for this

    private LogCompactionTask(String triggerSource) {
      this.triggerSource = triggerSource;
    }

    //

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
