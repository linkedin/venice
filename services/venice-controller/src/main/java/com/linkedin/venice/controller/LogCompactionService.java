package com.linkedin.venice.controller;

import com.linkedin.venice.service.AbstractVeniceService;


public class LogCompactionService extends AbstractVeniceService {
  public LogCompactionService() {
    // get schedulerThreadCount from MultiClusterConfigs
    // if no config, set to 1
    // initialise scheduler
  }

  @Override
  public boolean startInner() throws Exception {
    // TODO: schedule
    return false;
  }

  @Override
  public void stopInner() throws Exception {
    // TODO: stop scheduler
  }

  // TODO: LogCompactionTask
  private class TopicCleanupTask implements Runnable {
    @Override
    public void run() {
      // get cluster name
      // get stores for compaction
      // for each store,
      // response = RepushOrchestratorProvider.repush(storeName)
      // if response is not success, log error
    }
  }
}
