package com.linkedin.venice.controller;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;


public class DelayedVersionSwapService extends AbstractVeniceService {
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private final Set<String> allClusters;
  private final VeniceControllerMultiClusterConfig _veniceControllerMultiClusterConfig;
  private final VeniceHelixAdmin _veniceHelixAdmin;
  private final Thread delayedVersionSwapThread;
  private final Time time;

  public DelayedVersionSwapService(
      VeniceHelixAdmin veniceHelixAdmin,
      VeniceControllerMultiClusterConfig veniceControllerMultiClusterConfig) {
    _veniceHelixAdmin = veniceHelixAdmin;
    _veniceControllerMultiClusterConfig = veniceControllerMultiClusterConfig;
    allClusters = veniceControllerMultiClusterConfig.getClusters();
    delayedVersionSwapThread = new Thread(new DelayedVersionSwapTask(), "DelayedVersionSwapTask");
    time = new SystemTime();
  }

  @Override
  public boolean startInner() throws Exception {
    delayedVersionSwapThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stop.set(true);
    delayedVersionSwapThread.interrupt();
  }

  private class DelayedVersionSwapTask implements Runnable {
    @Override
    public void run() {
      while (!stop.get()) {
        for (String clusterName: allClusters) {
          List<Store> stores = _veniceHelixAdmin.getAllStores(clusterName);
          for (Store store: stores) {
            // Check if current time > waitTime, someway to know when push finished for the target colo?

          }
        }
      }
    }
  }
}
