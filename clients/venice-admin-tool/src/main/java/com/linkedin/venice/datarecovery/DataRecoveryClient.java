package com.linkedin.venice.datarecovery;

import static com.linkedin.venice.datarecovery.DataRecoveryWorker.INTERVAL_UNSET;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.RegionPushDetailsResponse;
import com.linkedin.venice.utils.Utils;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * DataRecoveryClient is the central class to manage data recovery feature.
 * It contains three main modules: a data recovery planner, a data recovery executor, and a data recovery monitor.
 * Depending on the different use cases, it delegates the tasks to a specific module.
 */
public class DataRecoveryClient {
  private static final Logger LOGGER = LogManager.getLogger(DataRecoveryClient.class);
  private final DataRecoveryExecutor executor;
  private final DataRecoveryEstimator estimator;
  private final DataRecoveryMonitor monitor;

  public DataRecoveryClient() {
    this(new DataRecoveryExecutor(), new DataRecoveryMonitor(), new DataRecoveryEstimator());
  }

  public DataRecoveryClient(
      DataRecoveryExecutor executor,
      DataRecoveryMonitor monitor,
      DataRecoveryEstimator estimator) {
    this.executor = executor;
    this.monitor = monitor;
    this.estimator = estimator;
  }

  public DataRecoveryExecutor getExecutor() {
    return executor;
  }

  public DataRecoveryEstimator getEstimator() {
    return estimator;
  }

  public DataRecoveryMonitor getMonitor() {
    return monitor;
  }

  public void filterStoresWithOngoingRepush(Set<String> storesList, ControllerClient cli, LocalDateTime timestamp) {
    for (String s: storesList) {
      try {
        RegionPushDetailsResponse regionPushDetailsResponse = cli.getRegionPushDetails(s, false);
        String latestTimestamp = regionPushDetailsResponse.getRegionPushDetails().getPushStartTimestamp();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss");
        LocalDateTime latestPushStartTime = LocalDateTime.parse(latestTimestamp, formatter);
        if (latestPushStartTime.compareTo(timestamp) > 0) {
          LOGGER.warn(
              "Removing store " + s
                  + " from stores list, as the has a push job started more recently than the input timestamp.");
          storesList.remove(s);
        }
      } catch (Exception e) {
        LOGGER.warn("Removing store " + s + "from stores list due to exception " + e);
        storesList.remove(s);
      }
    }
  }

  public void execute(DataRecoveryParams drParams, StoreRepushCommand.Params cmdParams) {
    Set<String> storeNames = drParams.getRecoveryStores();
    filterStoresWithOngoingRepush(storeNames, cmdParams.getPCtrlCliWithoutCluster(), cmdParams.getTimestamp());
    if (storeNames == null || storeNames.isEmpty()) {
      LOGGER.warn("store list is empty, exit.");
      return;
    }
    if (!drParams.isNonInteractive && !confirmStores(storeNames)) {
      return;
    }

    getExecutor().perform(storeNames, cmdParams);
    getExecutor().shutdownAndAwaitTermination();
  }

  public Long estimateRecoveryTime(DataRecoveryParams drParams, EstimateDataRecoveryTimeCommand.Params cmdParams) {
    Set<String> storeNames = drParams.getRecoveryStores();
    if (storeNames == null || storeNames.isEmpty()) {
      LOGGER.warn("store list is empty, exit.");
      return 0L;
    }

    getEstimator().perform(storeNames, cmdParams);
    getEstimator().shutdownAndAwaitTermination();
    Long totalRecoveryTime = 0L;
    for (DataRecoveryTask t: getEstimator().getTasks()) {
      totalRecoveryTime += ((EstimateDataRecoveryTimeCommand.Result) t.getTaskResult().getCmdResult())
          .getEstimatedRecoveryTimeInSeconds();
    }
    return totalRecoveryTime;
  }

  public void monitor(DataRecoveryParams drParams, MonitorCommand.Params monitorParams) {
    Set<String> storeNames = drParams.getRecoveryStores();
    if (storeNames == null || storeNames.isEmpty()) {
      LOGGER.warn("store list is empty, exit.");
      return;
    }

    getMonitor().setInterval(drParams.interval);
    getMonitor().perform(storeNames, monitorParams);
    getMonitor().shutdownAndAwaitTermination();
  }

  public boolean confirmStores(Set<String> storeNames) {
    LOGGER.info("stores to recover: " + storeNames);
    LOGGER.info("Recover " + storeNames.size() + " stores, please confirm (yes/no) [y/n]:");
    Scanner in = new Scanner(System.in);
    String line = in.nextLine();
    return line.equalsIgnoreCase("yes") || line.equalsIgnoreCase("y");
  }

  public static class DataRecoveryParams {
    private final String multiStores;
    private final Set<String> recoveryStores;
    private boolean isNonInteractive = false;
    private int interval = INTERVAL_UNSET;

    public DataRecoveryParams(String multiStores) {
      this.multiStores = multiStores;
      this.recoveryStores = calculateRecoveryStoreNames(this.multiStores);
    }

    public Set<String> getRecoveryStores() {
      return recoveryStores;
    }

    private Set<String> calculateRecoveryStoreNames(String multiStores) {
      Set<String> storeNames = null;
      if (multiStores != null && !multiStores.isEmpty()) {
        storeNames = Utils.parseCommaSeparatedStringToSet(multiStores);
      }
      return storeNames;
    }

    public void setInterval(int interval) {
      this.interval = interval;
    }

    public void setNonInteractive(boolean isNonInteractive) {
      this.isNonInteractive = isNonInteractive;
    }
  }
}
