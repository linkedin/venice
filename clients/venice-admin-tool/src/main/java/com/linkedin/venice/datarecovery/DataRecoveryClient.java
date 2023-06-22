package com.linkedin.venice.datarecovery;

import static com.linkedin.venice.datarecovery.DataRecoveryWorker.INTERVAL_UNSET;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.Utils;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
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

  public Map<String, Pair<Boolean, String>> getRepushViability(
      Set<String> storesList,
      StoreRepushCommand.Params params) {
    Map<String, Pair<Boolean, String>> ret = new HashMap<>();
    String url = params.getUrl();
    ControllerClient cli = params.getPCtrlCliWithoutCluster();
    LocalDateTime timestamp = params.getTimestamp();
    String destFabric = params.getDestFabric();
    for (String s: storesList) {
      try {
        String clusterName = cli.discoverCluster(s).getCluster();
        if (clusterName == null) {
          ret.put(s, Pair.of(false, "unable to discover cluster for store (likely invalid store name)"));
          continue;
        }
        try (ControllerClient parentCtrlCli = buildControllerClient(clusterName, url, params.getSSLFactory())) {
          StoreHealthAuditResponse storeHealthInfo = parentCtrlCli.listStorePushInfo(s, false);
          Map<String, RegionPushDetails> regionPushDetails = storeHealthInfo.getRegionPushDetails();
          if (!regionPushDetails.containsKey(destFabric)) {
            ret.put(s, Pair.of(false, "nothing to repush, store version 0"));
            continue;
          }
          String latestTimestamp = regionPushDetails.get(destFabric).getPushStartTimestamp();
          LocalDateTime latestPushStartTime =
              LocalDateTime.parse(latestTimestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME);

          if (latestPushStartTime.isAfter(timestamp)) {
            ret.put(s, Pair.of(false, "input timestamp earlier than latest push"));
            continue;
          }

          MultiStoreStatusResponse response = parentCtrlCli.getFutureVersions(clusterName, s);
          // No future version status for target region.
          if (!response.getStoreStatusMap().containsKey(destFabric)) {
            ret.put(s, Pair.of(true, StringUtils.EMPTY));
            continue;
          }

          int futureVersion = Integer.parseInt(response.getStoreStatusMap().get(destFabric));
          // No ongoing offline pushes detected for target region.
          if (futureVersion == Store.NON_EXISTING_VERSION) {
            ret.put(s, Pair.of(true, StringUtils.EMPTY));
            continue;
          }
          // Find ongoing pushes for this store, skip.
          ret.put(s, Pair.of(false, String.format("find ongoing push, version: %d", futureVersion)));
        }
      } catch (VeniceException e) {
        ret.put(s, Pair.of(false, "VeniceHttpException " + e.getErrorType().toString()));
      }
    }
    return ret;
  }

  public ControllerClient buildControllerClient(String clusterName, String url, Optional<SSLFactory> sslFactory) {
    return new ControllerClient(clusterName, url, sslFactory);
  }

  public void execute(DataRecoveryParams drParams, StoreRepushCommand.Params cmdParams) {
    Set<String> storeNames = drParams.getRecoveryStores();
    Map<String, Pair<Boolean, String>> pushMap = getRepushViability(storeNames, cmdParams);
    Set<String> filteredStoreNames = new HashSet<>();

    for (Map.Entry<String, Pair<Boolean, String>> e: pushMap.entrySet()) {
      if (e.getValue().getLeft()) {
        filteredStoreNames.add(e.getKey());
      } else {
        this.getExecutor().getSkippedStores().add(e.getKey());
      }
    }

    if (!filteredStoreNames.isEmpty()) {
      if (!drParams.isNonInteractive && !confirmStores(filteredStoreNames)) {
        return;
      }
      getExecutor().perform(filteredStoreNames, cmdParams);
    } else {
      LOGGER.warn("store list is empty, exit.");
    }

    // check if we filtered stores based on push info, report them
    if (getExecutor().getSkippedStores().size() > 0) {
      LOGGER.info("================");
      LOGGER.info("STORES STORES WERE SKIPPED:");
      for (String store: getExecutor().getSkippedStores()) {
        LOGGER.info(store + " : " + pushMap.get(store).getRight());
      }
      LOGGER.info("================");
    }
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
