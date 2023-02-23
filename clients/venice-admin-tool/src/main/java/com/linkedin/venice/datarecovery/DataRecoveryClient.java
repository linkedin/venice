package com.linkedin.venice.datarecovery;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreInfoResponse;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.Utils;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;
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

  public DataRecoveryClient() {
    this(new DataRecoveryExecutor());
  }

  public DataRecoveryClient(DataRecoveryExecutor module) {
    this.executor = module;
  }

  public DataRecoveryExecutor getExecutor() {
    return executor;
  }

  public void execute(DataRecoveryParams drParams, StoreRepushCommand.Params cmdParams) {
    Set<String> storeNames = drParams.getRecoveryStores();
    if (storeNames == null || storeNames.isEmpty()) {
      LOGGER.warn("store list is empty, exit.");
      return;
    }
    if (!confirmStores(storeNames)) {
      return;
    }

    getExecutor().perform(storeNames, cmdParams);
  }

  public boolean confirmStores(Set<String> storeNames) {
    LOGGER.info("stores to recover: " + storeNames);
    LOGGER.info("Recover " + storeNames.size() + " stores, please confirm (yes/no) [y/n]:");
    Scanner in = new Scanner(System.in);
    String line = in.nextLine();
    return line.equalsIgnoreCase("yes") || line.equalsIgnoreCase("y");
  }

  public static class DataRecoveryParams {
    private final String controllerUrl;
    private final String multiStores;
    private final String recoveryCluster;
    private final Set<String> recoveryStores;

    public DataRecoveryParams(String controllerUrl, String multiStores, String recoveryCluster) {
      this.controllerUrl = controllerUrl;
      this.multiStores = multiStores;
      this.recoveryCluster = recoveryCluster;
      this.recoveryStores = calculateRecoveryStoreNames(this.controllerUrl, this.multiStores, this.recoveryCluster);
    }

    public Set<String> getRecoveryStores() {
      return recoveryStores;
    }

    private Set<String> calculateRecoveryStoreNames(String controllerUrl, String multiStores, String recoveryCluster) {
      Set<String> storeNames = null;
      // Give high priority to multiStores, if it contains meaningful data, ignore recoveryCluster.
      if (multiStores != null && !multiStores.isEmpty()) {
        storeNames = Utils.parseCommaSeparatedStringToSet(multiStores);
      } else if (controllerUrl != null && recoveryCluster != null) {
        MultiStoreInfoResponse status;
        try (ControllerClient client =
            ControllerClient.constructClusterControllerClient(recoveryCluster, controllerUrl)) {
          status = client.getClusterStores(recoveryCluster);
        }
        if (status != null) {
          storeNames = status.getStoreInfoList()
              .stream()
              .map(StoreInfo::getName)
              .filter(name -> !Store.isSystemStore(name))
              .collect(Collectors.toSet());
        }
      }
      return storeNames;
    }
  }
}
