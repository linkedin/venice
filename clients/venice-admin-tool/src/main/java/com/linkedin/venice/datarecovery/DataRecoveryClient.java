package com.linkedin.venice.datarecovery;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.utils.Utils;
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
  private final PlanningExecutor _planningExecutor;

  public DataRecoveryClient() {
    this(new DataRecoveryExecutor());
  }

  public DataRecoveryClient(DataRecoveryExecutor module) {
    this.executor = module;
    this._planningExecutor = new PlanningExecutor();
  }

  public DataRecoveryExecutor getExecutor() {
    return executor;
  }

  public PlanningExecutor getPlanningExecutor() {
    return _planningExecutor;
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

  public Integer estimateRecoveryTime(
      DataRecoveryParams drParams,
      String clusterName,
      ControllerClient controllerClient) {
    Set<String> storeNames = drParams.getRecoveryStores();
    if (storeNames == null || storeNames.isEmpty()) {
      LOGGER.warn("store list is empty, exit.");
      return -1;
    }

    getPlanningExecutor().perform(clusterName, storeNames, controllerClient);
    Integer totalRecoveryTime = 0;
    for (PlanningTask t: getPlanningExecutor().getTasks()) {
      totalRecoveryTime += t.getEstimatedTimeResult();
    }
    return totalRecoveryTime;
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
  }
}
