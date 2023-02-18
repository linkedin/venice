package com.linkedin.venice.datarecovery;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreInfoResponse;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.compress.utils.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DataRecoveryClient {
  private static final Logger LOGGER = LogManager.getLogger(DataRecoveryClient.class);
  private final static String STORE_NAME_SEPARATOR = "[,:]+";
  private final DataRecoveryModule executor;

  public DataRecoveryClient() {
    this(new DataRecoveryModule());
  }

  public DataRecoveryClient(DataRecoveryModule module) {
    this.executor = module;
  }

  public DataRecoveryModule getExecutor() {
    return executor;
  }

  public void execute(OperationLevel level, StoreRepushCommand.Params params) {
    Set<String> storeNames = level.getStoreNames();
    if (storeNames == null || storeNames.isEmpty()) {
      LOGGER.warn("store list is empty, exit.");
      return;
    }
    if (!confirmStores(storeNames)) {
      return;
    }

    getExecutor().perform(storeNames, params);
  }

  public boolean confirmStores(Set<String> storeNames) {
    LOGGER.info("stores to recover: " + storeNames);
    LOGGER.info("Recover " + storeNames.size() + " stores, please confirm (yes/no) [y/n]:");
    Scanner in = new Scanner(System.in);
    String line = in.nextLine();
    return line.equalsIgnoreCase("yes") || line.equalsIgnoreCase("y");
  }

  public static class OperationLevel {
    private ControllerClient controllerClient;
    private final String multiStores;
    private String clusterName;

    public OperationLevel(String multiStores) {
      this.multiStores = multiStores;
    }

    public OperationLevel(ControllerClient controllerClient, String multiStores, String clusterName) {
      this.controllerClient = controllerClient;
      this.multiStores = multiStores;
      this.clusterName = clusterName;
    }

    private Set<String> getStoreNames() {
      Set<String> storeNames = null;
      if (multiStores != null && !multiStores.isEmpty()) {
        storeNames = Sets.newHashSet(multiStores.split(STORE_NAME_SEPARATOR));
      } else if (clusterName != null) {
        MultiStoreInfoResponse status = controllerClient.getClusterStores(clusterName);
        storeNames = status.getStoreInfoList()
            .stream()
            .map(StoreInfo::getName)
            .filter(name -> !Store.isSystemStore(name))
            .collect(Collectors.toSet());
      }
      return storeNames;
    }
  }
}
