package com.linkedin.venice.datarecovery;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreInfoResponse;
import com.linkedin.venice.meta.Store;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.compress.utils.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Client {
  private static final Logger LOGGER = LogManager.getLogger(Client.class);
  private final static String STORE_NAME_SEPARATOR = "[,:]+";
  private ControllerClient controllerClient;
  private Module executor;

  public Client(ControllerClient controllerClient) {
    this(controllerClient, new Module());
  }

  public Client(ControllerClient controllerClient, Module module) {
    this.controllerClient = controllerClient;
    this.executor = module;
  }

  public Module getExecutor() {
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
    if (line.toLowerCase().equals("yes") || line.toLowerCase().equals("y")) {
      return true;
    }
    return false;
  }

  public class OperationLevel {
    private String multiStores;
    private String clusterName;

    public OperationLevel(String multiStores) {
      this.multiStores = multiStores;
    }

    public OperationLevel(String multiStores, String clusterName) {
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
            .filter(storeInfo -> !Store.isSystemStore(storeInfo.getName()))
            .map(storeInfo -> storeInfo.getName())
            .collect(Collectors.toSet());
      }
      return storeNames;
    }
  }
}
