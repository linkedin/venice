package com.linkedin.venice.controller;

import com.linkedin.venice.utils.VeniceProperties;


/**
 * Configuration which is specific to a Venice controller.
 */
public class VeniceControllerConfig extends VeniceControllerClusterConfig {
  private static final String ADMIN_PORT = "admin.port";
  private static final String CONTROLLER_CLUSTER_ZK_ADDRESSS = "controller.cluster.zk.address";
  /**
   * Cluster name for all parent controllers
   */
  public static final String CONTROLLER_CLUSTER = "controller.cluster.name";
  /**
   * How many parent controllers are assigned to each venice cluster.
   */
  public static final String CONTROLLER_CLUSTER_REPLICA = "controoler.cluster.replica";

  private int adminPort;
  private int controllerClusterReplica;
  private String controllerClusterName;
  private String controllerClusterZkAddresss;

  public VeniceControllerConfig(VeniceProperties props) {
    super(props);
    checkProperties(props);
  }

  private void checkProperties(VeniceProperties props) {
    adminPort = props.getInt(ADMIN_PORT);
    controllerClusterName = props.getString(CONTROLLER_CLUSTER, "venice-controllers");
    controllerClusterReplica = props.getInt(CONTROLLER_CLUSTER_REPLICA, 3);
    controllerClusterZkAddresss = props.getString(CONTROLLER_CLUSTER_ZK_ADDRESSS, getZkAddress());
  }

  public int getAdminPort() {
    return adminPort;
  }

  public int getControllerClusterReplica() {
    return controllerClusterReplica;
  }

  public String getControllerClusterName() {
    return controllerClusterName;
  }

  public String getControllerClusterZkAddresss() {
    return controllerClusterZkAddresss;
  }
}
