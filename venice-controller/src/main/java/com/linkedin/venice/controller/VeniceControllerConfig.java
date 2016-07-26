package com.linkedin.venice.controller;

import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;


/**
 * Configuration which is specific to a Venice controller.
 */
public class VeniceControllerConfig extends VeniceControllerClusterConfig {
  private static final String ADMIN_PORT = "admin.port";
  private static final String CONTROLLER_CLUSTER_ZK_ADDRESSS = "controller.cluster.zk.address";
  /** Cluster name for all parent controllers */
  public static final String CONTROLLER_CLUSTER = "controller.cluster.name";
  /** How many parent controllers are assigned to each venice cluster. */
  public static final String CONTROLLER_CLUSTER_REPLICA = "controller.cluster.replica";
  /** The interval, in ms, between each polling iteration of the TopicMonitor */
  public static final String TOPIC_MONITOR_POLL_INTERVAL_MS = "topic.monitor.poll.interval.ms";

  private final int adminPort;
  private final int controllerClusterReplica;
  private final String controllerClusterName;
  private final String controllerClusterZkAddresss;
  private final int topicMonitorPollIntervalMs;

  public VeniceControllerConfig(VeniceProperties props) {
    super(props);
    this.adminPort = props.getInt(ADMIN_PORT);
    this.controllerClusterName = props.getString(CONTROLLER_CLUSTER, "venice-controllers");
    this.controllerClusterReplica = props.getInt(CONTROLLER_CLUSTER_REPLICA, 3);
    this.controllerClusterZkAddresss = props.getString(CONTROLLER_CLUSTER_ZK_ADDRESSS, getZkAddress());
    this.topicMonitorPollIntervalMs = props.getInt(TOPIC_MONITOR_POLL_INTERVAL_MS, 10 * Time.MS_PER_SECOND);
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

  public int getTopicMonitorPollIntervalMs() { return topicMonitorPollIntervalMs; }
}
