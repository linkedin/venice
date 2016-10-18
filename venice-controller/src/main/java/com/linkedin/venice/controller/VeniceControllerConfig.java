package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.*;

import static com.linkedin.venice.ConfigKeys.*;


/**
 * Configuration which is specific to a Venice controller.
 */
public class VeniceControllerConfig extends VeniceControllerClusterConfig {

  private final int adminPort;
  private final int controllerClusterReplica;
  private final String controllerClusterName;
  private final String controllerClusterZkAddresss;
  private final int topicMonitorPollIntervalMs;
  private final boolean parent;
  private Map<String, Set<String>> childClusterMap = null;
  private final int parentControllerWaitingTimeForConsumptionMs;

  public VeniceControllerConfig(VeniceProperties props) {
    super(props);
    this.adminPort = props.getInt(ADMIN_PORT);
    this.controllerClusterName = props.getString(CONTROLLER_CLUSTER, "venice-controllers");
    this.controllerClusterReplica = props.getInt(CONTROLLER_CLUSTER_REPLICA, 3);
    this.controllerClusterZkAddresss = props.getString(CONTROLLER_CLUSTER_ZK_ADDRESSS, getZkAddress());
    this.topicMonitorPollIntervalMs = props.getInt(TOPIC_MONITOR_POLL_INTERVAL_MS, 10 * Time.MS_PER_SECOND);
    this.parent = props.getBoolean(ConfigKeys.CONTROLLER_PARENT_MODE, false);
    if (this.parent) {
      this.childClusterMap = parseClusterMap(props.clipAndFilterNamespace(ConfigKeys.CHILD_CLUSTER_URL_PREFIX));
    }
    this.parentControllerWaitingTimeForConsumptionMs = props.getInt(ConfigKeys.PARENT_CONTROLLER_WAITING_TIME_FOR_CONSUMPTION_MS, 30 * Time.MS_PER_SECOND);
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

  public boolean isParent() {
    return parent;
  }

  /**
   * Map where keys are logical, human-readable names for child clusters (suitable for printing in logs or other output)
   * values are a list of cluster URLs that can be used to reach that cluster with the controller client.  List provides
   * redundancy in case of hardware or other failure.  Clients of this list should be sure they use another url if the
   * first one fails.
   *
   * @return
   */
  public Map<String, Set<String>> getChildClusterMap(){
    return childClusterMap;
  }

  public int getParentControllerWaitingTimeForConsumptionMs() {
    return parentControllerWaitingTimeForConsumptionMs;
  }

  /**
   * @param childClusterUris list of child controller uris
   * @return
   */
  public static Map<String, Set<String>> parseClusterMap(VeniceProperties childClusterUris) {
    Properties childClusterUriProps = childClusterUris.toProperties();
    if (childClusterUriProps.isEmpty()) {
      throw new VeniceException("child controller list can not be empty");
    }

    Map<String, Set<String>> outputMap = new HashMap<>();

    for (Map.Entry<Object, Object> uriEntry: childClusterUriProps.entrySet() ) {
      String datacenter = (String) uriEntry.getKey();

      String[] uris = ((String) uriEntry.getValue()).split(",\\s*");

      if (datacenter.isEmpty()) {
        throw new VeniceException("Invalid configuration" + CHILD_CLUSTER_URL_PREFIX + ".[missing]" + ": cluster name can't be empty. " + uriEntry.getValue());
      }

      outputMap.computeIfAbsent(datacenter, k -> new HashSet<>());

      for (String uri : uris) {
        if (uri.isEmpty()) {
          throw new VeniceException("Invalid configuration " + CHILD_CLUSTER_URL_PREFIX + "." + datacenter + ": found no urls for: " + uriEntry.getKey());
        }

        if (!uri.startsWith("http://")) {
          throw new VeniceException(
            "Invalid configuration " + CHILD_CLUSTER_URL_PREFIX + "." + datacenter + ": all urls must begin with http://, found: " + uriEntry.getValue());
        }

        outputMap.get(datacenter).add(uri);
      }
    }

    return outputMap;
  }
}
