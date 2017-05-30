package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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

  private Map<String, String> childClusterMap = null;
  private String d2ServiceName;
  private Map<String, String> childClusterD2Map = null;

  private final int parentControllerWaitingTimeForConsumptionMs;
  private final long adminConsumptionTimeoutMinutes;

  public VeniceControllerConfig(VeniceProperties props) {
    super(props);
    this.adminPort = props.getInt(ADMIN_PORT);
    this.controllerClusterName = props.getString(CONTROLLER_CLUSTER, "venice-controllers");
    this.controllerClusterReplica = props.getInt(CONTROLLER_CLUSTER_REPLICA, 3);
    this.controllerClusterZkAddresss = props.getString(CONTROLLER_CLUSTER_ZK_ADDRESSS, getZkAddress());
    this.topicMonitorPollIntervalMs = props.getInt(TOPIC_MONITOR_POLL_INTERVAL_MS, 10 * Time.MS_PER_SECOND);
    this.parent = props.getBoolean(ConfigKeys.CONTROLLER_PARENT_MODE, false);
    if (this.parent) {
      String clusterWhitelist = props.getString(CHILD_CLUSTER_WHITELIST);
      this.childClusterMap = parseClusterMap(props, clusterWhitelist);
      this.childClusterD2Map = parseClusterMap(props, clusterWhitelist, true);
      if (!childClusterD2Map.isEmpty()) {
        this.d2ServiceName = props.getString(CHILD_CLUSTER_D2_SERVICE_NAME);
      }

      if (childClusterMap.isEmpty() && childClusterD2Map.isEmpty()) {
        throw new VeniceException("child controller list can not be empty");
      }
    }
    this.parentControllerWaitingTimeForConsumptionMs = props.getInt(ConfigKeys.PARENT_CONTROLLER_WAITING_TIME_FOR_CONSUMPTION_MS, 30 * Time.MS_PER_SECOND);
    this.adminConsumptionTimeoutMinutes = props.getLong(ADMIN_CONSUMPTION_TIMEOUT_MINUTES, TimeUnit.DAYS.toMinutes(5));
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
  public Map<String, String> getChildClusterMap(){
    return childClusterMap;
  }

  public String getD2ServiceName() {
    return d2ServiceName;
  }

  public Map<String, String> getChildClusterD2Map() {
    return childClusterD2Map;
  }

  public int getParentControllerWaitingTimeForConsumptionMs() {
    return parentControllerWaitingTimeForConsumptionMs;
  }

  public long getAdminConsumptionTimeoutMinutes(){
    return adminConsumptionTimeoutMinutes;
  }

  public static Map<String, String> parseClusterMap(VeniceProperties clusterPros, String datacenterWhitelist) {
    return parseClusterMap(clusterPros, datacenterWhitelist, false);
  }

  /**
   * @param clusterPros list of child controller uris
   * @param datacenterWhitelist data centers that are taken into account
   * @param D2Routing whether uses D2 to route or not
   * @return
   */
  public static Map<String, String> parseClusterMap(VeniceProperties clusterPros, String datacenterWhitelist, Boolean D2Routing) {
    Properties childClusterUriProps;
    String propsPrefix =  D2Routing ? CHILD_CLUSTER_D2_PREFIX : CHILD_CLUSTER_URL_PREFIX;

    childClusterUriProps = clusterPros.clipAndFilterNamespace(propsPrefix).toProperties();

    if (Utils.isNullOrEmpty(datacenterWhitelist)) {
      throw new VeniceException("child controller list must have a whitelist");
    }

    Map<String, String> outputMap = new HashMap<>();
    List<String> whitelist = Arrays.asList(datacenterWhitelist.split(",\\s*"));

    for (Map.Entry<Object, Object> uriEntry : childClusterUriProps.entrySet()) {
      String datacenter = (String) uriEntry.getKey();
      String uris = (String) uriEntry.getValue();

      String errmsg = "Invalid configuration " + propsPrefix + "." + datacenter;
      if (datacenter.isEmpty()) {
        throw new VeniceException(errmsg + ": cluster name can't be empty. " + uris);
      }

      if (uris.isEmpty()) {
        throw new VeniceException(errmsg + ": found no urls for: " + datacenter);
      }

      if (whitelist.contains(datacenter)) {
        outputMap.computeIfAbsent(datacenter, k -> {
          String[] uriList = uris.split(",\\s*");

          if (D2Routing && uriList.length != 1) {
            throw new VeniceException(errmsg + ": can only have 1 zookeeper url");
          }

          if (!D2Routing) {
            if (uriList.length == 0) {
              throw new VeniceException(errmsg + ": urls can not be empty");
            }

            if (Arrays.stream(uriList).anyMatch(uri -> !uri.startsWith("http://"))) {
              throw new VeniceException(errmsg + ": urls must begin with http://");
            }
          }

          return uris;
        });
      }
    }

    return outputMap;
  }
}
