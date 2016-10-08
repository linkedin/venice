package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.math.stat.inference.TestUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

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
  private Map<String, List<String>> childClusterMap = null;
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
      this.childClusterMap = parseClusterMap(props.getString(ConfigKeys.CHILD_CLUSTER_URL_MAP));
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
  public Map<String, List<String>> getChildClusterMap(){
    return childClusterMap;
  }

  public int getParentControllerWaitingTimeForConsumptionMs() {
    return parentControllerWaitingTimeForConsumptionMs;
  }

  /**
   * The format for the property is json: {cluster1:[url1,url2,url3],cluster2:[url4,url5],cluster3:[url6,url7,url8]}
   * the cluster name should be human readable, ex: ei-ltx1
   * the url should be of the form http://host:port, urls are separated by semicolons.
   *
   * @param clusterMapProperty
   * @return
   */
  public static Map<String, List<String>> parseClusterMap(String clusterMapProperty){

    try {
      Map<String, List<String>> outputMap = new ObjectMapper().readValue(clusterMapProperty, new TypeReference<HashMap<String,List<String>>>() {});
      for (Map.Entry<String, List<String>> urlEntry : outputMap.entrySet()){
        if (urlEntry.getValue().size() < 1){
          throw new VeniceException("Invalid configuration " + CHILD_CLUSTER_URL_MAP + ": found no urls for: " + urlEntry.getKey());
        }
        for (String url : urlEntry.getValue()){
          if (!url.startsWith("http://")){
            throw new VeniceException("Invalid configuration " + CHILD_CLUSTER_URL_MAP + ": all urls must begin with http://, found: " + url);
          }
        }
      }
      return outputMap;
    } catch (Exception e) {
      throw new VeniceException("Cannot parse childClusterConfig: " + clusterMapProperty, e);
    }
  }
}
