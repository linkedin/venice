package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.SimpleStringSerializer;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * Class used to access to the white list stored in ZK. As operating white list should happen very rarely, all of
 * request will hit ZK directly without cache.
 */
public class ZkWhitelistAccessor implements WhitelistAccessor{
  private static final String PREFIX_PATH = "/whitelist";

  private boolean isCloseAble = false;

  private final ZkClient zkClient;

  public ZkWhitelistAccessor(ZkClient zkClient, HelixAdapterSerializer adapterSerializer) {
    this.zkClient = zkClient;
    adapterSerializer.registerSerializer(
        getWhiteListPath(PathResourceRegistry.WILDCARD_MATCH_ANY) + "/" + PathResourceRegistry.WILDCARD_MATCH_ANY,
        new SimpleStringSerializer());
    this.zkClient.setZkSerializer(adapterSerializer);
  }

  public ZkWhitelistAccessor(String zkAddress){
    zkClient = ZkClientFactory.newZkClient(zkAddress);
    isCloseAble = true;
  }
  /**
   * Judge whether the given helix nodeId is in the white list or not.
   */
  public boolean isInstanceInWhitelist(String clusterName, String helixNodeId) {
    return zkClient.exists(getWhiteListInstancePath(clusterName, helixNodeId));
  }

  /**
   * Get all helix nodeIds in the white list of the given cluster from ZK.
   */
  public Set<String> getWhiteList(String clusterName) {
    if (!zkClient.exists(getWhiteListPath(clusterName))) {
      return Collections.emptySet();
    }
    return new HashSet<>(zkClient.getChildren(getWhiteListPath(clusterName)));
  }

  /**
   * Add the given helix nodeId into the white list in ZK.
   */
  public void addInstanceToWhiteList(String clusterName, String helixNodeId) {
    zkClient.createPersistent(getWhiteListInstancePath(clusterName, helixNodeId), true);
  }

  /**
   * Remove the given helix nodeId from the white list in ZK.
   */
  public void removeInstanceFromWhiteList(String clusterName, String helixNodeId) {
    zkClient.delete(getWhiteListInstancePath(clusterName, helixNodeId));
  }

  private String getWhiteListPath(String clusterName) {
    return HelixUtils.getHelixClusterZkPath(clusterName)+ PREFIX_PATH;
  }

  private String getWhiteListInstancePath(String clusterName, String helixNodeId) {
    validateInstanceName(helixNodeId);
    return getWhiteListPath(clusterName) + "/" + helixNodeId;
  }

  private void validateInstanceName(String helixNodeId) {
    try {
      Utils.parseHostFromHelixNodeIdentifier(helixNodeId);
      Utils.parsePortFromHelixNodeIdentifier(helixNodeId);
    } catch (Exception e) {
      throw new VeniceException("Invalid helix nodeId:" + helixNodeId, e);
    }
  }

  public void close() {
    if (isCloseAble) {
      zkClient.close();
    }
  }
}
