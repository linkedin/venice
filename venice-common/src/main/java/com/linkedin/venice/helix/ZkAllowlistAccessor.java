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
 * Class used to access to the allowlist stored in ZK. As operating allowlist should happen very rarely, all of
 * request will hit ZK directly without cache.
 */
public class ZkAllowlistAccessor implements AllowlistAccessor {
  // go/inclusivecode deferred(changing zk paths will need migration)
  private static final String PREFIX_PATH = "/whitelist";

  private boolean isCloseAble = false;

  private final ZkClient zkClient;

  public ZkAllowlistAccessor(ZkClient zkClient, HelixAdapterSerializer adapterSerializer) {
    this.zkClient = zkClient;
    adapterSerializer.registerSerializer(
        getAllowListPath(PathResourceRegistry.WILDCARD_MATCH_ANY) + "/" + PathResourceRegistry.WILDCARD_MATCH_ANY,
        new SimpleStringSerializer());
    this.zkClient.setZkSerializer(adapterSerializer);
  }

  public ZkAllowlistAccessor(String zkAddress) {
    zkClient = ZkClientFactory.newZkClient(zkAddress);
    isCloseAble = true;
  }

  /**
   * Judge whether the given helix nodeId is in the allowlist or not.
   */
  public boolean isInstanceInAllowlist(String clusterName, String helixNodeId) {
    return zkClient.exists(getAllowListInstancePath(clusterName, helixNodeId));
  }

  /**
   * Get all helix nodeIds in the allowlist of the given cluster from ZK.
   */
  public Set<String> getAllowList(String clusterName) {
    if (!zkClient.exists(getAllowListPath(clusterName))) {
      return Collections.emptySet();
    }
    return new HashSet<>(zkClient.getChildren(getAllowListPath(clusterName)));
  }

  /**
   * Add the given helix nodeId into the allowlist in ZK.
   */
  public void addInstanceToAllowList(String clusterName, String helixNodeId) {
    zkClient.createPersistent(getAllowListInstancePath(clusterName, helixNodeId), true);
  }

  /**
   * Remove the given helix nodeId from the allowlist in ZK.
   */
  public void removeInstanceFromAllowList(String clusterName, String helixNodeId) {
    zkClient.delete(getAllowListInstancePath(clusterName, helixNodeId));
  }

  private String getAllowListPath(String clusterName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + PREFIX_PATH;
  }

  private String getAllowListInstancePath(String clusterName, String helixNodeId) {
    validateInstanceName(helixNodeId);
    return getAllowListPath(clusterName) + "/" + helixNodeId;
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
