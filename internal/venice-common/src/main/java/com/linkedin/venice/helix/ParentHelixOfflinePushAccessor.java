package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;


public class ParentHelixOfflinePushAccessor {
  // Use the different path from the normal offline push status znodes to prevent impacting the offline push monitor.
  public static final String OFFLINE_PUSH_SUB_PATH = "ParentOfflinePushes";
  private final ZkClient zkClient;
  /**
   * Zk accessor for offline push status ZNodes.
   */
  private final ZkBaseDataAccessor<OfflinePushStatus> offlinePushStatusAccessor;

  public ParentHelixOfflinePushAccessor(ZkClient zkClient, HelixAdapterSerializer adapter) {
    this.zkClient = zkClient;
    String offlinePushStatusPattern = getOfflinePushStatuesParentPath(PathResourceRegistry.WILDCARD_MATCH_ANY) + "/"
        + PathResourceRegistry.WILDCARD_MATCH_ANY;
    adapter.registerSerializer(offlinePushStatusPattern, new OfflinePushStatusJSONSerializer());
    this.zkClient.setZkSerializer(adapter);
    this.offlinePushStatusAccessor = new ZkBaseDataAccessor<>(zkClient);
  }

  public OfflinePushStatus getOfflinePushStatus(String clusterName, String kafkaTopic) {
    OfflinePushStatus offlinePushStatus =
        offlinePushStatusAccessor.get(getOfflinePushStatusPath(clusterName, kafkaTopic), null, AccessOption.PERSISTENT);
    if (offlinePushStatus == null) {
      throw new VeniceException(
          "Can not find offline push status in ZK from path:" + getOfflinePushStatusPath(clusterName, kafkaTopic));
    }
    return offlinePushStatus;
  }

  public List<String> getAllPushNames(String clusterName) {
    return offlinePushStatusAccessor
        .getChildNames(getOfflinePushStatuesParentPath(clusterName), AccessOption.PERSISTENT);
  }

  private String getOfflinePushStatuesParentPath(String clusterName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + "/" + OFFLINE_PUSH_SUB_PATH;
  }

  private String getOfflinePushStatusPath(String clusterNaem, String topic) {
    return getOfflinePushStatuesParentPath(clusterNaem) + "/" + topic;
  }
}
