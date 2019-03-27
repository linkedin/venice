package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


public class ParentHelixOfflinePushAccessor {
  // Use the different path from the normal offline push status znodes to prevent impacting the offline push monitor.
  public static final String OFFLINE_PUSH_SUB_PATH = "ParentOfflinePushes";
  private static final Logger logger = Logger.getLogger(ParentHelixOfflinePushAccessor.class);
  private final ZkClient zkClient;
  /**
   * Zk accessor for offline push status ZNodes.
   */
  private ZkBaseDataAccessor<OfflinePushStatus> offlinePushStatusAccessor;

  public ParentHelixOfflinePushAccessor(ZkClient zkClient, HelixAdapterSerializer adapter) {
    this.zkClient = zkClient;
    String offlinePushStatusPattern = getOfflinePushStatuesParentPath(PathResourceRegistry.WILDCARD_MATCH_ANY) + "/"
        + PathResourceRegistry.WILDCARD_MATCH_ANY;
    adapter.registerSerializer(offlinePushStatusPattern, new OfflinePushStatusJSONSerializer());
    this.zkClient.setZkSerializer(adapter);
    this.offlinePushStatusAccessor = new ZkBaseDataAccessor<>(zkClient);
  }

  public void createOfflinePushStatus(String clusterName, OfflinePushStatus pushStatus) {
    logger.info(
        "Start creating offline push status for topic:" + pushStatus.getKafkaTopic() + " in cluster:" + clusterName);
    HelixUtils.create(offlinePushStatusAccessor, getOfflinePushStatusPath(clusterName, pushStatus.getKafkaTopic()),
        pushStatus);
    logger.info("Created " + pushStatus.getNumberOfPartition() + " partition status Znodes.");
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

  public void deleteOfflinePushStatus(String clusterName, String topicName) {
    logger.info("Start deleting offline push status for topic: " + topicName + " in cluster: " + clusterName);
    HelixUtils.remove(offlinePushStatusAccessor, getOfflinePushStatusPath(clusterName, topicName));
    logger.info("Deleted offline push status for topic: " + topicName + " in cluster: " + clusterName);
  }

  public void updateOfflinePushStatus(String clusterName, OfflinePushStatus pushStatus) {
    HelixUtils.update(offlinePushStatusAccessor, getOfflinePushStatusPath(clusterName, pushStatus.getKafkaTopic()),
        pushStatus);
    logger.info(
        "Updated push status for topic: " + pushStatus.getKafkaTopic() + " in cluster:" + clusterName + " to status:"
            + pushStatus.getCurrentStatus());
  }

  public List<String> getAllPushNames(String clusterName) {
    return offlinePushStatusAccessor.getChildNames(getOfflinePushStatuesParentPath(clusterName),
        AccessOption.PERSISTENT);
  }

  private String getOfflinePushStatuesParentPath(String clusterName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + "/" + OFFLINE_PUSH_SUB_PATH;
  }

  private String getOfflinePushStatusPath(String clusterNaem, String topic) {
    return getOfflinePushStatuesParentPath(clusterNaem) + "/" + topic;
  }
}
