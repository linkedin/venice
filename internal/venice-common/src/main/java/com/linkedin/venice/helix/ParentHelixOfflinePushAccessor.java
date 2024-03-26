package com.linkedin.venice.helix;

import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import org.apache.helix.zookeeper.impl.client.ZkClient;


public class ParentHelixOfflinePushAccessor {
  // Use the different path from the normal offline push status znodes to prevent impacting the offline push monitor.
  public static final String OFFLINE_PUSH_SUB_PATH = "ParentOfflinePushes";

  public ParentHelixOfflinePushAccessor(ZkClient zkClient, HelixAdapterSerializer adapter) {
    String offlinePushStatusPattern = getOfflinePushStatuesParentPath(PathResourceRegistry.WILDCARD_MATCH_ANY) + "/"
        + PathResourceRegistry.WILDCARD_MATCH_ANY;
    adapter.registerSerializer(offlinePushStatusPattern, new OfflinePushStatusJSONSerializer());
    zkClient.setZkSerializer(adapter);
  }

  private String getOfflinePushStatuesParentPath(String clusterName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + "/" + OFFLINE_PUSH_SUB_PATH;
  }
}
