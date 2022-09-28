package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.consumer.StringToLongMapJSONSerializer;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.TrieBasedPathResourceRegistry;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ZkAdminTopicMetadataAccessor extends AdminTopicMetadataAccessor {
  private static final Logger LOGGER = LogManager.getLogger(ZkAdminTopicMetadataAccessor.class);
  private static final int ZK_UPDATE_RETRY = 3;
  private static final long ZK_UPDATE_RETRY_DELAY_MS = 1000;
  private static final String ADMIN_TOPIC_METADATA_NODE = "/adminTopicMetadata";
  private static final String ADMIN_TOPIC_METADATA_NODE_PATH_PATTERN =
      getAdminTopicMetadataNodePath(TrieBasedPathResourceRegistry.WILDCARD_MATCH_ANY);

  private final ZkBaseDataAccessor<Map<String, Long>> zkMapAccessor;

  public ZkAdminTopicMetadataAccessor(ZkClient zkClient, HelixAdapterSerializer adapterSerializer) {
    adapterSerializer.registerSerializer(ADMIN_TOPIC_METADATA_NODE_PATH_PATTERN, new StringToLongMapJSONSerializer());
    // TODO We get two objects (zkClient and adapterSerializer), then call a setter of the first object and pass it the
    // second object, effectively mutating the (zkClient) object that was passed in.
    zkClient.setZkSerializer(adapterSerializer);
    zkMapAccessor = new ZkBaseDataAccessor<>(zkClient);
  }

  /**
   * @see AdminTopicMetadataAccessor#updateMetadata(String, Map)
   */
  @Override
  public void updateMetadata(String clusterName, Map<String, Long> metadata) {
    String path = getAdminTopicMetadataNodePath(clusterName);
    HelixUtils.update(zkMapAccessor, path, metadata, ZK_UPDATE_RETRY);
    LOGGER.info("Persisted admin topic metadata map for cluster: {}, map: {}", clusterName, metadata);
  }

  /**
   * @see AdminTopicMetadataAccessor#getMetadata(String)
   */
  @Override
  public Map<String, Long> getMetadata(String clusterName) {
    int retry = ZK_UPDATE_RETRY;
    String path = getAdminTopicMetadataNodePath(clusterName);
    while (retry > 0) {
      try {
        Map<String, Long> metadata = zkMapAccessor.get(path, null, AccessOption.PERSISTENT);
        if (metadata == null) {
          metadata = new HashMap<>();
        }
        return metadata;
      } catch (Exception e) {
        LOGGER.warn("Could not get the admin topic metadata map from Zk with: {}. Will retry.", path, e);
        retry--;
        Utils.sleep(ZK_UPDATE_RETRY_DELAY_MS);
      }
    }
    throw new VeniceException(
        "After " + ZK_UPDATE_RETRY + " retries still could not get the admin topic metadata map" + " from Zk with: "
            + path);
  }

  static String getAdminTopicMetadataNodePath(String clusterName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + ADMIN_TOPIC_METADATA_NODE;
  }
}
