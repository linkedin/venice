package com.linkedin.venice.controller;

import static com.linkedin.venice.zk.VeniceZkPaths.ADMIN_TOPIC_METADATA;

import com.linkedin.venice.controller.kafka.consumer.StringToObjectMapJSONSerializer;
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


/**
 * This class is responsible for accessing and updating the admin topic metadata stored in Zookeeper.
 */
public class ZkAdminTopicMetadataAccessor extends AdminTopicMetadataAccessor {
  private static final Logger LOGGER = LogManager.getLogger(ZkAdminTopicMetadataAccessor.class);
  private static final int ZK_UPDATE_RETRY = 3;
  private static final long ZK_UPDATE_RETRY_DELAY_MS = 1000;
  private static final String ADMIN_TOPIC_METADATA_NODE = "/" + ADMIN_TOPIC_METADATA;
  private static final String ADMIN_TOPIC_METADATA_NODE_PATH_PATTERN =
      getAdminTopicMetadataNodePath(TrieBasedPathResourceRegistry.WILDCARD_MATCH_ANY);

  private final ZkBaseDataAccessor<Map<String, Object>> zkMapAccessor;

  public ZkAdminTopicMetadataAccessor(ZkClient zkClient, HelixAdapterSerializer adapterSerializer) {
    adapterSerializer.registerSerializer(ADMIN_TOPIC_METADATA_NODE_PATH_PATTERN, new StringToObjectMapJSONSerializer());
    // TODO We get two objects (zkClient and adapterSerializer), then call a setter of the first object and pass it the
    // second object, effectively mutating the (zkClient) object that was passed in.
    zkClient.setZkSerializer(adapterSerializer);
    zkMapAccessor = new ZkBaseDataAccessor<>(zkClient);
  }

  /**
   * Update the upstream metadata map for the given cluster with specific information provided in metadataDelta
   *
   * @see AdminTopicMetadataAccessor#updateMetadata(String, Map)
   */
  @Override
  public void updateMetadata(String clusterName, Map<String, Long> metadataDelta) {
    updateMetadataInternal(clusterName, metadataDelta, "Long metadata");
  }

  @Override
  public void updatePositionMetadata(String clusterName, Map<String, Position> positionMetadataDelta) {
    updateMetadataInternal(clusterName, positionMetadataDelta, "Position metadata");
  }

  /**
   * Common helper method to update metadata in ZooKeeper with retry logic and logging.
   *
   * @param clusterName the name of the cluster
   * @param metadataDelta the metadata changes to apply
   * @param metadataType descriptive name for logging purposes
   * @param <T> the type of metadata values (Long or Position)
   */
  private <T> void updateMetadataInternal(String clusterName, Map<String, T> metadataDelta, String metadataType) {
    String path = getAdminTopicMetadataNodePath(clusterName);
    HelixUtils.compareAndUpdate(zkMapAccessor, path, ZK_UPDATE_RETRY, currentMetadataMap -> {
      if (currentMetadataMap == null) {
        currentMetadataMap = new HashMap<>();
      }
      LOGGER.info(
          "Updating AdminTopicMetadata map for cluster: {}. Current metadata: {}. New delta {}: {}",
          clusterName,
          currentMetadataMap,
          metadataType,
          metadataDelta);
      currentMetadataMap.putAll(metadataDelta);
      return currentMetadataMap;
    });
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
        Map<String, Object> metadata = zkMapAccessor.get(path, null, AccessOption.PERSISTENT);
        if (metadata == null) {
          metadata = new HashMap<>();
        }

        // Filter out position fields and convert to Map<String, Long>
        Map<String, Long> longMetadata = new HashMap<>();
        for (Map.Entry<String, Object> entry: metadata.entrySet()) {
          String key = entry.getKey();
          Object value = entry.getValue();

          // Skip position fields
          if (key.equals(AdminTopicMetadataAccessor.POSITION_KEY)
              || key.equals(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY)) {
            continue;
          }

          // Convert to Long if it's a numeric value
          if (value instanceof Long) {
            longMetadata.put(key, (Long) value);
          } else if (value instanceof Number) {
            longMetadata.put(key, ((Number) value).longValue());
          }
        }

        return longMetadata;
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

  @Override
  public Map<String, Position> getPositionMetadata(String clusterName) {
    int retry = ZK_UPDATE_RETRY;
    String path = getAdminTopicMetadataNodePath(clusterName);
    while (retry > 0) {
      try {
        Map<String, Object> metadata = zkMapAccessor.get(path, null, AccessOption.PERSISTENT);
        if (metadata == null) {
          metadata = new HashMap<>();
        }

        // Filter out position fields and convert to Map<String, Position>
        Map<String, Position> positionMetadata = new HashMap<>();

        // Extract Position objects that Jackson has already deserialized
        Object localPosition = metadata.get(AdminTopicMetadataAccessor.POSITION_KEY);
        if (localPosition instanceof Position) {
          positionMetadata.put(AdminTopicMetadataAccessor.POSITION_KEY, (Position) localPosition);
        }

        Object upstreamPosition = metadata.get(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY);
        if (upstreamPosition instanceof Position) {
          positionMetadata.put(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY, (Position) upstreamPosition);
        }

        return positionMetadata;
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
