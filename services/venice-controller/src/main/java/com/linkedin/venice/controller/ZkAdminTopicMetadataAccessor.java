package com.linkedin.venice.controller;

import static com.linkedin.venice.zk.VeniceZkPaths.ADMIN_TOPIC_METADATA;
import static com.linkedin.venice.zk.VeniceZkPaths.ADMIN_TOPIC_METADATA_V2;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.controller.kafka.consumer.AdminMetadata;
import com.linkedin.venice.controller.kafka.consumer.AdminMetadataJSONSerializer;
import com.linkedin.venice.controller.kafka.consumer.StringToLongMapJSONSerializer;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
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
  private static final String ADMIN_TOPIC_METADATA_SECONDARY_NODE = "/" + ADMIN_TOPIC_METADATA_V2;
  private static final String ADMIN_TOPIC_METADATA_NODE_PATH_PATTERN =
      getAdminTopicMetadataNodePath(TrieBasedPathResourceRegistry.WILDCARD_MATCH_ANY);
  private static final String ADMIN_TOPIC_METADATA_NODE_V2_PATH_PATTERN =
      getAdminTopicV2MetadataNodePath(TrieBasedPathResourceRegistry.WILDCARD_MATCH_ANY);

  private final ZkBaseDataAccessor<Map<String, Long>> zkMapAccessor;
  private final ZkBaseDataAccessor<AdminMetadata> zkMapV2Accessor;
  Map<String, Boolean> useV2AdminTopicMetadataMap = new HashMap<>();

  public ZkAdminTopicMetadataAccessor(
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer,
      VeniceControllerClusterConfig clusterConfig) {
    this(zkClient, adapterSerializer);
    adapterSerializer.registerSerializer(
        ADMIN_TOPIC_METADATA_NODE_V2_PATH_PATTERN,
        new AdminMetadataJSONSerializer(clusterConfig.getPubSubPositionDeserializer()));
    useV2AdminTopicMetadataMap.put(clusterConfig.getClusterName(), clusterConfig.useV2AdminTopicMetadata());
  }

  public ZkAdminTopicMetadataAccessor(
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer,
      VeniceControllerMultiClusterConfig multiClusterConfig) {
    this(zkClient, adapterSerializer);
    adapterSerializer.registerSerializer(
        ADMIN_TOPIC_METADATA_NODE_V2_PATH_PATTERN,
        new AdminMetadataJSONSerializer(multiClusterConfig.getPubSubPositionDeserializer()));
    multiClusterConfig.getClusters()
        .forEach(
            clusterName -> useV2AdminTopicMetadataMap
                .put(clusterName, multiClusterConfig.getControllerConfig(clusterName).useV2AdminTopicMetadata()));
  }

  private ZkAdminTopicMetadataAccessor(ZkClient zkClient, HelixAdapterSerializer adapterSerializer) {
    adapterSerializer.registerSerializer(ADMIN_TOPIC_METADATA_NODE_PATH_PATTERN, new StringToLongMapJSONSerializer());
    // TODO We get two objects (zkClient and adapterSerializer), then call a setter of the first object and pass it the
    // second object, effectively mutating the (zkClient) object that was passed in.
    zkClient.setZkSerializer(adapterSerializer);
    zkMapAccessor = new ZkBaseDataAccessor<>(zkClient);
    zkMapV2Accessor = new ZkBaseDataAccessor<>(zkClient);
  }

  /**
   * Update the upstream metadata map for the given cluster with specific information provided in metadata
   *
   * @see AdminTopicMetadataAccessor#updateMetadata(String, AdminMetadata)
   */
  @Override
  public void updateMetadata(String clusterName, AdminMetadata metadataDelta) {
    updateV1Metadata(clusterName, metadataDelta);
    updateV2Metadata(clusterName, metadataDelta);
  }

  private void updateV1Metadata(String clusterName, AdminMetadata metadata) {
    String path = getAdminTopicMetadataNodePath(clusterName);
    // Use the new toLegacyMap method to get the V1 format
    Map<String, Long> metadataDelta = metadata.toLegacyMap();

    HelixUtils.compareAndUpdate(zkMapAccessor, path, ZK_UPDATE_RETRY, currentMetadataMap -> {
      if (currentMetadataMap == null) {
        currentMetadataMap = new HashMap<>();
      }
      LOGGER.info(
          "Updating AdminTopicMetadata map for cluster: {}. Current metadata: {}. New delta metadata: {}",
          clusterName,
          currentMetadataMap,
          metadataDelta);
      currentMetadataMap.putAll(metadataDelta);
      return currentMetadataMap;
    });
  }

  public void updateV2Metadata(String clusterName, AdminMetadata metadata) {
    String path = getAdminTopicV2MetadataNodePath(clusterName);

    HelixUtils.compareAndUpdate(zkMapV2Accessor, path, ZK_UPDATE_RETRY, currentMetadata -> {
      if (currentMetadata == null) {
        return metadata;
      }
      LOGGER.info(
          "Updating AdminTopicPositionMetadata for cluster: {}. Current metadata: {}. New delta metadata: {}",
          clusterName,
          currentMetadata,
          metadata);

      // Merge the metadata objects
      if (metadata.getExecutionId() != null) {
        currentMetadata.setExecutionId(metadata.getExecutionId());
      }
      if (metadata.getOffset() != null) {
        currentMetadata.setOffset(metadata.getOffset());
      }
      if (metadata.getUpstreamOffset() != null) {
        currentMetadata.setUpstreamOffset(metadata.getUpstreamOffset());
      }
      if (!metadata.getAdminOperationProtocolVersion().equals(UNDEFINED_VALUE)) {
        currentMetadata.setAdminOperationProtocolVersion(metadata.getAdminOperationProtocolVersion());
      }
      if (metadata.getPosition() != null && !metadata.getPosition().equals(PubSubSymbolicPosition.EARLIEST)) {
        currentMetadata.setPubSubPosition(metadata.getPosition());
      }
      if (metadata.getUpstreamPosition() != null
          && !metadata.getUpstreamPosition().equals(PubSubSymbolicPosition.EARLIEST)) {
        currentMetadata.setUpstreamPubSubPosition(metadata.getUpstreamPosition());
      }

      return currentMetadata;
    });
  }

  /**
   * @see AdminTopicMetadataAccessor#getMetadata(String)
   */
  @Override
  public AdminMetadata getMetadata(String clusterName) {
    return useV2AdminTopicMetadataMap.getOrDefault(clusterName, false)
        ? getV2AdminMetadata(clusterName)
        : getV1AdminMetadata(clusterName);
  }

  private AdminMetadata getV1AdminMetadata(String clusterName) {
    int retry = ZK_UPDATE_RETRY;
    String path = getAdminTopicMetadataNodePath(clusterName);
    while (retry > 0) {
      try {
        return AdminMetadata.fromLegacyMap(zkMapAccessor.get(path, null, AccessOption.PERSISTENT));
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

  /**
   * Get the AdminMetadata object directly from ZooKeeper
   */
  @VisibleForTesting
  AdminMetadata getV2AdminMetadata(String clusterName) {
    int retry = ZK_UPDATE_RETRY;
    String path = getAdminTopicV2MetadataNodePath(clusterName);
    while (retry > 0) {
      try {
        AdminMetadata metadata = zkMapV2Accessor.get(path, null, AccessOption.PERSISTENT);
        if (metadata == null) {
          metadata = new AdminMetadata();
        }
        return metadata;
      } catch (Exception e) {
        LOGGER.warn("Could not get the admin topic metadata from Zk with: {}. Will retry.", path, e);
        retry--;
        if (retry > 0) {
          Utils.sleep(ZK_UPDATE_RETRY_DELAY_MS);
        }
      }
    }
    throw new VeniceException("Could not get the admin topic metadata from Zk with: " + path);
  }

  static String getAdminTopicMetadataNodePath(String clusterName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + ADMIN_TOPIC_METADATA_NODE;
  }

  static String getAdminTopicV2MetadataNodePath(String clusterName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + ADMIN_TOPIC_METADATA_SECONDARY_NODE;
  }
}
