package com.linkedin.venice.controller;

import static com.linkedin.venice.zk.VeniceZkPaths.ADMIN_TOPIC_METADATA;

import com.linkedin.venice.controller.kafka.consumer.AdminMetadata;
import com.linkedin.venice.controller.kafka.consumer.AdminMetadataJSONSerializer;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.TrieBasedPathResourceRegistry;
import com.linkedin.venice.utils.Utils;
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
  private static final String ADMIN_TOPIC_METADATA_NODE_V2_PATH_PATTERN =
      getAdminTopicV2MetadataNodePath(TrieBasedPathResourceRegistry.WILDCARD_MATCH_ANY);

  private final ZkBaseDataAccessor<AdminMetadata> zkMapV2Accessor;

  public ZkAdminTopicMetadataAccessor(
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer,
      VeniceControllerClusterConfig clusterConfig) {
    this(zkClient, adapterSerializer);
    adapterSerializer.registerSerializer(
        ADMIN_TOPIC_METADATA_NODE_V2_PATH_PATTERN,
        new AdminMetadataJSONSerializer(clusterConfig.getPubSubPositionDeserializer()));
  }

  public ZkAdminTopicMetadataAccessor(
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer,
      VeniceControllerMultiClusterConfig multiClusterConfig) {
    this(zkClient, adapterSerializer);
    adapterSerializer.registerSerializer(
        ADMIN_TOPIC_METADATA_NODE_V2_PATH_PATTERN,
        new AdminMetadataJSONSerializer(multiClusterConfig.getPubSubPositionDeserializer()));
  }

  private ZkAdminTopicMetadataAccessor(ZkClient zkClient, HelixAdapterSerializer adapterSerializer) {
    // TODO We get two objects (zkClient and adapterSerializer), then call a setter of the first object and pass it the
    // second object, effectively mutating the (zkClient) object that was passed in.
    zkClient.setZkSerializer(adapterSerializer);
    zkMapV2Accessor = new ZkBaseDataAccessor<>(zkClient);
  }

  /**
   * Update the upstream metadata map for the given cluster with specific information provided in metadata
   *
   * @see AdminTopicMetadataAccessor#updateMetadata(String, AdminMetadata)
   */
  @Override
  public void updateMetadata(String clusterName, AdminMetadata metadataDelta) {
    updateV2Metadata(clusterName, metadataDelta);
  }

  private void updateV2Metadata(String clusterName, AdminMetadata metadata) {
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
      if (!metadata.getExecutionId().equals(UNDEFINED_VALUE)) {
        currentMetadata.setExecutionId(metadata.getExecutionId());
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
    return getV2AdminMetadata(clusterName);
  }

  /**
   * Get the AdminMetadata object directly from ZooKeeper
   */
  private AdminMetadata getV2AdminMetadata(String clusterName) {
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

  static String getAdminTopicV2MetadataNodePath(String clusterName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + ADMIN_TOPIC_METADATA_NODE;
  }
}
