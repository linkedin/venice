package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.consumer.AdminMetadata;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.utils.Pair;


/**
 * This class provides a set of methods to access and update metadata for admin topics.
 */
public abstract class AdminTopicMetadataAccessor {
  public static final String POSITION_KEY = "position";
  /**
   * When remote consumption is enabled, child controller will consume directly from the source admin topic; an extra
   * metadata called upstream offset will be maintained, which indicate the last offset in the source admin topic that
   * gets processed successfully.
   */
  public static final String UPSTREAM_POSITION_KEY = "upstreamPosition";
  public static final String EXECUTION_ID_KEY = "executionId";
  public static final String ADMIN_OPERATION_PROTOCOL_VERSION_KEY = "adminOperationProtocolVersion";
  public static final Long UNDEFINED_VALUE = -1L;

  public static Pair<PubSubPosition, PubSubPosition> getPositions(AdminMetadata metadata) {
    return new Pair<>(metadata.getPosition(), metadata.getUpstreamPosition());
  }

  /**
   * @return the value to which the specified key is mapped to {@linkplain AdminTopicMetadataAccessor#ADMIN_OPERATION_PROTOCOL_VERSION_KEY}.
   */
  public static long getAdminOperationProtocolVersion(AdminMetadata metadata) {
    return metadata.getAdminOperationProtocolVersion();
  }

  /**
   * @return the execution ID from the metadata
   */
  public static long getExecutionId(AdminMetadata metadata) {
    return metadata != null && metadata.getExecutionId() != null ? metadata.getExecutionId() : UNDEFINED_VALUE;
  }

  /**
   * Update specific metadata for a given cluster in a single transaction with information provided in metadata.
   * @param clusterName of the cluster at interest.
   * @param metadata AdminMetadata containing relevant information.
   */
  public abstract void updateMetadata(String clusterName, AdminMetadata metadata);

  /**
   * Retrieve the latest metadata.
   * @param clusterName of the cluster at interest.
   * @return AdminMetadata containing all metadata information
   */
  public abstract AdminMetadata getMetadata(String clusterName);
}
