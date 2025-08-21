package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.consumer.AdminMetadata;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.utils.Pair;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


/**
 * This class provides a set of methods to access and update metadata for admin topics.
 */
public abstract class AdminTopicMetadataAccessor {
  public static final String OFFSET_KEY = "offset";
  public static final String POSITION_KEY = "position";
  /**
   * When remote consumption is enabled, child controller will consume directly from the source admin topic; an extra
   * metadata called upstream offset will be maintained, which indicate the last offset in the source admin topic that
   * gets processed successfully.
   */
  public static final String UPSTREAM_OFFSET_KEY = "upstreamOffset";
  public static final String UPSTREAM_POSITION_KEY = "upstreamPosition";
  public static final String EXECUTION_ID_KEY = "executionId";
  public static final String ADMIN_OPERATION_PROTOCOL_VERSION_KEY = "adminOperationProtocolVersion";
  public static final Long UNDEFINED_VALUE = -1L;

  /**
   * @return a map with {@linkplain AdminTopicMetadataAccessor#OFFSET_KEY}, {@linkplain AdminTopicMetadataAccessor#UPSTREAM_OFFSET_KEY},
   *         {@linkplain AdminTopicMetadataAccessor#EXECUTION_ID_KEY}, {@linkplain AdminTopicMetadataAccessor#ADMIN_OPERATION_PROTOCOL_VERSION_KEY}
   *         specified to input values.
   */
  public static Map<String, Long> generateMetadataMap(
      Optional<Long> localOffset,
      Optional<Long> upstreamOffset,
      Optional<Long> executionId,
      Optional<Long> adminOperationProtocolVersion) {
    Map<String, Long> metadata = new HashMap<>();
    localOffset.ifPresent(offset -> metadata.put(OFFSET_KEY, offset));
    upstreamOffset.ifPresent(offset -> metadata.put(UPSTREAM_OFFSET_KEY, offset));
    executionId.ifPresent(id -> metadata.put(EXECUTION_ID_KEY, id));
    adminOperationProtocolVersion.ifPresent(version -> metadata.put(ADMIN_OPERATION_PROTOCOL_VERSION_KEY, version));
    return metadata;
  }

  /**
   * @return a pair of values to which the specified keys are mapped to {@linkplain AdminTopicMetadataAccessor#OFFSET_KEY}
   * and {@linkplain AdminTopicMetadataAccessor#UPSTREAM_OFFSET_KEY}.
   */
  public static Pair<Long, Long> getOffsets(Map<String, Long> metadata) {
    long localOffset = metadata.getOrDefault(OFFSET_KEY, UNDEFINED_VALUE);
    long upstreamOffset = metadata.getOrDefault(UPSTREAM_OFFSET_KEY, UNDEFINED_VALUE);
    return new Pair<>(localOffset, upstreamOffset);
  }

  public Pair<PubSubPosition, PubSubPosition> getPositions(AdminMetadata metadata) {
    return new Pair<>(metadata.getPosition(), metadata.getUpstreamPosition());
  }

  /**
   * @return the value to which the specified key is mapped to {@linkplain AdminTopicMetadataAccessor#EXECUTION_ID_KEY}.
   */
  public static long getExecutionId(Map<String, Long> metadata) {
    return metadata.getOrDefault(EXECUTION_ID_KEY, UNDEFINED_VALUE);
  }

  /**
   * @return the value to which the specified key is mapped to {@linkplain AdminTopicMetadataAccessor#ADMIN_OPERATION_PROTOCOL_VERSION_KEY}.
   */
  public static long getAdminOperationProtocolVersion(Map<String, Long> metadata) {
    return metadata.getOrDefault(ADMIN_OPERATION_PROTOCOL_VERSION_KEY, UNDEFINED_VALUE);
  }

  /**
   * @return a pair of values representing local and upstream offsets
   */
  public static Pair<Long, Long> getOffsets(AdminMetadata metadata) {
    long localOffset = metadata.getOffset() != null ? metadata.getOffset() : UNDEFINED_VALUE;
    long upstreamOffset = metadata.getUpstreamOffset() != null ? metadata.getUpstreamOffset() : UNDEFINED_VALUE;
    return new Pair<>(localOffset, upstreamOffset);
  }

  /**
   * @return the execution ID from the metadata
   */
  public static long getExecutionId(AdminMetadata metadata) {
    return metadata.getExecutionId() != null ? metadata.getExecutionId() : UNDEFINED_VALUE;
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
