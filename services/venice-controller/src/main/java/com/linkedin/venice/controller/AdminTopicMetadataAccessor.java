package com.linkedin.venice.controller;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.utils.Pair;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public abstract class AdminTopicMetadataAccessor {
  private static final String OFFSET_KEY = "offset";
  public static final String POSITION_KEY = "position";
  /**
   * When remote consumption is enabled, child controller will consume directly from the source admin topic; an extra
   * metadata called upstream offset will be maintained, which indicate the last offset in the source admin topic that
   * gets processed successfully.
   */
  private static final String UPSTREAM_OFFSET_KEY = "upstreamOffset";
  public static final String UPSTREAM_POSITION_KEY = "upstreamPosition";
  private static final String EXECUTION_ID_KEY = "executionId";
  private static final String ADMIN_OPERATION_PROTOCOL_VERSION_KEY = "adminOperationProtocolVersion";
  private static final long UNDEFINED_VALUE = -1;

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

  public static Map<String, Position> generatePositionMetadataMap(Position localPosition, Position upstreamPosition) {
    Map<String, Position> metadata = new HashMap<>();
    metadata.put(POSITION_KEY, localPosition);
    metadata.put(UPSTREAM_POSITION_KEY, upstreamPosition);
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

  public static Pair<Position, Position> getPositions(Map<String, Position> metadata) {
    Position localPosition = metadata.get(POSITION_KEY);
    Position upstreamPosition = metadata.get(UPSTREAM_POSITION_KEY);
    return new Pair<>(localPosition, upstreamPosition);
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
   * Update specific metadata for a given cluster in a single transaction with information provided in metadata.
   * @param clusterName of the cluster at interest.
   * @param metadata map containing relevant information.
   */
  public abstract void updateMetadata(String clusterName, Map<String, Long> metadata);

  public abstract void updatePositionMetadata(String clusterName, Map<String, Position> metadata);

  /**
   * Retrieve the latest metadata map.
   * @param clusterName of the cluster at interest.
   */
  public abstract Map<String, Long> getMetadata(String clusterName);

  public abstract Map<String, Position> getPositionMetadata(String clusterName);

  public static class Position {
    @JsonProperty("typeId")
    public int typeId;

    @JsonProperty("positionBytes")
    public byte[] positionBytes;

    // Default constructor for Jackson
    public Position() {
    }

    // Constructor for manual creation
    public Position(int typeId, byte[] positionBytes) {
      this.typeId = typeId;
      this.positionBytes = new byte[positionBytes.length];
      System.arraycopy(positionBytes, 0, this.positionBytes, 0, positionBytes.length);
    }

    public static Position parseFrom(int typeId, byte[] positionBytes) {
      return new Position(typeId, positionBytes);
    }
  }
}
