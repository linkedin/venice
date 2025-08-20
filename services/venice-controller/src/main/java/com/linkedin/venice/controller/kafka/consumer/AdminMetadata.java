package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.controller.AdminTopicMetadataAccessor.UNDEFINED_VALUE;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import java.util.HashMap;
import java.util.Map;


/**
 * Class representing admin topic metadata with strongly typed fields
 */
public class AdminMetadata {
  private Long executionId;
  private Long offset;
  private Long upstreamOffset;
  private Long adminOperationProtocolVersion;
  private PubSubPosition position;
  private PubSubPosition upstreamPosition;

  public AdminMetadata() {
  }

  public AdminMetadata(Map<String, Object> metadataMap) {
    this.executionId = getLongValue(metadataMap, AdminTopicMetadataAccessor.EXECUTION_ID_KEY);
    this.offset = getLongValue(metadataMap, AdminTopicMetadataAccessor.OFFSET_KEY);
    this.upstreamOffset = getLongValue(metadataMap, AdminTopicMetadataAccessor.UPSTREAM_OFFSET_KEY);
    this.adminOperationProtocolVersion =
        getLongValue(metadataMap, AdminTopicMetadataAccessor.ADMIN_OPERATION_PROTOCOL_VERSION_KEY);

    // Convert PubSubPositionWireFormat to JSON-friendly format
    Object positionObj = metadataMap.get(AdminTopicMetadataAccessor.POSITION_KEY);
    if (positionObj instanceof PubSubPosition) {
      this.position = (PubSubPosition) positionObj;
    }

    Object upstreamPositionObj = metadataMap.get(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY);
    if (upstreamPositionObj instanceof PubSubPosition) {
      this.upstreamPosition = (PubSubPosition) upstreamPositionObj;
    }
  }

  private Long getLongValue(Map<String, Object> map, String key) {
    Object value = map.get(key);
    if (value == null) {
      return null;
    } else if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof Integer) {
      return ((Integer) value).longValue();
    } else if (value instanceof Number) {
      return ((Number) value).longValue();
    } else {
      throw new VeniceException("Unexpected value type: " + value.getClass() + " for key: " + key);
    }
  }

  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    if (executionId != null) {
      map.put(AdminTopicMetadataAccessor.EXECUTION_ID_KEY, executionId);
    }
    if (offset != null) {
      map.put(AdminTopicMetadataAccessor.OFFSET_KEY, offset);
    }
    if (upstreamOffset != null) {
      map.put(AdminTopicMetadataAccessor.UPSTREAM_OFFSET_KEY, upstreamOffset);
    }
    if (adminOperationProtocolVersion != null) {
      map.put(AdminTopicMetadataAccessor.ADMIN_OPERATION_PROTOCOL_VERSION_KEY, adminOperationProtocolVersion);
    }

    map.put(AdminTopicMetadataAccessor.POSITION_KEY, position);
    map.put(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY, upstreamPosition);

    return map;
  }

  /**
   * Convert AdminMetadata to legacy Map<String, Long> format for V1 compatibility
   * This only includes the Long fields and excludes Position objects
   * @return Map<String, Long> containing only the Long fields
   */
  public Map<String, Long> toLegacyMap() {
    Map<String, Long> legacyMap = new HashMap<>();
    if (executionId != null) {
      legacyMap.put(AdminTopicMetadataAccessor.EXECUTION_ID_KEY, executionId);
    }
    if (offset != null) {
      legacyMap.put(AdminTopicMetadataAccessor.OFFSET_KEY, offset);
    }
    if (upstreamOffset != null) {
      legacyMap.put(AdminTopicMetadataAccessor.UPSTREAM_OFFSET_KEY, upstreamOffset);
    }
    if (adminOperationProtocolVersion != null) {
      legacyMap.put(AdminTopicMetadataAccessor.ADMIN_OPERATION_PROTOCOL_VERSION_KEY, adminOperationProtocolVersion);
    }
    return legacyMap;
  }

  /**
   * Factory method to create AdminMetadata from legacy Map<String, Long> format
   */
  public static AdminMetadata fromLegacyMap(Map<String, Long> legacyMap) {
    AdminMetadata metadata = new AdminMetadata();
    if (legacyMap != null) {
      metadata.setExecutionId(legacyMap.get(AdminTopicMetadataAccessor.EXECUTION_ID_KEY));
      metadata.setOffset(legacyMap.get(AdminTopicMetadataAccessor.OFFSET_KEY));
      metadata.setUpstreamOffset(legacyMap.get(AdminTopicMetadataAccessor.UPSTREAM_OFFSET_KEY));
      metadata.setAdminOperationProtocolVersion(
          legacyMap.get(AdminTopicMetadataAccessor.ADMIN_OPERATION_PROTOCOL_VERSION_KEY));
      metadata.setPubSubPosition(
          metadata.getOffset() == null
              ? PubSubSymbolicPosition.EARLIEST
              : ApacheKafkaOffsetPosition.of(metadata.getOffset()));
      metadata.setUpstreamPubSubPosition(
          metadata.getUpstreamOffset() == null
              ? PubSubSymbolicPosition.EARLIEST
              : ApacheKafkaOffsetPosition.of(metadata.getUpstreamOffset()));
    }
    return metadata;
  }

  // Getters and setters
  public Long getExecutionId() {
    return executionId;
  }

  public void setExecutionId(Long executionId) {
    this.executionId = executionId;
  }

  public Long getOffset() {
    return offset;
  }

  public void setOffset(Long offset) {
    this.offset = offset;
  }

  public Long getUpstreamOffset() {
    return upstreamOffset;
  }

  public void setUpstreamOffset(Long upstreamOffset) {
    this.upstreamOffset = upstreamOffset;
  }

  public Long getAdminOperationProtocolVersion() {
    return adminOperationProtocolVersion == null ? UNDEFINED_VALUE : adminOperationProtocolVersion;
  }

  public void setAdminOperationProtocolVersion(Long adminOperationProtocolVersion) {
    this.adminOperationProtocolVersion = adminOperationProtocolVersion;
  }

  public PubSubPosition getPosition() {
    return getPubSubPosition(position, offset);
  }

  public PubSubPosition getUpstreamPosition() {
    return getPubSubPosition(upstreamPosition, upstreamOffset);
  }

  private PubSubPosition getPubSubPosition(PubSubPosition position, Long offset) {
    if (position == null) {
      if (offset != null && !offset.equals(UNDEFINED_VALUE)) {
        return ApacheKafkaOffsetPosition.of(offset);
      } else {
        return PubSubSymbolicPosition.EARLIEST;
      }
    } else {
      return position;
    }
  }

  public void setPubSubPosition(PubSubPosition pubSubPosition) {
    this.position = pubSubPosition;
  }

  public void setUpstreamPubSubPosition(PubSubPosition upstreamPubPosition) {
    this.upstreamPosition = upstreamPubPosition;
  }

  @Override
  public String toString() {
    return "AdminMetadata{" + "executionId=" + executionId + ", offset=" + offset + ", upstreamOffset=" + upstreamOffset
        + ", adminOperationProtocolVersion=" + adminOperationProtocolVersion + ", position=" + position
        + ", upstreamPosition=" + upstreamPosition + '}';
  }
}
