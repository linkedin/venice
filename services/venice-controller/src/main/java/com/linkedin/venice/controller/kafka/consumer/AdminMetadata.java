package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.controller.AdminTopicMetadataAccessor.UNDEFINED_VALUE;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


/**
 * Class representing admin topic metadata with strongly typed fields
 */
public class AdminMetadata {
  private Long executionId;
  private Long adminOperationProtocolVersion;
  private PubSubPosition position = PubSubSymbolicPosition.EARLIEST;
  private PubSubPosition upstreamPosition = PubSubSymbolicPosition.EARLIEST;

  public AdminMetadata() {
  }

  public AdminMetadata(Map<String, Object> metadataMap) {
    this.executionId = getLongValue(metadataMap, AdminTopicMetadataAccessor.EXECUTION_ID_KEY);
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
    if (adminOperationProtocolVersion != null) {
      map.put(AdminTopicMetadataAccessor.ADMIN_OPERATION_PROTOCOL_VERSION_KEY, adminOperationProtocolVersion);
    }

    map.put(AdminTopicMetadataAccessor.POSITION_KEY, position);
    map.put(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY, upstreamPosition);

    return map;
  }

  // Getters and setters
  public Long getExecutionId() {
    return executionId == null ? UNDEFINED_VALUE : executionId;
  }

  public void setExecutionId(Long executionId) {
    this.executionId = executionId;
  }

  public Long getAdminOperationProtocolVersion() {
    return adminOperationProtocolVersion == null ? UNDEFINED_VALUE : adminOperationProtocolVersion;
  }

  public void setAdminOperationProtocolVersion(Long adminOperationProtocolVersion) {
    this.adminOperationProtocolVersion = adminOperationProtocolVersion;
  }

  public PubSubPosition getPosition() {
    return position;
  }

  public PubSubPosition getUpstreamPosition() {
    return upstreamPosition;
  }

  public void setPubSubPosition(PubSubPosition pubSubPosition) {
    this.position = pubSubPosition;
  }

  public void setUpstreamPubSubPosition(PubSubPosition upstreamPubPosition) {
    this.upstreamPosition = upstreamPubPosition;
  }

  @Override
  public String toString() {
    return "AdminMetadata{" + "executionId=" + executionId + ", adminOperationProtocolVersion="
        + adminOperationProtocolVersion + ", position=" + position + ", upstreamPosition=" + upstreamPosition + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AdminMetadata that = (AdminMetadata) o;
    return Objects.equals(this.getExecutionId(), that.getExecutionId())
        && Objects.equals(this.getAdminOperationProtocolVersion(), that.getAdminOperationProtocolVersion())
        && Objects.equals(this.getPosition(), that.getPosition())
        && Objects.equals(this.getUpstreamPosition(), that.getUpstreamPosition());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.getExecutionId(),
        this.getAdminOperationProtocolVersion(),
        this.getPosition(),
        this.getUpstreamPosition());
  }
}
