package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.controller.AdminTopicMetadataAccessor.UNDEFINED_VALUE;

import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Old version of AdminMetadata class for testing backward/forward compatibility.
 * This represents the code before offset fields were removed.
 * TODO(sushant): Remove this class after we are sure that all controllers have been upgraded and there is no risk of compatibility issues.
 */
public class AdminMetadataOld {
  private Long executionId;
  private Long offset;
  private Long upstreamOffset;
  private Long adminOperationProtocolVersion;
  private PubSubPosition position;
  private PubSubPosition upstreamPosition;
  private static final Logger LOGGER = LogManager.getLogger(AdminMetadataOld.class);

  public AdminMetadataOld() {
  }

  // Getters and setters
  public Long getExecutionId() {
    return executionId == null ? UNDEFINED_VALUE : executionId;
  }

  public void setExecutionId(Long executionId) {
    this.executionId = executionId;
  }

  public Long getOffset() {
    return offset == null ? UNDEFINED_VALUE : offset;
  }

  public void setOffset(Long offset) {
    this.offset = offset;
  }

  public Long getUpstreamOffset() {
    return upstreamOffset == null ? UNDEFINED_VALUE : upstreamOffset;
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
    } else if (offset != null && offset > position.getNumericOffset()) {
      LOGGER.warn(
          "Offset {} is greater than position {}. Resetting position to offset.",
          offset,
          position.getNumericOffset());
      return ApacheKafkaOffsetPosition.of(offset);
    } else {
      return position;
    }
  }

  public void setPubSubPosition(PubSubPosition pubSubPosition) {
    this.position = pubSubPosition;
    if (pubSubPosition != null) {
      this.offset = pubSubPosition.getNumericOffset();
    } else {
      this.offset = UNDEFINED_VALUE;
    }
  }

  public void setUpstreamPubSubPosition(PubSubPosition upstreamPubPosition) {
    this.upstreamPosition = upstreamPubPosition;
    if (upstreamPubPosition != null) {
      this.upstreamOffset = upstreamPubPosition.getNumericOffset();
    } else {
      this.upstreamOffset = UNDEFINED_VALUE;
    }
  }

  @Override
  public String toString() {
    return "AdminMetadataOld{" + "executionId=" + executionId + ", offset=" + offset + ", upstreamOffset="
        + upstreamOffset + ", adminOperationProtocolVersion=" + adminOperationProtocolVersion + ", position=" + position
        + ", upstreamPosition=" + upstreamPosition + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AdminMetadataOld that = (AdminMetadataOld) o;
    return Objects.equals(this.getExecutionId(), that.getExecutionId())
        && Objects.equals(this.getOffset(), that.getOffset())
        && Objects.equals(this.getUpstreamOffset(), that.getUpstreamOffset())
        && Objects.equals(this.getAdminOperationProtocolVersion(), that.getAdminOperationProtocolVersion())
        && Objects.equals(this.getPosition(), that.getPosition())
        && Objects.equals(this.getUpstreamPosition(), that.getUpstreamPosition());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.getExecutionId(),
        this.getOffset(),
        this.getUpstreamOffset(),
        this.getAdminOperationProtocolVersion(),
        this.getPosition(),
        this.getUpstreamPosition());
  }
}
