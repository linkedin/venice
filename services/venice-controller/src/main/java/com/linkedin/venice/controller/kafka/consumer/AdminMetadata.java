package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.controller.AdminTopicMetadataAccessor.UNDEFINED_VALUE;
import static com.linkedin.venice.pubsub.PubSubUtil.getBase64DecodedBytes;
import static com.linkedin.venice.pubsub.PubSubUtil.getBase64EncodedString;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Inner class representing admin topic metadata with strongly typed fields
 */
public class AdminMetadata {
  private Long executionId;
  private Long offset;
  private Long upstreamOffset;
  private Long adminOperationProtocolVersion;
  private PubSubPositionJsonWireFormat pubSubPositionJsonWireFormat;
  private PubSubPositionJsonWireFormat pubSubUpstreamPositionJsonWireFormat;

  public AdminMetadata() {
  }

  /**
   * Factory method to create AdminMetadata from legacy Map<String, Long> format
   */
  public static AdminMetadata fromLegacyMap(Map<String, Long> legacyMap) {
    AdminMetadata metadata = new AdminMetadata();
    if (legacyMap != null) {
      metadata.executionId = legacyMap.get(AdminTopicMetadataAccessor.EXECUTION_ID_KEY);
      metadata.offset = legacyMap.get(AdminTopicMetadataAccessor.OFFSET_KEY);
      metadata.upstreamOffset = legacyMap.get(AdminTopicMetadataAccessor.UPSTREAM_OFFSET_KEY);
      metadata.adminOperationProtocolVersion =
          legacyMap.get(AdminTopicMetadataAccessor.ADMIN_OPERATION_PROTOCOL_VERSION_KEY);
      metadata.pubSubPositionJsonWireFormat = metadata.offset == null
          ? null
          : PubSubPositionJsonWireFormat
              .fromWireFormat(ApacheKafkaOffsetPosition.of(metadata.offset).getPositionWireFormat());
      metadata.pubSubUpstreamPositionJsonWireFormat = metadata.upstreamOffset == null
          ? null
          : PubSubPositionJsonWireFormat
              .fromWireFormat(ApacheKafkaOffsetPosition.of(metadata.upstreamOffset).getPositionWireFormat());
    }
    return metadata;
  }

  public AdminMetadata(Map<String, Object> metadataMap) {
    this.executionId = getLongValue(metadataMap, AdminTopicMetadataAccessor.EXECUTION_ID_KEY);
    this.offset = getLongValue(metadataMap, AdminTopicMetadataAccessor.OFFSET_KEY);
    this.upstreamOffset = getLongValue(metadataMap, AdminTopicMetadataAccessor.UPSTREAM_OFFSET_KEY);
    this.adminOperationProtocolVersion =
        getLongValue(metadataMap, AdminTopicMetadataAccessor.ADMIN_OPERATION_PROTOCOL_VERSION_KEY);

    // Convert PubSubPositionWireFormat to JSON-friendly format
    Object positionObj = metadataMap.get(AdminTopicMetadataAccessor.POSITION_KEY);
    if (positionObj instanceof PubSubPositionWireFormat) {
      this.pubSubPositionJsonWireFormat =
          PubSubPositionJsonWireFormat.fromWireFormat((PubSubPositionWireFormat) positionObj);
    }

    Object upstreamPositionObj = metadataMap.get(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY);
    if (upstreamPositionObj instanceof PubSubPositionWireFormat) {
      this.pubSubUpstreamPositionJsonWireFormat =
          PubSubPositionJsonWireFormat.fromWireFormat((PubSubPositionWireFormat) upstreamPositionObj);
    }
  }

  private Long getLongValue(Map<String, Object> map, String key) {
    Object value = map.get(key);
    if (value == null)
      return null;
    if (value instanceof Long)
      return (Long) value;
    if (value instanceof Integer)
      return ((Integer) value).longValue();
    if (value instanceof Number)
      return ((Number) value).longValue();
    return null;
  }

  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    if (executionId != null)
      map.put(AdminTopicMetadataAccessor.EXECUTION_ID_KEY, executionId);
    if (offset != null)
      map.put(AdminTopicMetadataAccessor.OFFSET_KEY, offset);
    if (upstreamOffset != null)
      map.put(AdminTopicMetadataAccessor.UPSTREAM_OFFSET_KEY, upstreamOffset);
    if (adminOperationProtocolVersion != null)
      map.put(AdminTopicMetadataAccessor.ADMIN_OPERATION_PROTOCOL_VERSION_KEY, adminOperationProtocolVersion);

    // Convert back to PubSubPositionWireFormat for compatibility
    if (pubSubPositionJsonWireFormat != null)
      map.put(AdminTopicMetadataAccessor.POSITION_KEY, pubSubPositionJsonWireFormat.toWireFormat());
    if (pubSubUpstreamPositionJsonWireFormat != null)
      map.put(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY, pubSubUpstreamPositionJsonWireFormat.toWireFormat());

    return map;
  }

  /**
   * Convert AdminMetadata to legacy Map<String, Long> format for V1 compatibility
   * This only includes the Long fields and excludes Position objects
   * @return Map<String, Long> containing only the Long fields
   */
  public Map<String, Long> toLegacyMap() {
    Map<String, Long> legacyMap = new HashMap<>();
    if (executionId != null)
      legacyMap.put(AdminTopicMetadataAccessor.EXECUTION_ID_KEY, executionId);
    if (offset != null)
      legacyMap.put(AdminTopicMetadataAccessor.OFFSET_KEY, offset);
    if (upstreamOffset != null)
      legacyMap.put(AdminTopicMetadataAccessor.UPSTREAM_OFFSET_KEY, upstreamOffset);
    if (adminOperationProtocolVersion != null)
      legacyMap.put(AdminTopicMetadataAccessor.ADMIN_OPERATION_PROTOCOL_VERSION_KEY, adminOperationProtocolVersion);
    return legacyMap;
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

  public PubSubPositionJsonWireFormat getPubSubPositionJsonWireFormat() {
    return pubSubPositionJsonWireFormat;
  }

  public PubSubPositionJsonWireFormat getPubSubUpstreamPositionJsonWireFormat() {
    return pubSubUpstreamPositionJsonWireFormat;
  }

  @JsonIgnore
  public PubSubPosition getPosition() throws IOException {
    return getPubSubPosition(pubSubPositionJsonWireFormat, offset);
  }

  @Nullable
  private PubSubPosition getPubSubPosition(PubSubPositionJsonWireFormat pubSubPositionJsonWireFormat, Long offset)
      throws IOException {
    if (pubSubPositionJsonWireFormat == null) {
      if (!offset.equals(UNDEFINED_VALUE)) {
        return ApacheKafkaOffsetPosition.of(offset);
      } else {
        return null;
      }
    }
    PubSubPositionWireFormat wireFormat = pubSubPositionJsonWireFormat.toWireFormat();
    if (wireFormat.getRawBytes() == null) {
      return null;
    }
    return ApacheKafkaOffsetPosition.of(wireFormat.getRawBytes());
  }

  @JsonIgnore
  public PubSubPosition getUpstreamPosition() throws IOException {
    return getPubSubPosition(pubSubUpstreamPositionJsonWireFormat, upstreamOffset);
  }

  public void setPubSubPositionJsonWireFormat(PubSubPositionJsonWireFormat pubSubPositionJsonWireFormat) {
    this.pubSubPositionJsonWireFormat = pubSubPositionJsonWireFormat;
  }

  public void setPubSubUpstreamPositionJsonWireFormat(
      PubSubPositionJsonWireFormat pubSubUpstreamPositionJsonWireFormat) {
    this.pubSubUpstreamPositionJsonWireFormat = pubSubUpstreamPositionJsonWireFormat;
  }

  @Override
  public String toString() {
    return "AdminMetadata{" + "executionId=" + executionId + ", offset=" + offset + ", upstreamOffset=" + upstreamOffset
        + ", adminOperationProtocolVersion=" + adminOperationProtocolVersion + ", position="
        + pubSubPositionJsonWireFormat + ", upstreamPosition=" + pubSubUpstreamPositionJsonWireFormat + '}';
  }

  /**
   * JSON-friendly representation of PubSubPositionWireFormat for admin metadata serialization.
   * This class mirrors the structure of PubSubPositionWireFormat but uses JSON-friendly types.
   */
  @VisibleForTesting
  public static class PubSubPositionJsonWireFormat {
    private Integer typeId;
    private String base64PositionBytes; // Base64 encoded bytes

    public PubSubPositionJsonWireFormat() {
      // needed for serialization/deserialization by Jackson
    }

    public PubSubPositionJsonWireFormat(Integer typeId, String base64PositionBytes) {
      this.typeId = typeId;
      this.base64PositionBytes = base64PositionBytes;
    }

    /**
     * Convert from PubSubPositionWireFormat to JSON-friendly format
     */
    public static PubSubPositionJsonWireFormat fromWireFormat(PubSubPositionWireFormat wireFormat) {
      return wireFormat == null
          ? null
          : new PubSubPositionJsonWireFormat(
              wireFormat.getType(),
              getBase64EncodedString(wireFormat.getRawBytes().array()));
    }

    /**
     * Convert to PubSubPositionWireFormat from JSON-friendly format
     */
    public PubSubPositionWireFormat toWireFormat() {
      if (typeId == null || base64PositionBytes == null) {
        throw new VeniceException(
            "Cannot convert PubSubPositionJsonWireFormat to PubSubPositionWireFormat. typeId: " + typeId
                + ", positionBytes: " + base64PositionBytes);
      }
      return new PubSubPositionWireFormat(typeId, ByteBuffer.wrap(getBase64DecodedBytes(base64PositionBytes)));
    }

    // Getters and setters
    public Integer getTypeId() {
      return typeId;
    }

    public void setTypeId(Integer typeId) {
      this.typeId = typeId;
    }

    public String getBase64PositionBytes() {
      return base64PositionBytes;
    }

    public void setBase64PositionBytes(String base64PositionBytes) {
      this.base64PositionBytes = base64PositionBytes;
    }

    @Override
    public String toString() {
      return "PubSubPositionJsonWireFormat{" + "typeId=" + typeId + ", positionBytes='" + base64PositionBytes + '\''
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      PubSubPositionJsonWireFormat that = (PubSubPositionJsonWireFormat) o;

      if (typeId != null ? !typeId.equals(that.typeId) : that.typeId != null)
        return false;
      return base64PositionBytes != null
          ? base64PositionBytes.equals(that.base64PositionBytes)
          : that.base64PositionBytes == null;
    }

    @Override
    public int hashCode() {
      int result = typeId != null ? typeId.hashCode() : 0;
      result = 31 * result + (base64PositionBytes != null ? base64PositionBytes.hashCode() : 0);
      return result;
    }
  }
}
