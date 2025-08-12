package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.controller.AdminTopicMetadataAccessor.UNDEFINED_VALUE;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import java.util.HashMap;
import java.util.Map;


/**
 * Inner class representing admin topic metadata with strongly typed fields
 */
public class AdminMetadata {
  private Long executionId;
  private Long offset;
  private Long upstreamOffset;
  private Long adminOperationProtocolVersion;
  private PubSubPositionJsonWireFormat position;
  private PubSubPositionJsonWireFormat upstreamPosition;

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
      // Note: Position objects are not available in legacy format
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
      this.position = PubSubPositionJsonWireFormat.fromWireFormat((PubSubPositionWireFormat) positionObj);
    }

    Object upstreamPositionObj = metadataMap.get(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY);
    if (upstreamPositionObj instanceof PubSubPositionWireFormat) {
      this.upstreamPosition =
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
    if (position != null)
      map.put(AdminTopicMetadataAccessor.POSITION_KEY, position.toWireFormat());
    if (upstreamPosition != null)
      map.put(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY, upstreamPosition.toWireFormat());

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

  public PubSubPositionJsonWireFormat getPosition() {
    return position;
  }

  public void setPosition(PubSubPositionJsonWireFormat position) {
    this.position = position;
  }

  public PubSubPositionJsonWireFormat getUpstreamPosition() {
    return upstreamPosition;
  }

  public void setUpstreamPosition(PubSubPositionJsonWireFormat upstreamPosition) {
    this.upstreamPosition = upstreamPosition;
  }

  @Override
  public String toString() {
    return "AdminMetadata{" + "executionId=" + executionId + ", offset=" + offset + ", upstreamOffset=" + upstreamOffset
        + ", adminOperationProtocolVersion=" + adminOperationProtocolVersion + ", position=" + position
        + ", upstreamPosition=" + upstreamPosition + '}';
  }

  /**
   * JSON-friendly representation of PubSubPositionWireFormat for admin metadata serialization.
   * This class mirrors the structure of PubSubPositionWireFormat but uses JSON-friendly types.
   */
  public static class PubSubPositionJsonWireFormat {
    private Integer typeId;
    private String positionBytes; // Base64 encoded bytes

    public PubSubPositionJsonWireFormat() {
    }

    public PubSubPositionJsonWireFormat(Integer typeId, String positionBytes) {
      this.typeId = typeId;
      this.positionBytes = positionBytes;
    }

    /**
     * Convert from PubSubPositionWireFormat to JSON-friendly format
     */
    public static PubSubPositionJsonWireFormat fromWireFormat(PubSubPositionWireFormat wireFormat) {
      if (wireFormat == null)
        return null;

      String encodedBytes = wireFormat.getRawBytes() != null
          ? java.util.Base64.getEncoder().encodeToString(wireFormat.getRawBytes().array())
          : null;

      return new PubSubPositionJsonWireFormat(wireFormat.getType(), encodedBytes);
    }

    /**
     * Convert to PubSubPositionWireFormat from JSON-friendly format
     */
    public PubSubPositionWireFormat toWireFormat() {
      PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();

      if (typeId != null) {
        wireFormat.setType(typeId);
      }

      if (positionBytes != null) {
        byte[] rawBytes = java.util.Base64.getDecoder().decode(positionBytes);
        wireFormat.setRawBytes(java.nio.ByteBuffer.wrap(rawBytes));
      }

      return wireFormat;
    }

    // Getters and setters
    public Integer getTypeId() {
      return typeId;
    }

    public void setTypeId(Integer typeId) {
      this.typeId = typeId;
    }

    public String getPositionBytes() {
      return positionBytes;
    }

    public void setPositionBytes(String positionBytes) {
      this.positionBytes = positionBytes;
    }

    @Override
    public String toString() {
      return "PubSubPositionJsonWireFormat{" + "typeId=" + typeId + ", positionBytes='" + positionBytes + '\'' + '}';
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
      return positionBytes != null ? positionBytes.equals(that.positionBytes) : that.positionBytes == null;
    }

    @Override
    public int hashCode() {
      int result = typeId != null ? typeId.hashCode() : 0;
      result = 31 * result + (positionBytes != null ? positionBytes.hashCode() : 0);
      return result;
    }
  }
}
