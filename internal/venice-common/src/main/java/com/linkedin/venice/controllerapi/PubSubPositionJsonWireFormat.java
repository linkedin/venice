package com.linkedin.venice.controllerapi;

import static com.linkedin.venice.pubsub.PubSubUtil.getBase64EncodedString;

import com.linkedin.venice.protocols.controller.PubSubPositionGrpcWireFormat;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.utils.ByteUtils;
import java.util.Objects;


public class PubSubPositionJsonWireFormat {
  /**
   * JSON-friendly representation of {@link PubSubPositionWireFormat} for admin metadata serialization.
   * This class mirrors the structure of PubSubPositionWireFormat but uses JSON-friendly types.
   */
  private Integer typeId;
  private String base64PositionBytes; // Base64 encoded bytes

  public PubSubPositionJsonWireFormat() {
    // required by Jackson
  }

  public PubSubPositionJsonWireFormat(Integer typeId, String base64PositionBytes) {
    this.typeId = typeId;
    this.base64PositionBytes = base64PositionBytes;
  }

  public static PubSubPositionJsonWireFormat fromWireFormatByteBuffer(PubSubPositionWireFormat wireFormat) {
    return new PubSubPositionJsonWireFormat(
        wireFormat.getType(),
        getBase64EncodedString(ByteUtils.extractByteArray(wireFormat.getRawBytes())));
  }

  public static PubSubPositionJsonWireFormat fromGrpcWireFormat(PubSubPositionGrpcWireFormat pubSubPosition) {
    return new PubSubPositionJsonWireFormat(pubSubPosition.getTypeId(), pubSubPosition.getBase64PositionBytes());
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
    return "PubSubPositionJsonWireFormat{" + "typeId=" + typeId + ", positionBytes=" + base64PositionBytes + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PubSubPositionJsonWireFormat that = (PubSubPositionJsonWireFormat) o;

    if (!Objects.equals(typeId, that.typeId)) {
      return false;
    } else {
      return Objects.equals(base64PositionBytes, that.base64PositionBytes);
    }
  }

  @Override
  public int hashCode() {
    int result = typeId != null ? typeId.hashCode() : 0;
    result = 31 * result + (base64PositionBytes != null ? base64PositionBytes.hashCode() : 0);
    return result;
  }
}
