package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.pubsub.PubSubUtil.getBase64EncodedString;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;


public class AdminMetadataJSONSerializer implements VeniceSerializer<AdminMetadata> {
  private final static int serializedMapSizeLimit = 0xfffff;
  protected static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private final PubSubPositionDeserializer pubSubPositionDeserializer;

  public AdminMetadataJSONSerializer(PubSubPositionDeserializer pubSubPositionDeserializer) {
    this.pubSubPositionDeserializer = pubSubPositionDeserializer;
  }

  public byte[] serialize(AdminMetadata adminMetadata, String path) throws IOException {
    AdminMetadataJSON adminMetadataJSON = toAdminMetadataJSON(adminMetadata);
    byte[] serializedObject = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(adminMetadataJSON);
    if (serializedObject.length > serializedMapSizeLimit) {
      throw new IOException("Serialized object exceeded the size limit of " + serializedMapSizeLimit + " bytes");
    }
    return serializedObject;
  }

  private static AdminMetadataJSON toAdminMetadataJSON(AdminMetadata adminMetadata) {
    AdminMetadataJSON adminMetadataJSON = new AdminMetadataJSON();
    adminMetadataJSON.executionId = adminMetadata.getExecutionId();
    adminMetadataJSON.offset = adminMetadata.getOffset();
    adminMetadataJSON.upstreamOffset = adminMetadata.getUpstreamOffset();
    adminMetadataJSON.adminOperationProtocolVersion = adminMetadata.getAdminOperationProtocolVersion();
    adminMetadataJSON.pubSubPositionJsonWireFormat =
        PubSubPositionJsonWireFormat.fromPubSubPosition(adminMetadata.getPosition());
    adminMetadataJSON.pubSubUpstreamPositionJsonWireFormat =
        PubSubPositionJsonWireFormat.fromPubSubPosition(adminMetadata.getUpstreamPosition());

    return adminMetadataJSON;
  }

  public AdminMetadata deserialize(byte[] bytes, String path) throws IOException {
    AdminMetadataJSON adminMetadataJSON = OBJECT_MAPPER.readValue(bytes, AdminMetadataJSON.class);
    return toAdminMetadata(adminMetadataJSON);
  }

  private AdminMetadata toAdminMetadata(AdminMetadataJSON adminMetadataJSON) {
    AdminMetadata adminMetadata = new AdminMetadata();
    adminMetadata.setOffset(adminMetadataJSON.offset);
    adminMetadata.setUpstreamOffset(adminMetadataJSON.upstreamOffset);
    adminMetadata.setAdminOperationProtocolVersion(adminMetadataJSON.adminOperationProtocolVersion);
    adminMetadata.setExecutionId(adminMetadataJSON.executionId);
    adminMetadata.setPubSubPosition(jsonWireFormatToPosition(adminMetadataJSON.pubSubPositionJsonWireFormat));
    adminMetadata
        .setUpstreamPubSubPosition(jsonWireFormatToPosition(adminMetadataJSON.pubSubUpstreamPositionJsonWireFormat));

    return adminMetadata;
  }

  private PubSubPosition jsonWireFormatToPosition(PubSubPositionJsonWireFormat pubSubPositionJsonWireFormat) {
    return pubSubPositionDeserializer.toPosition(
        new PubSubPositionWireFormat(
            pubSubPositionJsonWireFormat.getTypeId(),
            ByteBuffer.wrap(PubSubUtil.getBase64DecodedBytes(pubSubPositionJsonWireFormat.getBase64PositionBytes()))));
  }

  private static final class AdminMetadataJSON {
    public Long executionId;
    public Long offset;
    public Long upstreamOffset;
    public Long adminOperationProtocolVersion;
    public PubSubPositionJsonWireFormat pubSubPositionJsonWireFormat;
    public PubSubPositionJsonWireFormat pubSubUpstreamPositionJsonWireFormat;
  }

  /**
   * JSON-friendly representation of {@link PubSubPositionWireFormat} for admin metadata serialization.
   * This class mirrors the structure of PubSubPositionWireFormat but uses JSON-friendly types.
   */
  private static class PubSubPositionJsonWireFormat {
    private Integer typeId;
    private String base64PositionBytes; // Base64 encoded bytes

    public PubSubPositionJsonWireFormat() {
      // required by Jackson
    }

    public PubSubPositionJsonWireFormat(Integer typeId, String base64PositionBytes) {
      this.typeId = typeId;
      this.base64PositionBytes = base64PositionBytes;
    }

    public static PubSubPositionJsonWireFormat fromPubSubPosition(PubSubPosition pubSubPosition) {
      return fromWireFormatByteBuffer(pubSubPosition.getPositionWireFormat());
    }

    public static PubSubPositionJsonWireFormat fromWireFormatByteBuffer(PubSubPositionWireFormat wireFormat) {
      return new PubSubPositionJsonWireFormat(
          wireFormat.getType(),
          getBase64EncodedString(ByteUtils.extractByteArray(wireFormat.getRawBytes())));
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
}
