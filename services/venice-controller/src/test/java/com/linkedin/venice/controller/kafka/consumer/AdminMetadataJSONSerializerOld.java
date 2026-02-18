package com.linkedin.venice.controller.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.controllerapi.PubSubPositionJsonWireFormat;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Old version of AdminMetadataJSONSerializer for testing backward/forward compatibility.
 * This represents the code before offset fields were removed - it includes offset and upstreamOffset
 * in the serialized JSON.
 * TODO(sushant): Remove this class after we are sure that all controllers have been upgraded and there is no risk of compatibility issues.
 */
public class AdminMetadataJSONSerializerOld implements VeniceSerializer<AdminMetadataOld> {
  private final static int serializedMapSizeLimit = 0xfffff;
  protected static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private final PubSubPositionDeserializer pubSubPositionDeserializer;

  public AdminMetadataJSONSerializerOld(PubSubPositionDeserializer pubSubPositionDeserializer) {
    this.pubSubPositionDeserializer = pubSubPositionDeserializer;
  }

  public byte[] serialize(AdminMetadataOld adminMetadata, String path) throws IOException {
    AdminMetadataJSON adminMetadataJSON = toAdminMetadataJSON(adminMetadata);
    byte[] serializedObject = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(adminMetadataJSON);
    if (serializedObject.length > serializedMapSizeLimit) {
      throw new IOException("Serialized object exceeded the size limit of " + serializedMapSizeLimit + " bytes");
    }
    return serializedObject;
  }

  private static AdminMetadataJSON toAdminMetadataJSON(AdminMetadataOld adminMetadata) {
    AdminMetadataJSON adminMetadataJSON = new AdminMetadataJSON();
    adminMetadataJSON.executionId = adminMetadata.getExecutionId();
    adminMetadataJSON.offset = adminMetadata.getOffset();
    adminMetadataJSON.upstreamOffset = adminMetadata.getUpstreamOffset();
    adminMetadataJSON.adminOperationProtocolVersion = adminMetadata.getAdminOperationProtocolVersion();
    adminMetadataJSON.pubSubPositionJsonWireFormat = adminMetadata.getPosition().toJsonWireFormat();
    adminMetadataJSON.pubSubUpstreamPositionJsonWireFormat = adminMetadata.getUpstreamPosition().toJsonWireFormat();

    return adminMetadataJSON;
  }

  public AdminMetadataOld deserialize(byte[] bytes, String path) throws IOException {
    AdminMetadataJSON adminMetadataJSON = OBJECT_MAPPER.readValue(bytes, AdminMetadataJSON.class);
    return toAdminMetadata(adminMetadataJSON);
  }

  private AdminMetadataOld toAdminMetadata(AdminMetadataJSON adminMetadataJSON) {
    AdminMetadataOld adminMetadata = new AdminMetadataOld();
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

  static final class AdminMetadataJSON {
    public Long executionId;
    public Long offset;
    public Long upstreamOffset;
    public Long adminOperationProtocolVersion;
    public PubSubPositionJsonWireFormat pubSubPositionJsonWireFormat;
    public PubSubPositionJsonWireFormat pubSubUpstreamPositionJsonWireFormat;
  }
}
