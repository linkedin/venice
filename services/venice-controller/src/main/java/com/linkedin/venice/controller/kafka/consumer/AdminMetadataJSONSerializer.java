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
    adminMetadataJSON.adminOperationProtocolVersion = adminMetadata.getAdminOperationProtocolVersion();
    adminMetadataJSON.pubSubPositionJsonWireFormat = adminMetadata.getPosition().toJsonWireFormat();
    adminMetadataJSON.pubSubUpstreamPositionJsonWireFormat = adminMetadata.getUpstreamPosition().toJsonWireFormat();
    return adminMetadataJSON;
  }

  public AdminMetadata deserialize(byte[] bytes, String path) throws IOException {
    AdminMetadataJSON adminMetadataJSON = OBJECT_MAPPER.readValue(bytes, AdminMetadataJSON.class);
    return toAdminMetadata(adminMetadataJSON);
  }

  private AdminMetadata toAdminMetadata(AdminMetadataJSON adminMetadataJSON) {
    AdminMetadata adminMetadata = new AdminMetadata();
    adminMetadata.setAdminOperationProtocolVersion(adminMetadataJSON.adminOperationProtocolVersion);
    adminMetadata.setExecutionId(adminMetadataJSON.executionId);
    // Only use position fields; offset fields (if present in old data) are ignored.
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

  /**
   * JSON representation of AdminMetadata for ZooKeeper storage.
   * Only contains position-based fields; numeric offset fields have been removed.
   */
  static final class AdminMetadataJSON {
    public Long executionId;
    public Long adminOperationProtocolVersion;
    public PubSubPositionJsonWireFormat pubSubPositionJsonWireFormat;
    public PubSubPositionJsonWireFormat pubSubUpstreamPositionJsonWireFormat;
  }
}
