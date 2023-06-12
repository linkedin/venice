package com.linkedin.davinci.listener.response;

import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.metadata.response.VersionProperties;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Map;


/**
 * This class stores all the information required for answering a server metadata fetch request.
 */
public class MetadataResponse {
  private boolean isError;
  private String message;
  private final MetadataResponseRecord responseRecord;

  public MetadataResponse() {
    this.responseRecord = new MetadataResponseRecord();
  }

  public void setVersionMetadata(VersionProperties versionProperties) {
    responseRecord.setVersionMetadata(versionProperties);
  }

  public void setVersions(List<Integer> versions) {
    responseRecord.setVersions(versions);
  }

  public void setKeySchema(Map<CharSequence, CharSequence> keySchema) {
    responseRecord.setKeySchema(keySchema);
  }

  public void setValueSchemas(Map<CharSequence, CharSequence> valueSchemas) {
    responseRecord.setValueSchemas(valueSchemas);
  }

  public void setLatestSuperSetValueSchemaId(int latestSuperSetValueSchemaId) {
    responseRecord.setLatestSuperSetValueSchemaId(latestSuperSetValueSchemaId);
  }

  public void setRoutingInfo(Map<CharSequence, List<CharSequence>> routingInfo) {
    responseRecord.setRoutingInfo(routingInfo);
  }

  public void setHelixGroupInfo(Map<CharSequence, Integer> helixGroupInfo) {
    responseRecord.setHelixGroupInfo(helixGroupInfo);
  }

  public ByteBuf getResponseBody() {
    return Unpooled.wrappedBuffer(serializedResponse());
  }

  private byte[] serializedResponse() {
    RecordSerializer<MetadataResponseRecord> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(MetadataResponseRecord.SCHEMA$);
    return serializer.serialize(responseRecord);
  }

  public int getResponseSchemaIdHeader() {
    return AvroProtocolDefinition.SERVER_METADATA_RESPONSE_V1.getCurrentProtocolVersion();
  }

  public void setError(boolean error) {
    this.isError = error;
  }

  public boolean isError() {
    return this.isError;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getMessage() {
    return this.message;
  }

  public MetadataResponseRecord getResponseRecord() {
    return responseRecord;
  }
}
