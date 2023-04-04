package com.linkedin.davinci.listener.response;

import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.metadata.response.VersionProperties;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.AvroSerializer;
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

  private final int hash;

  public MetadataResponse() {
    this.responseRecord = new MetadataResponseRecord();
    this.hash = hashCode(this.responseRecord); // 28629151
    this.isError = false;
  }

  public MetadataResponse(
      VersionProperties versionProperties,
      List<Integer> versions,
      Map<CharSequence, CharSequence> keySchema,
      Map<CharSequence, CharSequence> valueSchemas,
      int latestSuperSetValueSchemaId,
      Map<CharSequence, List<CharSequence>> routingInfo,
      Map<CharSequence, Integer> helixGroupInfo) {
    this.responseRecord = new MetadataResponseRecord(
        versionProperties,
        versions,
        keySchema,
        valueSchemas,
        latestSuperSetValueSchemaId,
        routingInfo,
        helixGroupInfo);
    this.hash = hashCode(this.responseRecord);
    this.isError = false;
  }

  public ByteBuf getResponseBody() {
    return Unpooled.wrappedBuffer(serializedResponse());
  }

  private byte[] serializedResponse() {
    RecordSerializer<MetadataResponseRecord> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(MetadataResponseRecord.SCHEMA$);

    return serializer.serialize(responseRecord, AvroSerializer.REUSE.get());
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

  public int getHash() {
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (obj.getClass() != this.getClass()) {
      return false;
    }

    final MetadataResponse other = (MetadataResponse) obj;
    return this.responseRecord.equals(other.responseRecord);
  }

  @Override
  public int hashCode() {
    return hashCode(responseRecord);
  }

  public static int hashCode(MetadataResponseRecord res) {
    int result = 1;
    result = 31 * result + (res.versionMetadata == null ? 0 : res.versionMetadata.hashCode());
    result = 31 * result + (res.versions == null ? 0 : res.versions.hashCode());
    result = 31 * result + (res.latestSuperSetValueSchemaId == null ? 0 : res.latestSuperSetValueSchemaId.hashCode());
    result = 31 * result + (res.routingInfo == null ? 0 : res.routingInfo.hashCode());
    result = 31 * result + (res.helixGroupInfo == null ? 0 : res.helixGroupInfo.hashCode());
    return result;
  }
}
