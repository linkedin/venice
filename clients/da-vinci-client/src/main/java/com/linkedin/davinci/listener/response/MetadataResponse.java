package com.linkedin.davinci.listener.response;

import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.metadata.response.VersionProperties;
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
  private final MetadataResponseRecord responseRecord;

  public MetadataResponse() {
    this.responseRecord = new MetadataResponseRecord();
  }

  public void setVersionMetadata(VersionProperties versionProperties) {
    responseRecord.setVersionMetadata(versionProperties);
  }

  public void setKeySchema(String keySchema) {
    responseRecord.setKeySchema(keySchema);
  }

  public void setValueSchemas(List<CharSequence> valueSchemas) {
    responseRecord.setValueSchemas(valueSchemas);
  }

  public void setStoreConfig(Map<CharSequence, CharSequence> storeConfig) {
    responseRecord.setStoreConfig(storeConfig);
  }

  public void setRoutingInfo(Map<CharSequence, Map<CharSequence, List<CharSequence>>> routingInfo) {
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

    return serializer.serialize(responseRecord, AvroSerializer.REUSE.get());
  }

  public MetadataResponseRecord getResponseRecord() {
    return responseRecord;
  }
}
