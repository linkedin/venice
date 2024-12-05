package com.linkedin.davinci.listener.response;

import com.linkedin.venice.metadata.response.StorePropertiesResponseRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Map;


/**
 * This class stores all the information required for answering a server metadata fetch request.
 */
public class StorePropertiesResponse {
  private boolean isError;
  private String message;

  private final StorePropertiesResponseRecord responseRecord;

  public StorePropertiesResponse() {
    this.responseRecord = new StorePropertiesResponseRecord();
  }

  public void setStoreMetaValue(StoreMetaValue storeMetaValue) {
    responseRecord.setStoreMetaValue(storeMetaValue);
  }

  public void setHelixGroupInfo(Map<CharSequence, Integer> helixGroupInfo) {
    responseRecord.setHelixGroupInfo(helixGroupInfo);
  }

  public void setRoutingInfo(Map<CharSequence, List<CharSequence>> routingInfo) {
    responseRecord.setRoutingInfo(routingInfo);
  }

  public ByteBuf getResponseBody() {
    return Unpooled.wrappedBuffer(serializedResponse());
  }

  private byte[] serializedResponse() {
    RecordSerializer<StorePropertiesResponseRecord> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(StorePropertiesResponseRecord.SCHEMA$);
    return serializer.serialize(responseRecord);
  }

  public int getResponseSchemaIdHeader() {
    return AvroProtocolDefinition.SERVER_STORE_PROPERTIES_RESPONSE.getCurrentProtocolVersion();
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

  public StorePropertiesResponseRecord getResponseRecord() {
    return responseRecord;
  }
}
