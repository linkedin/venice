package com.linkedin.davinci.listener.response;

import com.linkedin.venice.metadata.payload.StorePropertiesPayloadRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;


/**
 * This class stores all the information required for answering a server metadata fetch request.
 */
public class StorePropertiesPayload {
  private boolean isError;
  private String message;

  private final StorePropertiesPayloadRecord payloadRecord;

  public StorePropertiesPayload() {
    this.payloadRecord = new StorePropertiesPayloadRecord();
  }

  public void setStoreMetaValue(StoreMetaValue storeMetaValue) {

    // StoreMetaValueAvro
    RecordSerializer<StoreMetaValue> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(StoreMetaValue.SCHEMA$);
    byte[] serialized = serializer.serialize(storeMetaValue);
    payloadRecord.setStoreMetaValueAvro(ByteBuffer.wrap(serialized));

    // StoreMetaValueSchemaVersion
    payloadRecord.setStoreMetaValueSchemaVersion(
        AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion());
  }

  public void setHelixGroupInfo(Map<CharSequence, Integer> helixGroupInfo) {
    payloadRecord.setHelixGroupInfo(helixGroupInfo);
  }

  public void setRoutingInfo(Map<CharSequence, List<CharSequence>> routingInfo) {
    payloadRecord.setRoutingInfo(routingInfo);
  }

  public ByteBuf getResponseBody() {
    return Unpooled.wrappedBuffer(serializedResponse());
  }

  private byte[] serializedResponse() {
    RecordSerializer<StorePropertiesPayloadRecord> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(StorePropertiesPayloadRecord.SCHEMA$);
    return serializer.serialize(payloadRecord);
  }

  public int getResponseSchemaIdHeader() {
    return AvroProtocolDefinition.SERVER_STORE_PROPERTIES_PAYLOAD.getCurrentProtocolVersion();
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

  public StorePropertiesPayloadRecord getPayloadRecord() {
    return payloadRecord;
  }
}
