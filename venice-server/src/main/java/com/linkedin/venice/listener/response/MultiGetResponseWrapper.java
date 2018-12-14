package com.linkedin.venice.listener.response;

import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;

public class MultiGetResponseWrapper extends MultiKeyResponseWrapper<MultiGetResponseRecordV1> {
  @Override
  protected byte[] serializedResponse() {
    RecordSerializer<MultiGetResponseRecordV1> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);

    return serializer.serializeObjects(records);
  }

  @Override
  public int getResponseSchemaIdHeader() {
    return ReadAvroProtocolDefinition.MULTI_GET_RESPONSE_V1.getProtocolVersion();
  }
}
