package com.linkedin.venice.listener.response;

import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;


public class MultiGetResponseWrapper extends MultiKeyResponseWrapper<MultiGetResponseRecordV1> {
  public MultiGetResponseWrapper(int maxKeyCount) {
    super(maxKeyCount);
  }

  @Override
  protected byte[] serializedResponse() {
    RecordSerializer<MultiGetResponseRecordV1> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.getClassSchema());

    return serializer.serializeObjects(records, AvroSerializer.REUSE.get());
  }

  @Override
  public int getResponseSchemaIdHeader() {
    return ReadAvroProtocolDefinition.MULTI_GET_RESPONSE_V1.getProtocolVersion();
  }
}
