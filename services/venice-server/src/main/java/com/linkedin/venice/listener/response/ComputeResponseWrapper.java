package com.linkedin.venice.listener.response;

import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;


public class ComputeResponseWrapper extends MultiKeyResponseWrapper<ComputeResponseRecordV1> {
  public ComputeResponseWrapper(int maxKeyCount) {
    super(maxKeyCount);
  }

  @Override
  protected byte[] serializedResponse() {
    RecordSerializer<ComputeResponseRecordV1> serializer =
        FastSerializerDeserializerFactory.getAvroGenericSerializer(ComputeResponseRecordV1.getClassSchema());

    return serializer.serializeObjects(records);
  }

  @Override
  public int getResponseSchemaIdHeader() {
    return ReadAvroProtocolDefinition.COMPUTE_RESPONSE_V1.getProtocolVersion();
  }
}
