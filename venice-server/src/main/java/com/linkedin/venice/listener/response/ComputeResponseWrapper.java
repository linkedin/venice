package com.linkedin.venice.listener.response;

import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;

public class ComputeResponseWrapper extends MultiKeyResponseWrapper<ComputeResponseRecordV1> {
  @Override
  protected byte[] serializedResponse() {
    RecordSerializer<ComputeResponseRecordV1> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(ComputeResponseRecordV1.SCHEMA$);

    return serializer.serializeObjects(records, AvroSerializer.REUSE.get());
  }

  @Override
  public int getResponseSchemaIdHeader() {
    return ReadAvroProtocolDefinition.COMPUTE_RESPONSE_V1.getProtocolVersion();
  }
}
