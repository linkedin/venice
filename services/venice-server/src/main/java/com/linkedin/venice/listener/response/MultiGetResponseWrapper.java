package com.linkedin.venice.listener.response;

import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;


public class MultiGetResponseWrapper extends MultiKeyResponseWrapper<MultiGetResponseRecordV1> {
  private static final RecordSerializer<MultiGetResponseRecordV1> SERIALIZER =
      FastSerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.getClassSchema());

  public MultiGetResponseWrapper(int maxKeyCount) {
    super(maxKeyCount);
  }

  @Override
  protected RecordSerializer<MultiGetResponseRecordV1> getResponseSerializer() {
    return SERIALIZER;
  }

  @Override
  public int getResponseSchemaIdHeader() {
    return ReadAvroProtocolDefinition.MULTI_GET_RESPONSE_V1.getProtocolVersion();
  }
}
