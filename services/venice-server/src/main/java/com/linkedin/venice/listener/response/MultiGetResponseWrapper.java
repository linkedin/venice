package com.linkedin.venice.listener.response;

import com.linkedin.venice.listener.response.stats.MultiKeyResponseStats;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;


public class MultiGetResponseWrapper extends MultiKeyResponseWrapper<MultiGetResponseRecordV1> {
  static final RecordSerializer<MultiGetResponseRecordV1> SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(MultiGetResponseRecordV1.getClassSchema());

  public MultiGetResponseWrapper(int maxKeyCount) {
    this(maxKeyCount, new MultiKeyResponseStats());
  }

  public MultiGetResponseWrapper(int maxKeyCount, MultiKeyResponseStats responseStats) {
    super(maxKeyCount, responseStats, SERIALIZER);
  }

  @Override
  public int getResponseSchemaIdHeader() {
    return ReadAvroProtocolDefinition.MULTI_GET_RESPONSE_V1.getProtocolVersion();
  }
}
