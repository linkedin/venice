package com.linkedin.venice.listener.response;

import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.listener.response.stats.ComputeResponseStats;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;


public class ComputeResponseWrapper extends MultiKeyResponseWrapper<ComputeResponseRecordV1> {
  static final RecordSerializer<ComputeResponseRecordV1> SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(ComputeResponseRecordV1.getClassSchema());

  public ComputeResponseWrapper(int maxKeyCount) {
    this(maxKeyCount, new ComputeResponseStats());
  }

  public ComputeResponseWrapper(int maxKeyCount, ComputeResponseStats responseStats) {
    super(maxKeyCount, responseStats, SERIALIZER);
  }

  @Override
  public int getResponseSchemaIdHeader() {
    return ReadAvroProtocolDefinition.COMPUTE_RESPONSE_V1.getProtocolVersion();
  }
}
