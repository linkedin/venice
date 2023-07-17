package com.linkedin.venice.listener.response;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;


public class ComputeResponseWrapper extends MultiKeyResponseWrapper<ComputeResponseRecordV1> {
  private static final RecordSerializer<ComputeResponseRecordV1> SERIALIZER =
      FastSerializerDeserializerFactory.getAvroGenericSerializer(ComputeResponseRecordV1.getClassSchema());

  public ComputeResponseWrapper(int maxKeyCount) {
    super(maxKeyCount);
    // The following metrics will get incremented for each record processed in computeResult()
    setReadComputeDeserializationLatency(0.0);
    setDatabaseLookupLatency(0.0);
    setReadComputeSerializationLatency(0.0);
    setReadComputeLatency(0.0);

    // Compute responses are never compressed
    setCompressionStrategy(CompressionStrategy.NO_OP);
  }

  @Override
  protected RecordSerializer<ComputeResponseRecordV1> getResponseSerializer() {
    return SERIALIZER;
  }

  @Override
  public int getResponseSchemaIdHeader() {
    return ReadAvroProtocolDefinition.COMPUTE_RESPONSE_V1.getProtocolVersion();
  }
}
