package com.linkedin.venice.listener.response;

import com.linkedin.venice.compute.protocol.response.ComputeAggregationResponse;
import com.linkedin.venice.listener.response.stats.ComputeAggregationResponseStats;
import com.linkedin.venice.listener.response.stats.ReadResponseStatsRecorder;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;


/**
 * Response wrapper for compute aggregation operations.
 */
public class ComputeAggregationResponseWrapper extends AbstractReadResponse {
  static final RecordSerializer<ComputeAggregationResponse> SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(ComputeAggregationResponse.getClassSchema());

  private final ComputeAggregationResponse aggregationResponse;
  private final ComputeAggregationResponseStats stats;

  public ComputeAggregationResponseWrapper(ComputeAggregationResponseStats responseStats) {
    this.stats = responseStats;
    this.aggregationResponse = new ComputeAggregationResponse();
  }

  public ComputeAggregationResponse getAggregationResponse() {
    return aggregationResponse;
  }

  @Override
  public ComputeAggregationResponseStats getStats() {
    return stats;
  }

  @Override
  public int getResponseSchemaIdHeader() {
    // TODO: Add proper protocol version when available
    return 1;
  }

  @Override
  public ByteBuf getResponseBody() {
    return Unpooled.wrappedBuffer(SERIALIZER.serialize(aggregationResponse));
  }

  @Override
  public ReadResponseStatsRecorder getStatsRecorder() {
    return stats;
  }
}
