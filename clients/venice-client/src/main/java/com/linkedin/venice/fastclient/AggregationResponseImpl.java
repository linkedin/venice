package com.linkedin.venice.fastclient;

import com.linkedin.venice.protocols.CountByValueResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import java.util.Collections;
import java.util.Map;


/**
 * Implementation of AggregationResponse backed by gRPC CountByValueResponse.
 */
public class AggregationResponseImpl implements AggregationResponse {
  private final CountByValueResponse grpcResponse;

  public AggregationResponseImpl(CountByValueResponse grpcResponse) {
    this.grpcResponse = grpcResponse;
  }

  @Override
  public Map<String, Long> getValueCounts() {
    if (hasError()) {
      return Collections.emptyMap();
    }
    return grpcResponse.getValueCountsMap();
  }

  @Override
  public int getKeysProcessed() {
    return (int) grpcResponse.getResponseRCU();
  }

  @Override
  public boolean hasError() {
    return grpcResponse.getErrorCode() != VeniceReadResponseStatus.OK;
  }

  @Override
  public String getErrorMessage() {
    return grpcResponse.hasErrorMessage() ? grpcResponse.getErrorMessage() : null;
  }
}
