package com.linkedin.venice.fastclient;

import com.linkedin.venice.protocols.CountByValueResponse;
import com.linkedin.venice.protocols.ValueCount;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import java.util.Collections;
import java.util.HashMap;
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
  @Deprecated
  public Map<String, Integer> getValueCounts() {
    if (hasError()) {
      return Collections.emptyMap();
    }
    // For backward compatibility, return the first field's counts
    Map<String, Map<String, Integer>> fieldCounts = getFieldToValueCounts();
    if (fieldCounts.isEmpty()) {
      return Collections.emptyMap();
    }
    return fieldCounts.values().iterator().next();
  }

  @Override
  public Map<String, Map<String, Integer>> getFieldToValueCounts() {
    if (hasError()) {
      return Collections.emptyMap();
    }
    Map<String, Map<String, Integer>> result = new HashMap<>();
    for (Map.Entry<String, ValueCount> entry: grpcResponse.getFieldToValueCountsMap().entrySet()) {
      result.put(entry.getKey(), entry.getValue().getValueToCountsMap());
    }
    return result;
  }

  @Override
  public int getKeysProcessed() {
    // Count total values processed across all fields
    return getFieldToValueCounts().values()
        .stream()
        .mapToInt(fieldCounts -> fieldCounts.values().stream().mapToInt(Integer::intValue).sum())
        .sum();
  }

  @Override
  public boolean hasError() {
    return grpcResponse.getErrorCode() != VeniceReadResponseStatus.OK;
  }

  @Override
  public String getErrorMessage() {
    String errorMessage = grpcResponse.getErrorMessage();
    return errorMessage.isEmpty() ? null : errorMessage;
  }
}
