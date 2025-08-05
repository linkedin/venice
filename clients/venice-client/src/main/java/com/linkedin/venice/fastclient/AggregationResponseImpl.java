package com.linkedin.venice.fastclient;

import com.linkedin.venice.protocols.BucketCount;
import com.linkedin.venice.protocols.CountByBucketResponse;
import com.linkedin.venice.protocols.CountByValueResponse;
import com.linkedin.venice.protocols.ValueCount;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Implementation of AggregationResponse backed by gRPC responses.
 * Supports both CountByValueResponse and CountByBucketResponse.
 */
public class AggregationResponseImpl implements AggregationResponse {
  private final CountByValueResponse countByValueResponse;
  private final CountByBucketResponse countByBucketResponse;

  public AggregationResponseImpl(CountByValueResponse grpcResponse) {
    this.countByValueResponse = grpcResponse;
    this.countByBucketResponse = null;
  }

  public AggregationResponseImpl(CountByBucketResponse grpcResponse) {
    this.countByValueResponse = null;
    this.countByBucketResponse = grpcResponse;
  }

  @Override
  public Map<String, Map<String, Integer>> getFieldToValueCounts() {
    if (countByValueResponse == null || hasError()) {
      return Collections.emptyMap();
    }
    Map<String, Map<String, Integer>> result = new HashMap<>();
    for (Map.Entry<String, ValueCount> entry: countByValueResponse.getFieldToValueCountsMap().entrySet()) {
      result.put(entry.getKey(), entry.getValue().getValueToCountsMap());
    }
    return result;
  }

  @Override
  public Map<String, Map<String, Integer>> getFieldToBucketCounts() {
    if (countByBucketResponse == null || hasError()) {
      return Collections.emptyMap();
    }
    Map<String, Map<String, Integer>> result = new HashMap<>();
    for (Map.Entry<String, BucketCount> entry: countByBucketResponse.getFieldToBucketCountsMap().entrySet()) {
      result.put(entry.getKey(), entry.getValue().getBucketToCountsMap());
    }
    return result;
  }

  @Override
  public int getKeysProcessed() {
    if (countByValueResponse != null && !hasError()) {
      // Count total values processed across all fields for countByValue
      return getFieldToValueCounts().values()
          .stream()
          .mapToInt(fieldCounts -> fieldCounts.values().stream().mapToInt(Integer::intValue).sum())
          .sum();
    } else if (countByBucketResponse != null && !hasError()) {
      // Count total values processed across all fields for countByBucket
      return getFieldToBucketCounts().values()
          .stream()
          .mapToInt(fieldCounts -> fieldCounts.values().stream().mapToInt(Integer::intValue).sum())
          .sum();
    }
    return 0;
  }

  @Override
  public boolean hasError() {
    if (countByValueResponse != null) {
      return countByValueResponse.getErrorCode() != VeniceReadResponseStatus.OK;
    } else if (countByBucketResponse != null) {
      return countByBucketResponse.getErrorCode() != VeniceReadResponseStatus.OK;
    }
    return true; // No valid response
  }

  @Override
  public String getErrorMessage() {
    String errorMessage = null;
    if (countByValueResponse != null) {
      errorMessage = countByValueResponse.getErrorMessage();
    } else if (countByBucketResponse != null) {
      errorMessage = countByBucketResponse.getErrorMessage();
    }
    return (errorMessage == null || errorMessage.isEmpty()) ? null : errorMessage;
  }
}
