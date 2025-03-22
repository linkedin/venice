package com.linkedin.venice.client.store;

import java.util.Map;


public interface ComputeAggregationResponse {
  /**
   * @param fieldName for which to get the value -> count map
   * @return the value counts as defined by {@link ComputeAggregationRequestBuilder#countGroupByValue(int, String...)}
   */
  <T> Map<T, Integer> getValueToCount(String fieldName);

  /**
   * @param fieldName for which to get the bucket -> count map
   * @return the bucket counts as defined by {@link ComputeAggregationRequestBuilder#countGroupByBucket(Map, String...)}
   */
  Map<String, Integer> getBucketNameToCount(String fieldName);
}
