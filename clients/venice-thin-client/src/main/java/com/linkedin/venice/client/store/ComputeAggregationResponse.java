package com.linkedin.venice.client.store;

import java.util.Map;


public interface ComputeAggregationResponse {
  /**
   * @return all requested value counts as defined by {@link ComputeAggregationRequestBuilder#countGroupByValue(int, String...)}
   */
  Map<String, Map<Object, Integer>> getFieldNameToValueToCount();

  /**
   * Equivalent to calling {@code (Map<T, Integer>) getFieldNameToValueToCount().get(fieldName)}
   *
   * @param fieldName for which to get the value -> count map
   * @return the value counts as defined by {@link ComputeAggregationRequestBuilder#countGroupByValue(int, String...)}
   */
  <T> Map<T, Integer> getValueToCount(String fieldName);

  /**
   * @return all requested bucket counts as defined by {@link ComputeAggregationRequestBuilder#countGroupByBucket(Map, String...)}
   */
  Map<String, Map<String, Integer>> getFieldNameToBucketNameToCount();

  /**
   * Equivalent to calling {@code getFieldNameToBucketNameToCount().get(fieldName)}
   *
   * @param fieldName for which to get the bucket -> count map
   * @return the bucket counts as defined by {@link ComputeAggregationRequestBuilder#countGroupByBucket(Map, String...)}
   */
  Map<String, Integer> getBucketNameToCount(String fieldName);
}
