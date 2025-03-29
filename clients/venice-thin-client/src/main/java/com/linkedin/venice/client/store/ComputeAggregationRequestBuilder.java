package com.linkedin.venice.client.store;

import com.linkedin.venice.client.store.predicate.Predicate;
import java.util.Map;


public interface ComputeAggregationRequestBuilder<K> extends ExecutableRequestBuilder<K, ComputeAggregationResponse> {
  /**
   * Aggregation query where the content of specified fields are going to be grouped by their value, then the occurrence
   * of each distinct value counted, after which the top K highest counts along with their associated values are going
   * to be returned.
   *
   * @param topK The max number of distinct values for which to return a count.
   * @param fieldNames The names of fields for which to perform the facet counting.
   * @return The same builder instance, to chain additional operations onto.
   */
  ComputeAggregationRequestBuilder<K> countGroupByValue(int topK, String... fieldNames);

  /**
   * Aggregation query where the content of specified fields are going to be grouped by buckets, with each bucket being
   * defined by some {@link Predicate}, and each matching occurrence incrementing a count for its associated bucket,
   * after which the bucket names to counts are going to be returned.
   *
   * @param bucketNameToPredicate A map of predicate name -> {@link Predicate} to define the buckets to group values by.
   * @param fieldNames The names of fields for which to perform the facet counting.
   * @return The same builder instance, to chain additional operations onto.
   * @param <T> The type of the fields to apply the bucket predicates to.
   */
  <T> ComputeAggregationRequestBuilder<K> countGroupByBucket(
      Map<String, Predicate<T>> bucketNameToPredicate,
      String... fieldNames);
}
