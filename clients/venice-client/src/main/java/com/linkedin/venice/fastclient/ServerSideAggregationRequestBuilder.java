package com.linkedin.venice.fastclient;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * Builder for server-side aggregation requests in venice-client.
 * This builder only supports server-side execution via gRPC.
 */
public interface ServerSideAggregationRequestBuilder<K> {
  /**
   * Count and group by field values, returning the top K most frequent values for each field.
   * 
   * @param fieldNames The list of fields to count values for
   * @param topK The maximum number of top values to return per field
   * @return This builder for chaining
   */
  ServerSideAggregationRequestBuilder<K> countByValue(List<String> fieldNames, int topK);

  /**
   * Execute the aggregation request on the server side using gRPC.
   * 
   * @param keys The set of keys to aggregate over
   * @return A future containing the aggregation response
   * @throws VeniceException if the request fails
   */
  CompletableFuture<AggregationResponse> execute(Set<K> keys) throws VeniceException;
}
