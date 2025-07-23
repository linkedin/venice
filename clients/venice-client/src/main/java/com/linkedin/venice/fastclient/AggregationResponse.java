package com.linkedin.venice.fastclient;

import java.util.Map;


/**
 * Response interface for aggregation operations in venice-client.
 */
public interface AggregationResponse {
  /**
   * Get the aggregated value counts for countByValue operations.
   * @return A map where keys are field values and values are counts
   */
  Map<String, Long> getValueCounts();

  /**
   * Get the total number of keys processed.
   * @return The number of keys processed
   */
  int getKeysProcessed();

  /**
   * Check if there was an error during aggregation.
   * @return true if there was an error, false otherwise
   */
  boolean hasError();

  /**
   * Get the error message if there was an error.
   * @return The error message, or null if no error
   */
  String getErrorMessage();
}
