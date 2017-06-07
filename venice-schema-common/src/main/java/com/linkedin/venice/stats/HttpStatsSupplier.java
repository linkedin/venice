package com.linkedin.venice.stats;

import com.linkedin.venice.read.RequestType;
import io.tehuti.metrics.MetricsRepository;


/**
 * This interface defines the function to retrieve {@link AbstractVeniceStats} based on store name and {@link RequestType}.
 * And it is mostly being used in http related metrics tracking.
 *
 * @param <T>
 */
public interface HttpStatsSupplier<T extends AbstractVeniceStats> {
  T get(MetricsRepository metricsRepository, String storeName, RequestType requestType);
}
