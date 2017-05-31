package com.linkedin.venice.client.store;

import io.tehuti.metrics.MetricsRepository;
import org.apache.avro.specific.SpecificRecord;

/**
 * This class is necessary because Venice needs to maintain a separate interface: {@link AvroSpecificStoreClient}.
 * @param <V>
 */
public class SpecificStatTrackingStoreClient<K, V extends SpecificRecord>
    extends StatTrackingStoreClient<K, V> implements AvroSpecificStoreClient<K, V> {
  public SpecificStatTrackingStoreClient(InternalAvroStoreClient<K, V> innerStoreClient) {
    super(innerStoreClient);
  }

  public SpecificStatTrackingStoreClient(InternalAvroStoreClient<K, V> innerStoreClient, MetricsRepository metricsRepository) {
    super(innerStoreClient, metricsRepository);
  }
}
