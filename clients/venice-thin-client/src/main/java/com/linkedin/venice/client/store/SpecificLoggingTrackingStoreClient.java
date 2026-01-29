package com.linkedin.venice.client.store;

import org.apache.avro.specific.SpecificRecord;


/**
 * This class is necessary because Venice needs to maintain a separate interface: {@link AvroSpecificStoreClient}.
 * @param <V>
 */
public class SpecificLoggingTrackingStoreClient<K, V extends SpecificRecord> extends LoggingTrackingStoreClient<K, V>
    implements AvroSpecificStoreClient<K, V> {
  public SpecificLoggingTrackingStoreClient(InternalAvroStoreClient<K, V> innerStoreClient) {
    super(innerStoreClient);
  }
}
