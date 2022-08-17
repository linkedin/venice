package com.linkedin.venice.client.store;

import org.apache.avro.specific.SpecificRecord;


/**
 * This class is necessary because Venice needs to maintain a separate interface: {@link AvroSpecificStoreClient}.
 * @param <V>
 */
public class SpecificRetriableStoreClient<K, V extends SpecificRecord> extends RetriableStoreClient<K, V>
    implements AvroSpecificStoreClient<K, V> {
  public SpecificRetriableStoreClient(
      SpecificStatTrackingStoreClient<K, V> innerStoreClient,
      ClientConfig clientConfig) {
    super(innerStoreClient, clientConfig);
  }
}
