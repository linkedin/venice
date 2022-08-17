package com.linkedin.venice.client.store;

import org.apache.avro.specific.SpecificRecord;


/**
 * This class is necessary because Venice needs to maintain a separate interface: {@link AvroSpecificStoreClient}.
 * @param <V>
 */
public class SpecificStatTrackingStoreClient<K, V extends SpecificRecord> extends StatTrackingStoreClient<K, V>
    implements AvroSpecificStoreClient<K, V> {
  public SpecificStatTrackingStoreClient(InternalAvroStoreClient<K, V> innerStoreClient, ClientConfig clientConfig) {
    super(innerStoreClient, clientConfig);
  }
}
