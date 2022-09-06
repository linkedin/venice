package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import org.apache.avro.specific.SpecificRecord;


public class StatsAvroSpecificStoreClient<K, V extends SpecificRecord> extends StatsAvroGenericStoreClient<K, V>
    implements AvroSpecificStoreClient<K, V> {
  public StatsAvroSpecificStoreClient(InternalAvroStoreClient<K, V> delegate, ClientConfig clientConfig) {
    super(delegate, clientConfig);
  }
}
