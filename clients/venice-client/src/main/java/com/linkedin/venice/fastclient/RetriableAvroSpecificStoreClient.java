package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import org.apache.avro.specific.SpecificRecord;


public class RetriableAvroSpecificStoreClient<K, V extends SpecificRecord> extends RetriableAvroGenericStoreClient<K, V>
    implements AvroSpecificStoreClient<K, V> {
  public RetriableAvroSpecificStoreClient(InternalAvroStoreClient<K, V> delegate, ClientConfig clientConfig) {
    super(delegate, clientConfig);
  }
}
