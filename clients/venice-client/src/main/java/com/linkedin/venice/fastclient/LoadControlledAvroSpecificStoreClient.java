package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import org.apache.avro.specific.SpecificRecord;


public class LoadControlledAvroSpecificStoreClient<K, V extends SpecificRecord>
    extends LoadControlledAvroGenericStoreClient<K, V> implements AvroSpecificStoreClient<K, V> {
  public LoadControlledAvroSpecificStoreClient(InternalAvroStoreClient<K, V> delegate, ClientConfig clientConfig) {
    super(delegate, clientConfig);
  }
}
