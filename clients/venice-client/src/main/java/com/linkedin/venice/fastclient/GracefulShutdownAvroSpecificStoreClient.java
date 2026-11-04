package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import org.apache.avro.specific.SpecificRecord;


public class GracefulShutdownAvroSpecificStoreClient<K, V extends SpecificRecord>
    extends GracefulShutdownAvroGenericStoreClient<K, V> implements AvroSpecificStoreClient<K, V> {
  public GracefulShutdownAvroSpecificStoreClient(
      InternalAvroStoreClient<K, V> delegate,
      ClientConfig config,
      ClientRequestTracker tracker) {
    super(delegate, config, tracker, true);
  }
}
