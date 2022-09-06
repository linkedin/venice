package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import org.apache.avro.specific.SpecificRecord;


public class DualReadAvroSpecificStoreClient<K, V extends SpecificRecord> extends DualReadAvroGenericStoreClient<K, V>
    implements AvroSpecificStoreClient<K, V> {
  public DualReadAvroSpecificStoreClient(InternalAvroStoreClient<K, V> delegate, ClientConfig config) {
    super(delegate, config, config.getSpecificThinClient());
    if (config.getSpecificThinClient() == null) {
      throw new VeniceClientException(
          "SpecificThinClient in ClientConfig shouldn't be null when constructing a specific dual-read store client");
    }
  }
}
