package com.linkedin.davinci.client;

import com.linkedin.venice.client.store.ClientConfig;
import org.apache.avro.specific.SpecificRecord;


public class StatsAvroSpecificDaVinciClient<K, V extends SpecificRecord> extends StatsAvroGenericDaVinciClient<K, V> {
  public StatsAvroSpecificDaVinciClient(AvroSpecificDaVinciClient<K, V> delegate, ClientConfig clientConfig) {
    super(delegate, clientConfig);
  }
}
