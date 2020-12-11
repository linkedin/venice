package com.linkedin.venice.fastclient.factory;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.DispatchingAvroGenericStoreClient;
import com.linkedin.venice.fastclient.DispatchingAvroSpecificStoreClient;
import com.linkedin.venice.fastclient.DualReadAvroGenericStoreClient;
import com.linkedin.venice.fastclient.DualReadAvroSpecificStoreClient;
import com.linkedin.venice.fastclient.StatsAvroGenericStoreClient;
import com.linkedin.venice.fastclient.StatsAvroSpecificStoreClient;
import com.linkedin.venice.fastclient.meta.HelixBasedStoreMetadata;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import org.apache.avro.specific.SpecificRecord;


public class ClientFactory {

  // Use Helix based store metadata for integration/perf test for now
  public static <K, V> AvroGenericStoreClient<K, V> getGenericStoreClient(ClientConfig clientConfig) {
    return getGenericStoreClient(new HelixBasedStoreMetadata(clientConfig), clientConfig);
  }

  // Use Helix based store metadata for integration/perf test for now
  public static <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getSpecificStoreClient(ClientConfig clientConfig) {
    return getSpecificStoreClient(new HelixBasedStoreMetadata(clientConfig), clientConfig);
  }

  /**
   * TODO: once the implementation of {@link StoreMetadata} is ready, we won't need to pass the param: {@param storeMetadata}
   * in these factory methods.
   * So far, it is for the testing purpose.
   */
  public static <K, V> AvroGenericStoreClient<K, V> getGenericStoreClient(StoreMetadata storeMetadata, ClientConfig clientConfig) {
    DispatchingAvroGenericStoreClient<K, V> dispatchingStoreClient = new DispatchingAvroGenericStoreClient<>(storeMetadata, clientConfig);
    StatsAvroGenericStoreClient<K, V> statsStoreClient = new StatsAvroGenericStoreClient<>(dispatchingStoreClient, clientConfig);
    if (clientConfig.isDualReadEnabled()) {
      return new DualReadAvroGenericStoreClient<>(statsStoreClient, clientConfig);
    }
    return statsStoreClient;
  }

  public static <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getSpecificStoreClient(StoreMetadata storeMetadata,  ClientConfig clientConfig) {
    DispatchingAvroSpecificStoreClient<K, V> dispatchingStoreClient = new DispatchingAvroSpecificStoreClient<>(storeMetadata, clientConfig);
    StatsAvroSpecificStoreClient<K, V>
        statsStoreClient = new StatsAvroSpecificStoreClient<>(dispatchingStoreClient, clientConfig);
    if (clientConfig.isDualReadEnabled()) {
      return new DualReadAvroSpecificStoreClient<>(statsStoreClient, clientConfig);
    }
    return statsStoreClient;
  }
}
