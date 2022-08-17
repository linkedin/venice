package com.linkedin.venice.client.factory;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import org.apache.avro.specific.SpecificRecord;


public interface VeniceStoreClientFactory {
  <K, V> AvroGenericStoreClient<K, V> getAndStartAvroGenericStoreClient(String storeName);

  /**
   * create a generic client along with customized client configs. This is only for sophisticated
   * use cases. Do not use it if you are not familiar with these configs.
   *
   * e.g.
   * client = veniceStoreClientFactory.getAndStartAvroGenericStoreClient(storeName, (config) -> config.setVsonClient(true));
   * @return
   */
  default <K, V> AvroGenericStoreClient<K, V> getAndStartAvroGenericStoreClient(
      String storeName,
      ClientConfigUpdater configUpdater) {
    throw new UnsupportedOperationException();
  }

  <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getAndStartAvroSpecificStoreClient(
      String storeName,
      Class<V> specificRecordClass);

  default <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getAndStartAvroSpecificStoreClient(
      String storeName,
      Class<V> specificRecordClass,
      ClientConfigUpdater configUpdater) {
    throw new UnsupportedOperationException();
  }

  void close();

  interface ClientConfigUpdater {
    ClientConfig update(ClientConfig clientConfig);
  }
}
