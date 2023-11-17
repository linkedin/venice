package com.linkedin.davinci.client.factory;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.store.CustomStorageEngine;
import org.apache.avro.specific.SpecificRecord;


public interface DaVinciClientFactory {
  <K, V> DaVinciClient<K, V> getGenericAvroClient(String storeName, DaVinciConfig config);

  <K, V> DaVinciClient<K, V> getAndStartGenericAvroClient(String storeName, DaVinciConfig config);

  <K, V extends SpecificRecord> DaVinciClient<K, V> getSpecificAvroClient(
      String storeName,
      DaVinciConfig config,
      Class<V> valueClass);

  <K, V extends SpecificRecord> DaVinciClient<K, V> getAndStartSpecificAvroClient(
      String storeName,
      DaVinciConfig config,
      Class<V> valueClass);

  <K, V> DaVinciClient<K, V> getGenericAvroClientWithCustomStorageEngine(
      String storeName,
      DaVinciConfig config,
      CustomStorageEngine customStorageEngine);

  <K, V> DaVinciClient<K, V> getAndStartGenericAvroClientWithCustomStorageEngine(
      String storeName,
      DaVinciConfig config,
      CustomStorageEngine customStorageEngine);

  <K, V extends SpecificRecord> DaVinciClient<K, V> getSpecificAvroClientWithCustomStorageEngine(
      String storeName,
      DaVinciConfig config,
      Class<V> valueClass,
      CustomStorageEngine customStorageEngine);

  <K, V extends SpecificRecord> DaVinciClient<K, V> getAndStartSpecificAvroClientWithCustomStorageEngine(
      String storeName,
      DaVinciConfig config,
      Class<V> valueClass,
      CustomStorageEngine customStorageEngine);
}
