package com.linkedin.davinci.client.factory;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;


/**
 * Factory interface for version-specific DaVinci Clients. This is only intended for internal Venice use.
 */
public interface VersionSpecificDaVinciClientFactory {
  <K, V> DaVinciClient<K, V> getVersionSpecificGenericAvroClient(
      String storeName,
      int storeVersion,
      DaVinciConfig config);

  <K, V> DaVinciClient<K, V> getAndStartVersionSpecificGenericAvroClient(
      String storeName,
      int storeVersion,
      DaVinciConfig config);

  <K, V> DaVinciClient<K, V> getVersionSpecificGenericAvroClient(
      String storeName,
      int storeVersion,
      String viewName,
      DaVinciConfig config);

  <K, V> DaVinciClient<K, V> getAndStartVersionSpecificGenericAvroClient(
      String storeName,
      int storeVersion,
      String viewName,
      DaVinciConfig config);
}
