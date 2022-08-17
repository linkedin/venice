package com.linkedin.venice.fastclient.factory;

import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.DispatchingAvroGenericStoreClient;
import com.linkedin.venice.fastclient.DispatchingAvroSpecificStoreClient;
import com.linkedin.venice.fastclient.DualReadAvroGenericStoreClient;
import com.linkedin.venice.fastclient.DualReadAvroSpecificStoreClient;
import com.linkedin.venice.fastclient.RetriableAvroGenericStoreClient;
import com.linkedin.venice.fastclient.RetriableAvroSpecificStoreClient;
import com.linkedin.venice.fastclient.StatsAvroGenericStoreClient;
import com.linkedin.venice.fastclient.StatsAvroSpecificStoreClient;
import com.linkedin.venice.fastclient.meta.DaVinciClientBasedMetadata;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import org.apache.avro.specific.SpecificRecord;


/**
 * Every call in this factory will create its own {@link DaVinciClientBasedMetadata} and its internal
 * {@link CachingDaVinciClientFactory}. However, they will share the same {@link com.linkedin.davinci.DaVinciBackend}
 */
public class ClientFactory {

  // Use daVinci based store metadata by default
  public static <K, V> AvroGenericStoreClient<K, V> getAndStartGenericStoreClient(ClientConfig clientConfig) {
    return getAndStartGenericStoreClient(new DaVinciClientBasedMetadata(clientConfig), clientConfig);
  }

  // Use daVinci based store metadata default
  public static <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getAndStartSpecificStoreClient(
      ClientConfig clientConfig) {
    /**
     * TODO:
     * Need to construct {@link DaVinciClientBasedMetadata} inside store client, so that the store client could control
     * the lifecycle of the metadata instance.
     */
    return getAndStartSpecificStoreClient(new DaVinciClientBasedMetadata(clientConfig), clientConfig);
  }

  /**
   * TODO: once we decide to completely remove the helix based implementation, we won't need to pass the param: {@param storeMetadata}
   * in these factory methods.
   * So far, it is for the testing purpose.
   */
  public static <K, V> AvroGenericStoreClient<K, V> getAndStartGenericStoreClient(
      StoreMetadata storeMetadata,
      ClientConfig clientConfig) {
    final DispatchingAvroGenericStoreClient<K, V> dispatchingStoreClient =
        new DispatchingAvroGenericStoreClient<>(storeMetadata, clientConfig);
    StatsAvroGenericStoreClient<K, V> statsStoreClient;
    if (clientConfig.isLongTailRetryEnabledForSingleGet() || clientConfig.isLongTailRetryEnabledForBatchGet()) {
      statsStoreClient = new StatsAvroGenericStoreClient<>(
          new RetriableAvroGenericStoreClient<>(dispatchingStoreClient, clientConfig),
          clientConfig);
    } else {
      statsStoreClient = new StatsAvroGenericStoreClient<>(dispatchingStoreClient, clientConfig);
    }

    AvroGenericStoreClient<K, V> returningClient = statsStoreClient;
    if (clientConfig.isDualReadEnabled()) {
      returningClient = new DualReadAvroGenericStoreClient<>(statsStoreClient, clientConfig);
    }
    returningClient.start();
    return returningClient;
  }

  public static <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getAndStartSpecificStoreClient(
      StoreMetadata storeMetadata,
      ClientConfig clientConfig) {
    final DispatchingAvroSpecificStoreClient<K, V> dispatchingStoreClient =
        new DispatchingAvroSpecificStoreClient<>(storeMetadata, clientConfig);
    StatsAvroSpecificStoreClient<K, V> statsStoreClient;

    if (clientConfig.isLongTailRetryEnabledForSingleGet()) {
      statsStoreClient = new StatsAvroSpecificStoreClient<>(
          new RetriableAvroSpecificStoreClient<>(dispatchingStoreClient, clientConfig),
          clientConfig);
    } else {
      statsStoreClient = new StatsAvroSpecificStoreClient<>(dispatchingStoreClient, clientConfig);
    }

    AvroSpecificStoreClient<K, V> returningClient = statsStoreClient;
    if (clientConfig.isDualReadEnabled()) {
      returningClient = new DualReadAvroSpecificStoreClient<>(statsStoreClient, clientConfig);
    }
    returningClient.start();
    return returningClient;
  }
}
