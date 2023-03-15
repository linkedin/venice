package com.linkedin.venice.fastclient.factory;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.DispatchingAvroGenericStoreClient;
import com.linkedin.venice.fastclient.DispatchingAvroSpecificStoreClient;
import com.linkedin.venice.fastclient.DispatchingVsonStoreClient;
import com.linkedin.venice.fastclient.DualReadAvroGenericStoreClient;
import com.linkedin.venice.fastclient.DualReadAvroSpecificStoreClient;
import com.linkedin.venice.fastclient.RetriableAvroGenericStoreClient;
import com.linkedin.venice.fastclient.RetriableAvroSpecificStoreClient;
import com.linkedin.venice.fastclient.StatsAvroGenericStoreClient;
import com.linkedin.venice.fastclient.StatsAvroSpecificStoreClient;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.fastclient.meta.ThinClientBasedMetadata;
import org.apache.avro.specific.SpecificRecord;


/**
 * Every call in this factory will create its own {@link ThinClientBasedMetadata}. However, they will share the same
 * thin-client that's being passed in as a config.
 */
public class ClientFactory {

  // Use Venice thin client based store metadata by default
  public static <K, V> AvroGenericStoreClient<K, V> getAndStartGenericStoreClient(ClientConfig clientConfig) {
    return getAndStartGenericStoreClient(
        new ThinClientBasedMetadata(clientConfig, clientConfig.getThinClientForMetaStore()),
        clientConfig);
  }

  // Use Venice thin client based store metadata default
  public static <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getAndStartSpecificStoreClient(
      ClientConfig clientConfig) {
    /**
     * TODO:
     * Need to construct {@link DaVinciClientBasedMetadata} inside store client, so that the store client could control
     * the lifecycle of the metadata instance.
     */
    return getAndStartSpecificStoreClient(
        new ThinClientBasedMetadata(clientConfig, clientConfig.getThinClientForMetaStore()),
        clientConfig);
  }

  /**
   * TODO: once we decide to completely remove the helix based implementation, we won't need to pass
   *  the param: {@param storeMetadata} in these factory methods.
   *  So far, it is for the testing purpose.
   */
  public static <K, V> AvroGenericStoreClient<K, V> getAndStartGenericStoreClient(
      StoreMetadata storeMetadata,
      ClientConfig clientConfig) {
    final DispatchingAvroGenericStoreClient<K, V> dispatchingStoreClient = clientConfig.isVsonStore()
        ? new DispatchingVsonStoreClient<>(storeMetadata, clientConfig)
        : new DispatchingAvroGenericStoreClient<>(storeMetadata, clientConfig);
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
