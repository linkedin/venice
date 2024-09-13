package com.linkedin.venice.fastclient.factory;

import static com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode.SERVER_BASED_METADATA;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
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
import com.linkedin.venice.fastclient.meta.RequestBasedMetadata;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import java.util.Objects;
import org.apache.avro.specific.SpecificRecord;


public class ClientFactory {
  public static <K, V> AvroGenericStoreClient<K, V> getAndStartGenericStoreClient(ClientConfig clientConfig) {
    StoreMetadata storeMetadata = constructStoreMetadataReader(clientConfig);

    return getAndStartGenericStoreClient(storeMetadata, clientConfig);
  }

  public static <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getAndStartSpecificStoreClient(
      ClientConfig clientConfig) {
    /**
     * TODO:
     * Need to construct {@link StoreMetadata} inside store client, so that the store client could control
     * the lifecycle of the metadata instance.
     */
    StoreMetadata storeMetadata = constructStoreMetadataReader(clientConfig);

    return getAndStartSpecificStoreClient(storeMetadata, clientConfig);
  }

  private static StoreMetadata constructStoreMetadataReader(ClientConfig clientConfig) {
    if (clientConfig.getStoreMetadataFetchMode() == SERVER_BASED_METADATA) {
      Objects.requireNonNull(clientConfig.getClusterDiscoveryD2Service());
      Objects.requireNonNull(clientConfig.getD2Client());
      return new RequestBasedMetadata(
          clientConfig,
          new D2TransportClient(clientConfig.getClusterDiscoveryD2Service(), clientConfig.getD2Client()));
    }
    throw new VeniceUnsupportedOperationException("Store metadata with " + clientConfig.getStoreMetadataFetchMode());
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
    if (clientConfig.isLongTailRetryEnabledForSingleGet() || clientConfig.isLongTailRetryEnabledForBatchGet()
        || clientConfig.isLongTailRetryEnabledForCompute()) {
      statsStoreClient = new StatsAvroGenericStoreClient<>(
          /**
           * Reuse the {@link TimeoutProcessor} from {@link InstanceHealthMonitor} to
           * reduce the thread usage.
           */
          new RetriableAvroGenericStoreClient<>(
              dispatchingStoreClient,
              clientConfig,
              storeMetadata.getInstanceHealthMonitor().getTimeoutProcessor()),
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

    if (clientConfig.isLongTailRetryEnabledForSingleGet() || clientConfig.isLongTailRetryEnabledForBatchGet()
        || clientConfig.isLongTailRetryEnabledForCompute()) {
      statsStoreClient = new StatsAvroSpecificStoreClient<>(
          new RetriableAvroSpecificStoreClient<>(
              dispatchingStoreClient,
              clientConfig,
              storeMetadata.getInstanceHealthMonitor().getTimeoutProcessor()),
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
