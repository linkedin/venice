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
import com.linkedin.venice.fastclient.InternalAvroStoreClient;
import com.linkedin.venice.fastclient.LoadControlledAvroGenericStoreClient;
import com.linkedin.venice.fastclient.LoadControlledAvroSpecificStoreClient;
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

  /**
   * Builds the Fast Client wrapper chain without calling {@link AvroGenericStoreClient#start()}. Callers that need
   * to register a {@code StoreVersionSwitchListener} or {@code StoreConfigChangeListener} before the first metadata
   * refresh fires should use this entry point, register on the returned client, then call {@code start()}.
   *
   * <p>{@code register*Listener} only delivers transitions observed after the listener exists in the registry — a
   * listener registered after {@code start()} returns will not see the initial {@code (-1 -> currentVersion)}
   * transition committed by the first refresh. This factory plus {@code start()} from the caller is the supported
   * way to observe that initial transition.
   */
  public static <K, V> AvroGenericStoreClient<K, V> getGenericStoreClient(ClientConfig clientConfig) {
    StoreMetadata storeMetadata = constructStoreMetadataReader(clientConfig);
    return buildGenericStoreClient(storeMetadata, clientConfig);
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
    AvroGenericStoreClient<K, V> client = buildGenericStoreClient(storeMetadata, clientConfig);
    client.start();
    return client;
  }

  /**
   * Builds the generic Fast Client wrapper chain over an externally-supplied {@link StoreMetadata} without calling
   * {@link AvroGenericStoreClient#start()}. See {@link #getGenericStoreClient(ClientConfig)} for the rationale.
   */
  private static <K, V> AvroGenericStoreClient<K, V> buildGenericStoreClient(
      StoreMetadata storeMetadata,
      ClientConfig clientConfig) {
    final DispatchingAvroGenericStoreClient<K, V> dispatchingStoreClient = clientConfig.isVsonStore()
        ? new DispatchingVsonStoreClient<>(storeMetadata, clientConfig)
        : new DispatchingAvroGenericStoreClient<>(storeMetadata, clientConfig);

    InternalAvroStoreClient<K, V> retryClient = dispatchingStoreClient;
    if (clientConfig.isLongTailRetryEnabledForSingleGet() || clientConfig.isLongTailRetryEnabledForBatchGet()
        || clientConfig.isLongTailRetryEnabledForCompute()) {
      retryClient = new RetriableAvroGenericStoreClient<>(
          dispatchingStoreClient,
          clientConfig,
          /**
           * Reuse the {@link TimeoutProcessor} from {@link InstanceHealthMonitor} to
           * reduce the thread usage.
           */
          storeMetadata.getInstanceHealthMonitor().getTimeoutProcessor());
    }

    InternalAvroStoreClient<K, V> loadControlClient = retryClient;
    if (clientConfig.isStoreLoadControllerEnabled()) {
      loadControlClient = new LoadControlledAvroGenericStoreClient<>(retryClient, clientConfig);
    }

    StatsAvroGenericStoreClient<K, V> statsStoreClient =
        new StatsAvroGenericStoreClient<>(loadControlClient, clientConfig);

    AvroGenericStoreClient<K, V> dualReadClient = statsStoreClient;
    if (clientConfig.isDualReadEnabled()) {
      dualReadClient = new DualReadAvroGenericStoreClient<>(statsStoreClient, clientConfig);
    }
    return dualReadClient;
  }

  public static <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getAndStartSpecificStoreClient(
      StoreMetadata storeMetadata,
      ClientConfig clientConfig) {
    final DispatchingAvroSpecificStoreClient<K, V> dispatchingStoreClient =
        new DispatchingAvroSpecificStoreClient<>(storeMetadata, clientConfig);

    InternalAvroStoreClient<K, V> retryClient = dispatchingStoreClient;
    if (clientConfig.isLongTailRetryEnabledForSingleGet() || clientConfig.isLongTailRetryEnabledForBatchGet()
        || clientConfig.isLongTailRetryEnabledForCompute()) {
      retryClient = new RetriableAvroSpecificStoreClient<>(
          dispatchingStoreClient,
          clientConfig,
          storeMetadata.getInstanceHealthMonitor().getTimeoutProcessor());
    }

    InternalAvroStoreClient<K, V> loadControlClient = retryClient;
    if (clientConfig.isStoreLoadControllerEnabled()) {
      loadControlClient = new LoadControlledAvroSpecificStoreClient<>(retryClient, clientConfig);
    }

    StatsAvroSpecificStoreClient<K, V> statsStoreClient =
        new StatsAvroSpecificStoreClient<>(loadControlClient, clientConfig);
    AvroSpecificStoreClient<K, V> dualReadClient = statsStoreClient;
    if (clientConfig.isDualReadEnabled()) {
      dualReadClient = new DualReadAvroSpecificStoreClient<>(statsStoreClient, clientConfig);
    }
    dualReadClient.start();

    return dualReadClient;
  }
}
