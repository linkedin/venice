package com.linkedin.venice.store;

import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.AvroGenericStoreClientImpl;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.helix.StoreJSONSerializer;
import com.linkedin.venice.helix.SystemStoreJSONSerializer;
import com.linkedin.venice.meta.SerializableSystemStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SystemStore;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


/**
 * This class retrieves the corresponding Store object from a router endpoint which contains the latest store state.
 * E.g. store level configs and versions. This class doesn't maintain any local cache of the retrieved store state.
 */
public class StoreStateReader implements Closeable {
  private static final StoreJSONSerializer STORE_SERIALIZER = new StoreJSONSerializer();
  private static final SystemStoreJSONSerializer SYSTEM_STORE_SERIALIZER = new SystemStoreJSONSerializer();

  private final String storeName;
  private final String requestPath;
  private final String exceptionMessageFooter;
  private final VeniceSystemStoreType veniceSystemStoreType;
  private final AbstractAvroStoreClient storeClient;
  private final boolean externalClient;

  private StoreStateReader(AbstractAvroStoreClient client, boolean externalClient) {
    this.storeClient = client;
    this.externalClient = externalClient;
    this.storeName = client.getStoreName();
    requestPath = VeniceConstants.TYPE_STORE_STATE + "/" + storeName;
    exceptionMessageFooter = "while trying to fetch store: " + storeName + " with path: " + requestPath;
    veniceSystemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
  }

  public static StoreStateReader getInstance(ClientConfig clientConfig) {
    AvroGenericStoreClientImpl client =
        new AvroGenericStoreClientImpl<>(ClientFactory.getTransportClient(clientConfig), false, clientConfig);
    client.start();
    return new StoreStateReader(client, false);
  }

  public static StoreStateReader getInstance(AbstractAvroStoreClient storeClient) {
    return new StoreStateReader(storeClient, true);
  }

  public Store getStore() {
    try {
      byte[] response = RetryUtils.executeWithMaxAttempt(
          () -> ((CompletableFuture<byte[]>) storeClient.getRaw(requestPath)).get(),
          3,
          Duration.ofMillis(100),
          Arrays.asList(ExecutionException.class));
      if (response == null) {
        throw new VeniceClientException("Unexpected null response " + exceptionMessageFooter);
      }
      if (veniceSystemStoreType != null && veniceSystemStoreType.isNewMedataRepositoryAdopted()) {
        SerializableSystemStore serializableSystemStore = SYSTEM_STORE_SERIALIZER.deserialize(response, null);
        return new SystemStore(
            serializableSystemStore.getZkSharedStore(),
            serializableSystemStore.getSystemStoreType(),
            serializableSystemStore.getVeniceStore());
      } else {
        return STORE_SERIALIZER.deserialize(response, null);
      }
    } catch (Exception e) {
      throw new VeniceClientException("Unexpected exception " + exceptionMessageFooter, e);
    }
  }

  @Override
  public void close() {
    if (!externalClient) {
      Utils.closeQuietlyWithErrorLogged(storeClient);
    }
  }
}
