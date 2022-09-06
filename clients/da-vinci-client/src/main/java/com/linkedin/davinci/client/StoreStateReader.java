package com.linkedin.davinci.client;

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
  private static final StoreJSONSerializer storeSerializer = new StoreJSONSerializer();
  private static final SystemStoreJSONSerializer systemStoreSerializer = new SystemStoreJSONSerializer();

  private final String storeName;
  private final String requestPath;
  private final String exceptionMessageFooter;
  private final VeniceSystemStoreType veniceSystemStoreType;
  private final AbstractAvroStoreClient storeClient;

  private StoreStateReader(AbstractAvroStoreClient client) {
    this.storeClient = client;
    this.storeName = client.getStoreName();
    requestPath = VeniceConstants.TYPE_STORE_STATE + "/" + storeName;
    exceptionMessageFooter = "while trying to fetch store: " + storeName + " with path: " + requestPath;
    veniceSystemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
  }

  public static StoreStateReader getInstance(ClientConfig clientConfig) {
    AvroGenericStoreClientImpl client =
        new AvroGenericStoreClientImpl<>(ClientFactory.getTransportClient(clientConfig), false, clientConfig);
    client.start();
    return new StoreStateReader(client);
  }

  public Store getStore() {
    try {
      byte[] response = RetryUtils.executeWithMaxAttempt(
          () -> ((CompletableFuture<byte[]>) storeClient.getRaw(requestPath)).get(),
          3,
          Duration.ofMillis(100),
          Arrays.asList(ExecutionException.class));
      if (null == response) {
        throw new VeniceClientException("Unexpected null response " + exceptionMessageFooter);
      }
      if (veniceSystemStoreType != null && veniceSystemStoreType.isNewMedataRepositoryAdopted()) {
        SerializableSystemStore serializableSystemStore = systemStoreSerializer.deserialize(response, null);
        return new SystemStore(
            serializableSystemStore.getZkSharedStore(),
            serializableSystemStore.getSystemStoreType(),
            serializableSystemStore.getVeniceStore());
      } else {
        return storeSerializer.deserialize(response, null);
      }
    } catch (Exception e) {
      throw new VeniceClientException("Unexpected exception " + exceptionMessageFooter, e);
    }
  }

  @Override
  public void close() {
    Utils.closeQuietlyWithErrorLogged(storeClient);
  }
}
