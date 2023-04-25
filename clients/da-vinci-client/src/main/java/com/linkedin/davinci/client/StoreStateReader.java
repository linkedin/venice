package com.linkedin.davinci.client;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;


/**
 * This class retrieves the corresponding Store object from a router endpoint which contains the latest store state.
 * E.g. store level configs and versions. This class doesn't maintain any local cache of the retrieved store state.
 *
 * @deprecated Use {@link com.linkedin.venice.store.StoreStateReader} instead
 */
@Deprecated
public class StoreStateReader implements Closeable {
  private final com.linkedin.venice.store.StoreStateReader internalStoreStateReader;

  private StoreStateReader(com.linkedin.venice.store.StoreStateReader storeStateReader) {
    this.internalStoreStateReader = storeStateReader;
  }

  @Deprecated
  public static StoreStateReader getInstance(ClientConfig clientConfig) {
    return new StoreStateReader(com.linkedin.venice.store.StoreStateReader.getInstance(clientConfig));
  }

  @Deprecated
  public Store getStore() {
    return internalStoreStateReader.getStore();
  }

  @Override
  public void close() {
    Utils.closeQuietlyWithErrorLogged(internalStoreStateReader);
  }
}
