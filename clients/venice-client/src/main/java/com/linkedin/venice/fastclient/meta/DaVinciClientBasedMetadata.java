package com.linkedin.venice.fastclient.meta;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.exceptions.MissingKeyInStoreMetadataException;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import java.util.concurrent.ExecutionException;


/**
 * A wrapper with a DaVinci client subscribed to the corresponding Meta store to serve data required by {@link StoreMetadata}.
 * TODO All data are cached locally and refreshed periodically for performance reasons before either object cache becomes
 *  available for meta system store or a decorator class of the underlying rocksDB classes is made available for consuming
 *  deserialized meta system store data directly.
 */
@Deprecated
public class DaVinciClientBasedMetadata extends VeniceClientBasedMetadata {
  private final DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClient;

  public DaVinciClientBasedMetadata(
      ClientConfig clientConfig,
      DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClient) {
    super(clientConfig);
    if (daVinciClient == null) {
      throw new VeniceClientException(
          "'daVinciClient' should not be null in when DaVinciClientBasedMetadata is being used.");
    }
    this.daVinciClient = daVinciClient;
  }

  @Override
  public void start() {
    try {
      daVinciClient.subscribeAll().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new VeniceClientException(
          "Failed to start the " + DaVinciClientBasedMetadata.class.getSimpleName() + " for store: " + storeName,
          e);
    }
    super.start();
  }

  @Override
  protected StoreMetaValue getStoreMetaValue(StoreMetaKey key) {
    try {
      StoreMetaValue value = daVinciClient.get(key).get();
      if (value == null) {
        throw new MissingKeyInStoreMetadataException(key.toString(), StoreMetaValue.class.getSimpleName());
      }
      return value;
    } catch (InterruptedException | ExecutionException e) {
      throw new VeniceClientException(
          "Failed to get data from DaVinci client backed meta store for store: " + storeName + " with key: "
              + key.toString());
    }
  }
}
