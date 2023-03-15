package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.exceptions.MissingKeyInStoreMetadataException;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import java.util.concurrent.ExecutionException;


/**
 * An implementation of the {@link VeniceClientBasedMetadata} that uses a Venice thin client to refresh the local
 * metadata cache periodically.
 */
public class ThinClientBasedMetadata extends VeniceClientBasedMetadata {
  private final AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> thinClient;

  public ThinClientBasedMetadata(
      ClientConfig clientConfig,
      AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> thinClient) {
    super(clientConfig);
    if (thinClient == null) {
      throw new VeniceClientException("'thinClient' should not be null when ThinClientBasedMetadata is being used");
    }
    this.thinClient = thinClient;
  }

  @Override
  protected StoreMetaValue getStoreMetaValue(StoreMetaKey key) {
    try {
      StoreMetaValue value = thinClient.get(key).get();
      if (value == null) {
        throw new MissingKeyInStoreMetadataException(key.toString(), StoreMetaValue.class.getSimpleName());
      }
      return value;
    } catch (InterruptedException | ExecutionException e) {
      throw new VeniceClientException(
          "Failed to get data from thin client meta store for store: " + storeName + " with key: " + key.toString(),
          e);
    }
  }
}
