package com.linkedin.davinci.client;

import com.linkedin.davinci.storage.chunking.GenericChunkingAdapter;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * Version-specific DaVinci client implementation that subscribes to a specific store version.
 *
 * Key features:
 * - Subscribes to a specific version (does not follow version swaps)
 * - Validates version existence when subscribing
 * - Cannot be mixed with regular DaVinci clients on the same store
 */
public class VersionSpecificAvroGenericDaVinciClient<K, V> extends AvroGenericDaVinciClient<K, V> {
  public VersionSpecificAvroGenericDaVinciClient(
      DaVinciConfig daVinciConfig,
      ClientConfig clientConfig,
      VeniceProperties backendConfig,
      Optional<Set<String>> managedClients,
      int storeVersion) {

    super(
        daVinciConfig,
        clientConfig,
        backendConfig,
        managedClients,
        null,
        GenericChunkingAdapter.INSTANCE,
        () -> {},
        null,
        storeVersion);
  }

  @Override
  protected CompletableFuture<Void> subscribe(ComplementSet<Integer> partitions) {
    throwIfNotReady();

    Store store = getBackend().getStoreRepository().getStoreOrThrow(getStoreName());
    Version targetVersionObj = store.getVersion(getStoreVersion());

    if (targetVersionObj == null) {
      throw new VeniceClientException("Version: " + getStoreVersion() + " does not exist for store: " + getStoreName());
    }

    addPartitionsToSubscription(partitions);
    return getStoreBackend().subscribe(partitions, Optional.of(targetVersionObj));
  }
}
