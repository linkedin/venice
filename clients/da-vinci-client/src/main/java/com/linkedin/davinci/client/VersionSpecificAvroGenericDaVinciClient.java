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
 * This is only intended for internal Venice use.
 * Must be used with {@link com.linkedin.venice.ConfigKeys#DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY
 * set to false. Otherwise, the client may subscribe to an unintended version based on what's on disk.
 *
 * Key features:
 * - Subscribes to a specific version (does not follow version swaps)
 * - Validates version existence when subscribing
 * - Cannot be mixed with regular DaVinci clients on the same store with the same
 *   {@link com.linkedin.davinci.DaVinciBackend} or JVM.
 *
 * Please note that when the client is subscribed to the backup version, on restart
 * {@link com.linkedin.davinci.DaVinciBackend#functionToCheckWhetherStorageEngineShouldBeKeptOrNot(Optional)}
 * will automatically delete the backup version data on disk and it will have to reingest. We are ok with this
 * behavior because the plan is to use {@link DaVinciRecordTransformer} in diskless mode where the user will need to
 * prevent the PubSub position to seek to instead of relying on the {@link com.linkedin.venice.offsets.OffsetRecord}.
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
    Version version = store.getVersion(getStoreVersion());

    if (version == null) {
      throw new VeniceClientException("Version: " + getStoreVersion() + " does not exist for store: " + getStoreName());
    }

    addPartitionsToSubscription(partitions);
    return getStoreBackend().subscribe(partitions, Optional.of(version));
  }
}
