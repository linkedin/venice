package com.linkedin.davinci.client;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;


/**
 * Version-specific DaVinci client implementation that subscribes to a specific store version.
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
public class VersionSpecificAvroGenericDaVinciClient<K, V> extends AvroGenericSeekableDaVinciClient<K, V> {
  public VersionSpecificAvroGenericDaVinciClient(
      DaVinciConfig daVinciConfig,
      ClientConfig clientConfig,
      VeniceProperties backendConfig,
      Optional<Set<String>> managedClients,
      ICProvider icProvider,
      Executor readChunkExecutorForLargeRequest,
      int storeVersion) {

    super(
        daVinciConfig,
        clientConfig,
        backendConfig,
        managedClients,
        icProvider,
        readChunkExecutorForLargeRequest,
        storeVersion);
  }
}
