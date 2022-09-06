package com.linkedin.venice.controller.helix;

import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository;
import java.io.Closeable;
import java.io.IOException;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * This class is intended to only be used in Controller, which is sharing one instance across
 * all the clusters.
 * Since the leader->standby transition for a given cluster will clear all the resources, this implementation
 * will do nothing in {@link #clear()} since it will be shared across all the clusters, whose leader are
 * in the same Controller node.
 */
public class SharedHelixReadOnlyZKSharedSchemaRepository extends HelixReadOnlyZKSharedSchemaRepository
    implements Closeable {
  public SharedHelixReadOnlyZKSharedSchemaRepository(
      HelixReadOnlyZKSharedSystemStoreRepository storeRepository,
      ZkClient zkClient,
      HelixAdapterSerializer adapter,
      String systemStoreClusterName,
      int refreshAttemptsForZkReconnect,
      long refreshIntervalForZkReconnectInMs) {
    super(
        storeRepository,
        zkClient,
        adapter,
        systemStoreClusterName,
        refreshAttemptsForZkReconnect,
        refreshIntervalForZkReconnectInMs);
  }

  @Override
  public void clear() {
    /**
     * Do nothing here since it is shared across different clusters.
     */
  }

  @Override
  public void close() throws IOException {
    super.clear();
  }
}
