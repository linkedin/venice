package com.linkedin.venice.controller.helix;

import com.linkedin.venice.helix.HelixAdapterSerializer;
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
public class SharedHelixReadOnlyZKSharedSystemStoreRepository extends HelixReadOnlyZKSharedSystemStoreRepository
    implements Closeable {
  public SharedHelixReadOnlyZKSharedSystemStoreRepository(
      ZkClient zkClient,
      HelixAdapterSerializer compositeSerializer,
      String systemStoreClusterName) {
    super(zkClient, compositeSerializer, systemStoreClusterName);
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
