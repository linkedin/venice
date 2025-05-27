package com.linkedin.venice.helix;

import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * This repository is used to retrieve all the zk shared system store schemas from the system store cluster.
 *
 * The reason to introduce this class is that this class needs to use a {@link HelixReadOnlyZKSharedSystemStoreRepository}
 * to make sure that this repo only accesses the schemas belonging to zk shared system stores to avoid unnnecessary
 * zk watches.
 */
public class HelixReadOnlyZKSharedSchemaRepository extends HelixReadOnlySchemaRepository {
  public HelixReadOnlyZKSharedSchemaRepository(
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
}
