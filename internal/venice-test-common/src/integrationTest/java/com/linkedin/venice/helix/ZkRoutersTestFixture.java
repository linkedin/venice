package com.linkedin.venice.helix;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * Shared test fixture for ZkRouters tests. Encapsulates per-test ZkServer, ZkClient,
 * and HelixAdapterSerializer creation/teardown.
 */
public class ZkRoutersTestFixture {
  private ZkClient zkClient;
  private ZkServerWrapper zkServerWrapper;
  private String clusterName;
  private HelixAdapterSerializer adapter;

  public void setUp() {
    clusterName = "ZkRoutersClusterManagerTest";
    zkServerWrapper = ServiceFactory.getZkServer();
    adapter = new HelixAdapterSerializer();
    zkClient = new ZkClient(zkServerWrapper.getAddress());
  }

  public void tearDown() {
    zkClient.close();
    zkServerWrapper.close();
  }

  public ZkClient getZkClient() {
    return zkClient;
  }

  public ZkServerWrapper getZkServerWrapper() {
    return zkServerWrapper;
  }

  public String getClusterName() {
    return clusterName;
  }

  public HelixAdapterSerializer getAdapter() {
    return adapter;
  }

  public ZkRoutersClusterManager createManager() {
    return createManager(zkClient);
  }

  public ZkRoutersClusterManager createManager(ZkClient zkClient) {
    ZkRoutersClusterManager manager = new ZkRoutersClusterManager(zkClient, adapter, clusterName, 1, 1000);
    manager.refresh();
    manager.createRouterClusterConfig();
    return manager;
  }
}
