package com.linkedin.venice.helix;

import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;


public class ZkClientFactory {
  public static ZkClient newZkClient(String zkServers) {
    HelixZkClient.ZkConnectionConfig connectionConfig = new HelixZkClient.ZkConnectionConfig(zkServers);
    return (ZkClient) SharedZkClientFactory.getInstance().buildZkClient(connectionConfig);
  }

  public static ZkClient newZkClient(String zkServers, long operationRetryTimeoutInMs) {
    HelixZkClient.ZkConnectionConfig connectionConfig = new HelixZkClient.ZkConnectionConfig(zkServers);
    HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();
    clientConfig.setOperationRetryTimeout(operationRetryTimeoutInMs);
    return (ZkClient) SharedZkClientFactory.getInstance().buildZkClient(connectionConfig, clientConfig);
  }
}
