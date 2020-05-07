package com.linkedin.venice.helix;

import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;


public class ZkClientFactory {
  public static ZkClient newZkClient(String zkServers) {
    HelixZkClient.ZkConnectionConfig connectionConfig = new HelixZkClient.ZkConnectionConfig(zkServers);
    return (ZkClient) SharedZkClientFactory.getInstance().buildZkClient(connectionConfig);
  }
}
