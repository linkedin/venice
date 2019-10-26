package com.linkedin.venice.helix;

import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.manager.zk.client.SharedZkClientFactory;

public class ZkClientFactory {
  public static ZkClient newZkClient(String zkServers) {
    HelixZkClient.ZkConnectionConfig connectionConfig = new HelixZkClient.ZkConnectionConfig(zkServers);
    return (ZkClient) SharedZkClientFactory.getInstance().buildZkClient(connectionConfig);
  }
}
