package com.linkedin.venice.client.consumer;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class VeniceChangeLogConsumerClientFactory {
  private final Map<String, VeniceChangelogConsumer> storeClientMap = new HashMap<>();

  private final ChangelogClientConfig globalChangeLogClientConfig;

  public VeniceChangeLogConsumerClientFactory(ChangelogClientConfig globalChangeLogClientConfig) {
    this.globalChangeLogClientConfig = globalChangeLogClientConfig;
  }

  public synchronized <K, V> VeniceChangelogConsumer<K, V> getChangeLogConsumer(String storeName, String clusterName) {
    return storeClientMap.computeIfAbsent(storeName, name -> {
      ChangelogClientConfig newStoreChangeLogClientConfig =
          ChangelogClientConfig.cloneConfig(globalChangeLogClientConfig).setStoreName(storeName);
      D2Client d2Client =
          ((D2TransportClient) ClientFactory.getTransportClient(globalChangeLogClientConfig.getInnerClientConfig()))
              .getD2Client();
      D2ControllerClient d2ControllerClient = new D2ControllerClient(
          globalChangeLogClientConfig.getD2ServiceName(),
          clusterName,
          d2Client,
          Optional.ofNullable(newStoreChangeLogClientConfig.getInnerClientConfig().getSslFactory()));
      newStoreChangeLogClientConfig.setD2ControllerClient(d2ControllerClient);
      newStoreChangeLogClientConfig
          .setSchemaReader(ClientFactory.getSchemaReader(newStoreChangeLogClientConfig.getInnerClientConfig()));
      return new VeniceChangeLogConsumerImpl(newStoreChangeLogClientConfig);
    });
  }
}
