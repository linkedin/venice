package com.linkedin.venice.client.consumer;

import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.D2ControllerClientFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class VeniceChangeLogConsumerClientFactory {
  private final Map<String, VeniceChangelogConsumer> storeClientMap = new HashMap<>();

  private final ChangelogClientConfig globalChangeLogClientConfig;

  public VeniceChangeLogConsumerClientFactory(ChangelogClientConfig globalChangeLogClientConfig) {
    this.globalChangeLogClientConfig = globalChangeLogClientConfig;
  }

  public synchronized <K, V> VeniceChangelogConsumer<K, V> getChangeLogConsumer(String storeName) {
    return storeClientMap.computeIfAbsent(storeName, name -> {

      ChangelogClientConfig newStoreChangeLogClientConfig =
          ChangelogClientConfig.cloneConfig(globalChangeLogClientConfig).setStoreName(storeName);
      D2ControllerClient d2ControllerClient = D2ControllerClientFactory.discoverAndConstructControllerClient(
          storeName,
          globalChangeLogClientConfig.getControllerD2ServiceName(),
          globalChangeLogClientConfig.getVeniceURL(),
          Optional.ofNullable(newStoreChangeLogClientConfig.getInnerClientConfig().getSslFactory()),
          globalChangeLogClientConfig.getControllerRequestRetryCount());
      newStoreChangeLogClientConfig.setD2ControllerClient(d2ControllerClient);
      newStoreChangeLogClientConfig
          .setSchemaReader(ClientFactory.getSchemaReader(newStoreChangeLogClientConfig.getInnerClientConfig()));
      return new VeniceChangeLogConsumerImpl(newStoreChangeLogClientConfig);
    });
  }
}
