package com.linkedin.venice.client.consumer;

import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.D2ControllerClientFactory;
import com.linkedin.venice.views.ChangeCaptureView;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class VeniceChangelogConsumerClientFactory {
  private final Map<String, VeniceChangelogConsumer> storeClientMap = new HashMap<>();

  private final ChangelogClientConfig globalChangelogClientConfig;

  public VeniceChangelogConsumerClientFactory(ChangelogClientConfig globalChangelogClientConfig) {
    this.globalChangelogClientConfig = globalChangelogClientConfig;
  }

  public synchronized <K, V> VeniceChangelogConsumer<K, V> getChangelogConsumer(String storeName) {
    return storeClientMap.computeIfAbsent(storeName, name -> {

      ChangelogClientConfig newStoreChangelogClientConfig =
          ChangelogClientConfig.cloneConfig(globalChangelogClientConfig).setStoreName(storeName);
      D2ControllerClient d2ControllerClient = D2ControllerClientFactory.discoverAndConstructControllerClient(
          storeName,
          globalChangelogClientConfig.getControllerD2ServiceName(),
          globalChangelogClientConfig.getLocalD2ZkHosts(),
          Optional.ofNullable(newStoreChangelogClientConfig.getInnerClientConfig().getSslFactory()),
          globalChangelogClientConfig.getControllerRequestRetryCount());
      newStoreChangelogClientConfig.setD2ControllerClient(d2ControllerClient);
      newStoreChangelogClientConfig
          .setSchemaReader(ClientFactory.getSchemaReader(newStoreChangelogClientConfig.getInnerClientConfig()));
      if (newStoreChangelogClientConfig.getViewClassName()
          .equals(ChangeCaptureView.CHANGE_CAPTURE_VIEW_WRITER_CLASS_NAME)) {
        return new VeniceChangelogConsumerImpl(newStoreChangelogClientConfig);
      }
      return new VeniceAfterImageConsumerImpl(newStoreChangelogClientConfig);
    });
  }
}
