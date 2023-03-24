package com.linkedin.venice.client.consumer;

import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.D2ControllerClientFactory;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ViewConfig;
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

      // TODO: This is a redundant controller query. Need to condense it with the storeInfo query that happens
      // inside the changecaptureclient itself
      String viewClass =
          newStoreChangelogClientConfig.getViewName() == null ? "" : newStoreChangelogClientConfig.getViewName();
      if (!viewClass.isEmpty()) {
        viewClass = getViewClass(
            storeName,
            newStoreChangelogClientConfig.getViewName(),
            d2ControllerClient,
            globalChangelogClientConfig.getControllerRequestRetryCount());
      }
      if (viewClass.equals(ChangeCaptureView.class.getCanonicalName())) {
        return new VeniceChangelogConsumerImpl(newStoreChangelogClientConfig);
      }
      return new VeniceAfterImageConsumerImpl(newStoreChangelogClientConfig);
    });
  }

  private String getViewClass(String storeName, String viewName, D2ControllerClient d2ControllerClient, int retries) {
    StoreResponse response =
        d2ControllerClient.retryableRequest(retries, controllerClient -> controllerClient.getStore(storeName));
    if (response.isError()) {
      throw new VeniceException(
          "Couldn't retrieve store information when building change capture client for store " + storeName);
    }
    ViewConfig viewConfig = response.getStore().getViewConfigs().get(viewName);
    if (viewConfig == null) {
      throw new VeniceException(
          "Couldn't retrieve store view information when building change capture client for store " + storeName
              + " viewName " + viewName);
    }
    return viewConfig.getViewClassName();
  }
}
