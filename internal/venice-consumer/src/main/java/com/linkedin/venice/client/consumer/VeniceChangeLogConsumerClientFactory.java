package com.linkedin.venice.client.consumer;

import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.SubscriptionBasedStoreRepository;
import com.linkedin.venice.helix.ZkClientFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.zookeeper.impl.client.ZkClient;


public class VeniceChangeLogConsumerClientFactory {
  private final Map<String, VeniceChangelogConsumer> storeClientMap = new HashMap<>();

  private final ChangelogClientConfig globalChangeLogClientConfig;

  public VeniceChangeLogConsumerClientFactory(ChangelogClientConfig globalChangeLogClientConfig) {
    this.globalChangeLogClientConfig = globalChangeLogClientConfig;

  }

  public synchronized <K, V> VeniceChangelogConsumer<K, V> getChangeLogConsumer(String storeName, String clusterName) {
    return storeClientMap.computeIfAbsent(storeName, name -> {
      ChangelogClientConfig newStoreChangeLogClientConfig =
          ChangelogClientConfig.cloneConfig(globalChangeLogClientConfig)
              .setStoreName(storeName)
              .setClusterName(clusterName);
      ZkClient zkClient = ZkClientFactory.newZkClient(globalChangeLogClientConfig.getZkAddressForStoreRepo());
      HelixAdapterSerializer adapter = new HelixAdapterSerializer();
      HelixReadOnlyStoreRepository storeRepo = new SubscriptionBasedStoreRepository(zkClient, adapter, clusterName);
      storeRepo.refresh();
      newStoreChangeLogClientConfig.setStoreRepo(storeRepo);
      newStoreChangeLogClientConfig
          .setSchemaReader(ClientFactory.getSchemaReader(newStoreChangeLogClientConfig.getInnerClientConfig()));
      return new VeniceChangeLogConsumerImpl(newStoreChangeLogClientConfig);
    });
  }
}
