package com.linkedin.davinci.consumer;

import com.linkedin.davinci.repository.ThinClientMetaStoreBasedRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.HashSet;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class VersionSwapDataChangeListener<K, V> implements StoreDataChangedListener {
  private static final Logger LOGGER = LogManager.getLogger(VersionSwapDataChangeListener.class);
  private final VeniceAfterImageConsumerImpl<K, V> consumer;
  private final ThinClientMetaStoreBasedRepository storeRepository;
  private final String storeName;
  private final String consumerName;

  VersionSwapDataChangeListener(
      VeniceAfterImageConsumerImpl<K, V> consumer,
      ThinClientMetaStoreBasedRepository storeRepository,
      String storeName,
      String consumerName) {
    this.consumer = consumer;
    this.storeRepository = storeRepository;
    this.storeName = storeName;
    this.consumerName = consumerName;
  }

  @Override
  public void handleStoreChanged(Store store) {
    // store may be null as this is called by other repair tasks
    if (!consumer.subscribed()) {
      // skip this for now as the consumer hasn't even been set up yet
      return;
    }
    synchronized (this) {
      Set<Integer> partitions = new HashSet<>();
      try {
        // Check the current version of the server
        int currentVersion = this.storeRepository.getStore(this.storeName).getCurrentVersion();

        // Check the current ingested version
        Set<PubSubTopicPartition> subscriptions = this.consumer.getTopicAssignment();
        if (subscriptions.isEmpty()) {
          return;
        }

        // for all partition subscriptions that are not subscribed to the current version, resubscribe them
        for (PubSubTopicPartition topicPartition: subscriptions) {
          int version = Version.parseVersionFromVersionTopicName(topicPartition.getPubSubTopic().getName());
          if (version != currentVersion) {
            partitions.add(topicPartition.getPartitionNumber());
          }
        }

        LOGGER.info(
            "New Version detected!  Seeking consumer to version: " + currentVersion + " in consumer: " + consumerName);
        this.consumer.seekToEndOfPush(partitions).get();
      } catch (Exception e) {
        LOGGER.error(
            "Seek to End of Push Failed for store: " + this.storeName + " partitions: " + partitions + " on consumer: "
                + consumerName + "will retry...",
            e);
      }
    }
  }
}
