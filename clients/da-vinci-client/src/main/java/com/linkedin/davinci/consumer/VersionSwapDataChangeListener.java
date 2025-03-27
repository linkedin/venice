package com.linkedin.davinci.consumer;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.repository.NativeMetadataRepositoryViewAdapter;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.views.VeniceView;
import java.util.HashSet;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class VersionSwapDataChangeListener<K, V> implements StoreDataChangedListener {
  private static final Logger LOGGER = LogManager.getLogger(VersionSwapDataChangeListener.class);
  private static final int MAX_RETRIES = 3;
  private final VeniceAfterImageConsumerImpl<K, V> consumer;
  private NativeMetadataRepositoryViewAdapter storeRepository;
  private final String storeName;
  private final String consumerName;
  protected final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  VersionSwapDataChangeListener(
      VeniceAfterImageConsumerImpl<K, V> consumer,
      NativeMetadataRepositoryViewAdapter storeRepository,
      String storeName,
      String consumerName) {
    this.consumer = consumer;
    this.storeRepository = storeRepository;
    this.storeName = storeName;
    this.consumerName = consumerName;
  }

  @Override
  public void handleStoreChanged(Store store) {
    synchronized (this) {
      for (int i = 0; i <= MAX_RETRIES; i++) {
        // store may be null as this is called by other repair tasks
        if (!consumer.subscribed()) {
          // skip this for now as the consumer hasn't even been set up yet
          return;
        }
        Set<Integer> partitions = new HashSet<>();
        try {
          // Check the current version of the server, we use the store repo so that for retries tripped because
          // of a deleted version topic, we'll always get the latest version on subsequent retries
          Store currentStore = storeRepository.getStore(storeName);
          int currentVersion = currentStore.getCurrentVersion();

          // Check the current ingested version
          Set<PubSubTopicPartition> subscriptions = this.consumer.getTopicAssignment();
          if (subscriptions.isEmpty()) {
            return;
          }

          // for all partition subscriptions that are not subscribed to the current version, resubscribe them
          for (PubSubTopicPartition topicPartition: subscriptions) {
            int version;
            if (topicPartition.getPubSubTopic().isViewTopic()) {
              version = VeniceView.parseVersionFromViewTopic(topicPartition.getPubSubTopic().getName());
            } else {
              version = Version.parseVersionFromVersionTopicName(topicPartition.getPubSubTopic().getName());
            }
            if (version != currentVersion) {
              partitions.add(topicPartition.getPartitionNumber());
            }
          }

          if (partitions.isEmpty()) {
            return;
          }

          LOGGER.info(
              "New Version detected!  Seeking consumer to version: " + currentVersion + " in consumer: "
                  + consumerName);
          this.consumer
              .internalSeekToEndOfPush(
                  partitions,
                  pubSubTopicRepository.getTopic(currentStore.getVersion(currentVersion).kafkaTopicName()),
                  true)
              .get();
          LOGGER.info("Seeked consumer to version: " + currentVersion + " in consumer: " + consumerName);
          return;
        } catch (Exception e) {
          LOGGER.error(
              "Seek to End of Push Failed for store: " + this.storeName + " partitions: " + partitions
                  + " on consumer: " + consumerName + "will retry...",
              e);
          if (i == MAX_RETRIES) {
            LOGGER.error(
                "Max retries reached for seeking to end of push for store: " + this.storeName + " partitions: "
                    + partitions + " on consumer: " + consumerName);
            return;
          }
        }
      }
    }
  }

  @VisibleForTesting
  void setStoreRepository(NativeMetadataRepositoryViewAdapter storeRepository) {
    // This is chiefly to make static analysis happy
    synchronized (this) {
      this.storeRepository = storeRepository;
    }
  }
}
