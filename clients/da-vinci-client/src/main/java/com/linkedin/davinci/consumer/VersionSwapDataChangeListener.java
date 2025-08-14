package com.linkedin.davinci.consumer;

import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.davinci.repository.NativeMetadataRepositoryViewAdapter;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.views.VeniceView;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class VersionSwapDataChangeListener<K, V> implements StoreDataChangedListener {
  private static final Logger LOGGER = LogManager.getLogger(VersionSwapDataChangeListener.class);
  private static final int MAX_VERSION_SWAP_RETRIES = 3;
  private final VeniceAfterImageConsumerImpl<K, V> consumer;
  private NativeMetadataRepositoryViewAdapter storeRepository;
  private final String storeName;
  private final String consumerName;
  private final BasicConsumerStats changeCaptureStats;
  protected final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  VersionSwapDataChangeListener(
      VeniceAfterImageConsumerImpl<K, V> consumer,
      NativeMetadataRepositoryViewAdapter storeRepository,
      String storeName,
      String consumerName,
      BasicConsumerStats changeCaptureStats) {
    this.consumer = consumer;
    this.storeRepository = storeRepository;
    this.storeName = storeName;
    this.consumerName = consumerName;
    this.changeCaptureStats = changeCaptureStats;
  }

  @Override
  public void handleStoreChanged(Store store) {
    synchronized (this) {
      for (int attempt = 1; attempt <= MAX_VERSION_SWAP_RETRIES; attempt++) {
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
              "New version detected for store: {}. Performing Version Swap to version: {} for partitions: {} on consumer: {}",
              storeName,
              currentVersion,
              partitions,
              consumerName);

          this.consumer
              .internalSeekToEndOfPush(
                  partitions,
                  pubSubTopicRepository.getTopic(currentStore.getVersion(currentVersion).kafkaTopicName()),
                  true)
              .get();

          if (changeCaptureStats != null) {
            changeCaptureStats.emitVersionSwapCountMetrics(SUCCESS);
          }

          LOGGER.info(
              "Version Swap succeeded for store: {} when switching to version: {} for partitions: {} after {} attempts on consumer: {}",
              storeName,
              currentVersion,
              partitions,
              attempt,
              consumerName);
          return;
        } catch (Exception error) {
          if (attempt == MAX_VERSION_SWAP_RETRIES) {
            if (changeCaptureStats != null) {
              changeCaptureStats.emitVersionSwapCountMetrics(FAIL);
            }

            LOGGER.error(
                "Version Swap failed for store: {} for partitions: {} after {} attempts on consumer: {}",
                storeName,
                partitions,
                attempt,
                consumerName,
                error);
            consumer.handleVersionSwapFailure(error);
          } else {
            LOGGER.error(
                "Version Swap failed for store: {} for partitions: {} on attempt {}/{} on consumer: {}. Retrying.",
                storeName,
                partitions,
                attempt,
                MAX_VERSION_SWAP_RETRIES,
                consumerName);

            Utils.sleep(Duration.ofSeconds(1).toMillis() * attempt);
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
