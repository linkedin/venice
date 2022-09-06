package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MetadataRepoBasedTopicExistingCheckerImpl implements TopicExistenceChecker {
  private final ReadOnlyStoreRepository readOnlyStoreRepository;
  private final Logger logger = LogManager.getLogger(MetadataRepoBasedTopicExistingCheckerImpl.class);

  public MetadataRepoBasedTopicExistingCheckerImpl(ReadOnlyStoreRepository readOnlyStoreRepository) {
    this.readOnlyStoreRepository = readOnlyStoreRepository;
  }

  public boolean checkTopicExists(String topic) {
    boolean isExistingTopic = true;

    try {
      String storeName = Version.parseStoreFromKafkaTopicName(topic);
      Store store = readOnlyStoreRepository.getStoreOrThrow(storeName);

      if (Version.isVersionTopicOrStreamReprocessingTopic(topic)) {
        int version = Version.parseVersionFromKafkaTopicName(topic);
        if (!store.getVersion(version).isPresent()) {
          isExistingTopic = false;
        }
      } else if (Version.isRealTimeTopic(topic)) {
        if (!store.isHybrid()) {
          isExistingTopic = false;
        }
      }
    } catch (VeniceNoStoreException e) {
      isExistingTopic = false;
    } catch (Exception e) {
      logger.error("Exception thrown in checkTopicExists: ", e);
    }
    return isExistingTopic;
  }
}
