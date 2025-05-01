package com.linkedin.davinci.kafka.consumer;

import com.linkedin.alpini.base.misc.Pair;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MetadataRepoBasedStaleTopicCheckerImpl implements StaleTopicChecker {
  private static final Logger LOGGER = LogManager.getLogger(MetadataRepoBasedStaleTopicCheckerImpl.class);
  private final ReadOnlyStoreRepository readOnlyStoreRepository;
  private final ConcurrentHashMap<Store, Pair<Long, Boolean>> hybridVersionCheckCache = new ConcurrentHashMap<>();

  public MetadataRepoBasedStaleTopicCheckerImpl(ReadOnlyStoreRepository readOnlyStoreRepository) {
    this.readOnlyStoreRepository = readOnlyStoreRepository;
  }

  public boolean shouldTopicExist(String topic) {
    try {
      String storeName = Version.parseStoreFromKafkaTopicName(topic);
      Store store = readOnlyStoreRepository.getStoreOrThrow(storeName);

      if (Version.isVersionTopicOrStreamReprocessingTopic(topic)) {
        int version = Version.parseVersionFromKafkaTopicName(topic);
        if (store.getVersion(version) == null) {
          LOGGER.warn("Version {} not found for topic: {}", version, topic);
          return false;
        }
      } else if (Version.isRealTimeTopic(topic)) {
        if (!store.isHybrid() && !Version.containsHybridVersion(store.getVersions())) {
          LOGGER.warn("Store {} is not hybrid and no hybrid version, so topic {} should not exist", storeName, topic);
          return false;
        }
      }
    } catch (VeniceNoStoreException e) {
      LOGGER.warn("Store not found for topic: {}", topic);
      return false;
    } catch (Exception e) {
      LOGGER.error("Exception thrown in checkTopicExists; unable to decide if topic {} should exist ", topic, e);
    }
    return true;
  }
}
