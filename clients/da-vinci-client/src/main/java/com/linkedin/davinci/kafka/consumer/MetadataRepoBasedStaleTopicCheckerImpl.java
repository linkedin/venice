package com.linkedin.davinci.kafka.consumer;

import com.linkedin.alpini.base.misc.Pair;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MetadataRepoBasedStaleTopicCheckerImpl implements StaleTopicChecker {
  private static final Logger LOGGER = LogManager.getLogger(MetadataRepoBasedStaleTopicCheckerImpl.class);
  static final long STALE_TOPIC_CHECK_INTERVAL_MS = TimeUnit.MINUTES.toMillis(30);
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
        return shouldRealTimeTopicExist(store, topic);
      }
    } catch (VeniceNoStoreException e) {
      LOGGER.warn("Store not found for topic: {}", topic);
      return false;
    } catch (Exception e) {
      LOGGER.error("Exception thrown in checkTopicExists; unable to decide if topic {} should exist ", topic, e);
    }
    return true;
  }

  /**
   * Check if the topic should exist based on the store's hybrid version.
   * If the store is hybrid, the topic should exist.
   * If the store is not hybrid, check if it has a hybrid version.
   */
  private boolean shouldRealTimeTopicExist(Store store, String topic) {
    if (store.isHybrid()) {
      return true;
    }

    // Default value - always check whether the store contains hybrid version
    boolean containsHybridVersion = false;
    boolean shouldRecheck = true;

    Pair<Long, Boolean> cachedInfo = hybridVersionCheckCache.get(store);

    if (cachedInfo != null) {
      long lastCheckTime = cachedInfo.getFirst();
      boolean hasHybridVersion = cachedInfo.getSecond();
      if (hasHybridVersion) {
        return true;
      }
      // If the store did not have hybrid version in previous check and the last check time is longer than the stale
      // topic check interval, we should recheck the store to see if it has hybrid version now.
      shouldRecheck = LatencyUtils.getElapsedTimeFromMsToMs(lastCheckTime) >= STALE_TOPIC_CHECK_INTERVAL_MS;
    }

    if (shouldRecheck) {
      containsHybridVersion = Version.containsHybridVersion(store.getVersions());
      hybridVersionCheckCache.put(store, new Pair<>(System.currentTimeMillis(), containsHybridVersion));
    }

    if (!containsHybridVersion) {
      LOGGER.info(
          "Store {} is not hybrid and has no hybrid version, so topic {} should not exist",
          store.getName(),
          topic);
      return false;
    }

    return true;
  }

  /**
   * This method is used for testing purposes only.
   * It allows to set the hybrid version check cache directly.
   */
  protected void setHybridVersionCheckCache(Store store, Pair<Long, Boolean> hybridVersionCheckCache) {
    this.hybridVersionCheckCache.put(store, hybridVersionCheckCache);
  }

  /**
   * This method is used for testing purposes only.
   */
  protected Pair<Long, Boolean> getHybridVersionCheckCache(Store store) {
    return this.hybridVersionCheckCache.get(store);
  }
}
