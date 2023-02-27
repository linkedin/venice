package com.linkedin.venice.controller;

import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.utils.Pair;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A task that periodically polls the job state for existing version topics in the parent fabric. This is to make sure
 * topics in the parent fabric are cleaned up in a timely manner once they reach terminal states in all child fabrics.
 * This is to prevent out-of-order issue when compaction is enabled and MM rewind old messages from parent to child
 * fabrics during restart. Topic cleanup is part of job status polling because it's only safe to delete the VT in parent
 * fabric once the job reaches terminal state in all child fabrics.
 */
public class TerminalStateTopicCheckerForParentController implements Runnable, Closeable {
  private static final long MAX_BACKOFF_MS = TimeUnit.HOURS.toMillis(6);

  private static final Logger LOGGER = LogManager.getLogger(TerminalStateTopicCheckerForParentController.class);
  private final AtomicBoolean isRunning = new AtomicBoolean(true);
  private final VeniceParentHelixAdmin parentController;
  private final HelixReadOnlyStoreConfigRepository storeConfigRepository;
  /**
   * Map of topic name to a pair representing the backoff where the first element of the pair is the time which we can
   * check the topic's job status again and the second element is the current backoff time.
   */
  private final Map<String, Pair<Long, Long>> topicToBackoffMap = new HashMap<>();
  private final long checkDelayInMs;

  public TerminalStateTopicCheckerForParentController(
      VeniceParentHelixAdmin parentController,
      HelixReadOnlyStoreConfigRepository storeConfigRepository,
      long checkDelayInMs) {
    this.parentController = parentController;
    this.storeConfigRepository = storeConfigRepository;
    this.checkDelayInMs = checkDelayInMs;
  }

  /**
   * Causes {@link TerminalStateTopicCheckerForParentController} task to stop executing.
   */
  @Override
  public void close() {
    isRunning.set(false);
    topicToBackoffMap.clear();
    LOGGER.info("Stopped running {}", getClass().getSimpleName());
  }

  @Override
  public void run() {
    LOGGER.info("Started running {}", getClass().getSimpleName());
    while (isRunning.get()) {
      try {
        Thread.sleep(checkDelayInMs);
        storeConfigRepository.refresh();
        Map<String, Map<String, Long>> relevantVersionTopics = getRelevantVeniceVersionTopics();
        for (Map.Entry<String, Map<String, Long>> relevantVersionTopicsEntry: relevantVersionTopics.entrySet()) {
          String storeName = relevantVersionTopicsEntry.getKey();
          // The relevantVersionTopics only contains topics for stores that exist in the local store config repo.
          String clusterName = storeConfigRepository.getStoreConfig(storeName).get().getCluster();
          // Check for leadership again because things could have changed since the relevantVersionTopics was acquired.
          if (!parentController.isLeaderControllerFor(clusterName)) {
            continue;
          }
          for (Map.Entry<String, Long> entry: relevantVersionTopicsEntry.getValue().entrySet()) {
            String topic = entry.getKey();
            long retention = entry.getValue();
            try {
              if (parentController.isTopicTruncatedBasedOnRetention(retention)) {
                topicToBackoffMap.remove(topic);
                continue;
              }
              if (topicToBackoffMap.containsKey(topic)
                  && topicToBackoffMap.get(topic).getFirst() > System.currentTimeMillis()) {
                continue;
              }
              if (parentController.getOffLinePushStatus(clusterName, topic).getExecutionStatus().isTerminal()) {
                // Topic is truncated since the overall status is terminal.
                topicToBackoffMap.remove(topic);
              } else {
                // Apply exponential backoff since polling job status is quite expensive.
                topicToBackoffMap.compute(topic, (t, backoff) -> {
                  long now = System.currentTimeMillis();
                  if (backoff == null) {
                    backoff = new Pair<>(now + checkDelayInMs, checkDelayInMs);
                  } else {
                    long expBackoff = Math.min(MAX_BACKOFF_MS, 2 * backoff.getSecond());
                    backoff = new Pair<>(now + expBackoff, expBackoff);
                  }
                  return backoff;
                });
              }
            } catch (Exception e) {
              LOGGER.error("Unexpected exception while checking topic state for: {}", topic);
            }
          }
        }
      } catch (InterruptedException ie) {
        break;
      } catch (Throwable e) {
        LOGGER.error("Unexpected throwable while running {}", getClass().getSimpleName(), e);
      }
    }
    close();
  }

  /**
   * Get version topics that don't have incremental push enabled and the current controller is the leader of its
   * corresponding Venice cluster.
   */
  private Map<String, Map<String, Long>> getRelevantVeniceVersionTopics() {
    Map<PubSubTopic, Long> topicRetentions = parentController.getTopicManager().getAllTopicRetentions();
    Map<String, Map<String, Long>> allVeniceVersionTopics = new HashMap<>();
    for (Map.Entry<PubSubTopic, Long> entry: topicRetentions.entrySet()) {
      String topic = entry.getKey().getName();
      try {
        if (!Version.isRealTimeTopic(topic) && Version.isVersionTopicOrStreamReprocessingTopic(topic)) {
          String storeName = Version.parseStoreFromKafkaTopicName(topic);
          Optional<StoreConfig> storeConfig = storeConfigRepository.getStoreConfig(storeName);
          if (!storeConfig.isPresent()) {
            // Local store config repo might be lagging behind.
            continue;
          }
          String clusterName = storeConfig.get().getCluster();
          if (!parentController.isLeaderControllerFor(clusterName)) {
            continue;
          }
          if (parentController.getStore(clusterName, storeName).isIncrementalPushEnabled()) {
            continue;
          }
          allVeniceVersionTopics.compute(storeName, (s, topics) -> {
            if (topics == null) {
              topics = new HashMap<>();
            }
            topics.put(topic, entry.getValue());
            return topics;
          });
        }
      } catch (Exception e) {
        LOGGER.error(
            "Unexpected exception while processing topic: {} for populating relevant Venice version topics map",
            topic,
            e);
      }
    }
    return allVeniceVersionTopics;
  }
}
