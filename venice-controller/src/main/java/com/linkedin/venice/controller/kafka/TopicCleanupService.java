package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.service.AbstractVeniceService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;


/**
 * The topic cleanup in Venice adopts the following strategy:
 * 1. When Controller needs to clean up topics for retired versions or uncompleted store pushes or store deletion, it only
 * truncates the topics (lower topic retention) instead of deleting them right away.
 * 2. The {@link TopicCleanupService} is working as a single process to clean up all the unused topics.
 * With this way, most of time (no mastership handover), there is only one controller talking to Kafka to delete topic, which is expected
 * from Kafka's perspective to avoid concurrent topic deletion.
 * In theory, it is still possible to have two controllers talking to Kafka to delete topic during mastership handover since
 * the previous master controller could be still working on the topic cleaning up but the new master controller starts
 * processing.
 *
 * If required, there might be several ways to alleviate this potential concurrent Kafka topic deletion:
 * 1. Do master controller check every time when deleting topic;
 * 2. Register a callback to monitor mastership change;
 * 3. Use a global Zookeeper lock;
 *
 * Right now, {@link TopicCleanupService} is fully decoupled from {@link com.linkedin.venice.meta.Store} since there is
 * only one process actively running to cleanup topics and the controller running this process may not be the master
 * controller of the cluster owning the store that the topic to be deleted belongs to.
 *
 *
 * Here is how {@link TopicCleanupService} works to clean up deprecated topics [topic with low retention policy]:
 * 1. This service is only running in master controller of controller cluster, which means there should be only one
 * topic cleanup service running among all the Venice clusters (not strictly considering master handover.);
 * 2. This service is running in a infinite loop, which will execute the following operations:
 *    2.1 For every round, check whether current controller is the master controller of controller parent.
 *        If yes, continue; Otherwise, sleep for a pre-configured period and check again;
 *    2.2 Collect all the topics and categorize them based on store names;
 *    2.3 For deprecated real-time topic, will remove it right away;
 *    2.4 For deprecated version topics, will keep pre-configured minimal unused topics to avoid MM crash and remove others;
 */
public class TopicCleanupService extends AbstractVeniceService {
  private static final Logger LOGGER = Logger.getLogger(TopicCleanupService.class);

  private final Admin admin;
  private final Thread cleanupThread;
  private final long sleepIntervalBetweenTopicListFetchMs;
  private final int minNumberOfUnusedKafkaTopicsToPreserve;
  private boolean stop = false;
  private boolean isMasterControllerOfControllerCluster = false;

  public TopicCleanupService(Admin admin, VeniceControllerMultiClusterConfig multiClusterConfigs) {
    this.admin = admin;
    this.sleepIntervalBetweenTopicListFetchMs = multiClusterConfigs.getTopicCleanupSleepIntervalBetweenTopicListFetchMs();
    this.minNumberOfUnusedKafkaTopicsToPreserve = multiClusterConfigs.getMinNumberOfUnusedKafkaTopicsToPreserve();
    this.cleanupThread = new Thread(new TopicCleanupTask(), "TopicCleanupTask");
  }

  @Override
  public boolean startInner() throws Exception {
    cleanupThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stop = true;
    cleanupThread.interrupt();
  }

  public TopicManager getTopicManager() {
    return admin.getTopicManager();
  }

  private class TopicCleanupTask implements Runnable {

    @Override
    public void run() {
      while (!stop) {
        try {
          Thread.sleep(sleepIntervalBetweenTopicListFetchMs);
        } catch (InterruptedException e) {
          LOGGER.error("Received InterruptedException during sleep in TopicCleanup thread");
          break;
        }
        if (stop) {
          break;
        }
        try {
          if (admin.isMasterControllerOfControllerCluster()) {
            if (!isMasterControllerOfControllerCluster) {
              /**
               * Sleep for some time when current controller firstly becomes master controller of controller cluster.
               * This is trying to avoid concurrent Kafka topic deletion sent by both previous master controller
               * (Kafka topic cleanup doesn't finish in a short period) and the new master controller.
               */
              isMasterControllerOfControllerCluster = true;
              LOGGER.info("Current controller becomes the master controller of controller cluster");
              continue;
            }
            cleanupVeniceTopics();
          } else {
            isMasterControllerOfControllerCluster = false;
          }
        } catch (Exception e) {
          LOGGER.error("Received exception when cleaning up topics", e);
        }
      }
      LOGGER.info("TopicCleanupTask stopped");
    }
  }

  protected Admin getAdmin() {
    return admin;
  }

  protected Map<String, Map<String, Long>> getAllVeniceStoreTopics() {
    Map<String, Long> topicRetentions = getTopicManager().getAllTopicRetentions();
    Map<String, Map<String, Long>> allStoreTopics = new HashMap<>();

    for (Map.Entry<String, Long> entry : topicRetentions.entrySet()) {
      String topic = entry.getKey();
      long retention = entry.getValue();
      Optional<String> storeName = Optional.empty();
      if (Version.isRealTimeTopic(topic)) {
        storeName = Optional.of(Version.parseStoreFromRealTimeTopic(topic));
      } else if (Version.topicIsValidStoreVersion(topic)) {
        storeName = Optional.of(Version.parseStoreFromKafkaTopicName(topic));
      }
      if (!storeName.isPresent()) {
        // TODO: check whether Venice needs to cleanup topics not belonging to Venice.
        continue;
      }
      allStoreTopics.compute(storeName.get(), (s, topics) -> {
        if (null == topics) {
          topics = new HashMap<>();
        }
        topics.put(topic, retention);
        return topics;
      });
    }
    return allStoreTopics;
  }


  protected void cleanupVeniceTopics() {
    Map<String, Map<String, Long>> allStoreTopics = getAllVeniceStoreTopics();
    allStoreTopics.forEach((storeName, topicRetentions) -> {
      String realTimeTopic = Version.composeRealTimeTopic(storeName);
      /**
       * Until now, we haven't figured out a good way to handle real-time topic cleanup:
       * 1. If {@link TopicCleanupService} doesn't delete real-time topic, the truncated real-time topic could cause inconsistent data problem
       * between parent cluster and prod cluster if the deleted hybrid store gets re-created;
       * 2. If {@link TopicCleanupService} deletes the real-time topic, it might crash MM if application is still producing to the real-time topic
       * in parent cluster;
       *
       * Since Kafka nurse script will automatically kick in if MM crashes (which should still happen very infrequently),
       * for the time being, we choose to delete the real-time topic.
       *
       * TODO: figure out a better way to handle real-time topic cleanup.
       */
      if (topicRetentions.containsKey(realTimeTopic)) {
        if (admin.isTopicTruncatedBasedOnRetention(topicRetentions.get(realTimeTopic))) {
          LOGGER.info("Real-time topic: " + realTimeTopic + " is deprecated, will delete it");
          getTopicManager().ensureTopicIsDeletedAndBlock(realTimeTopic);
          LOGGER.info("Real-time topic: " + realTimeTopic + " was deleted");
        }
        topicRetentions.remove(realTimeTopic);
      }

      List<String> oldTopicsToDelete = extractVeniceTopicsToCleanup(topicRetentions);

      if (oldTopicsToDelete.isEmpty()) {
        LOGGER.debug("Searched for old topics belonging to store '" + storeName + "', and did not find any.");
      } else {
        LOGGER.info("Detected the following old topics to delete: " + String.join(", ", oldTopicsToDelete));
        oldTopicsToDelete.stream()
            .forEach(t -> getTopicManager().ensureTopicIsDeletedAndBlock(t));
        LOGGER.info("Finished deleting old topics for store '" + storeName + "'.");
      }
    });
  }

  protected List<String> extractVeniceTopicsToCleanup(Map<String, Long> topicRetentions) {
    if (topicRetentions.isEmpty()) {
      return new ArrayList<>();
    }
    Set<String> veniceTopics = topicRetentions.keySet();
    int maxVersion = veniceTopics.stream()
        .map(t -> Version.parseVersionFromKafkaTopicName(t))
        .max(Integer::compare)
        .get();

    final long maxVersionNumberToDelete = maxVersion - minNumberOfUnusedKafkaTopicsToPreserve;

    return veniceTopics.stream()
        /** Consider only truncated topics */
        .filter(t -> admin.isTopicTruncatedBasedOnRetention(topicRetentions.get(t)))
        /** Always preserve the last {@link #minNumberOfUnusedKafkaTopicsToPreserve} topics, whether they are healthy or not */
        .filter(t -> Version.parseVersionFromKafkaTopicName(t) <= maxVersionNumberToDelete)
        .collect(Collectors.toList());
  }
}