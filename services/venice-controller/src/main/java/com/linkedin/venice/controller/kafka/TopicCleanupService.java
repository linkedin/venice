package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.Time;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The topic cleanup in Venice adopts the following strategy:
 * 1. When Controller needs to clean up topics for retired versions or uncompleted store pushes or store deletion, it only
 * truncates the topics (lower topic retention) instead of deleting them right away.
 * 2. The {@link TopicCleanupService} is working as a single process to clean up all the unused topics.
 * With this way, most of time (no leadership handover), there is only one controller talking to Kafka to delete topic, which is expected
 * from Kafka's perspective to avoid concurrent topic deletion.
 * In theory, it is still possible to have two controllers talking to Kafka to delete topic during leadership handover since
 * the previous leader controller could be still working on the topic cleaning up but the new leader controller starts
 * processing.
 *
 * If required, there might be several ways to alleviate this potential concurrent Kafka topic deletion:
 * 1. Do leader controller check every time when deleting topic;
 * 2. Register a callback to monitor leadership change;
 * 3. Use a global Zookeeper lock;
 *
 * Right now, {@link TopicCleanupService} is fully decoupled from {@link com.linkedin.venice.meta.Store} since there is
 * only one process actively running to cleanup topics and the controller running this process may not be the leader
 * controller of the cluster owning the store that the topic to be deleted belongs to.
 *
 *
 * Here is how {@link TopicCleanupService} works to clean up deprecated topics [topic with low retention policy]:
 * 1. This service is only running in leader controller of controller cluster, which means there should be only one
 * topic cleanup service running among all the Venice clusters (not strictly considering leader handover.);
 * 2. This service is running in a infinite loop, which will execute the following operations:
 *    2.1 For every round, check whether current controller is the leader controller of controller parent.
 *        If yes, continue; Otherwise, sleep for a pre-configured period and check again;
 *    2.2 Collect all the topics and categorize them based on store names;
 *    2.3 For deprecated real-time topic, will remove it right away;
 *    2.4 For deprecated version topics, will keep pre-configured minimal unused topics to avoid MM crash and remove others;
 */
public class TopicCleanupService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(TopicCleanupService.class);
  private static final Map<String, Integer> storeToCountdownForDeletion = new HashMap<>();

  private final Admin admin;
  private final Thread cleanupThread;
  protected final long sleepIntervalBetweenTopicListFetchMs;
  protected final int delayFactor;
  private final int minNumberOfUnusedKafkaTopicsToPreserve;
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private boolean isLeaderControllerOfControllerCluster = false;
  private long refreshQueueCycle = Time.MS_PER_MINUTE;

  private PubSubTopicRepository pubSubTopicRepository;
  protected final VeniceControllerMultiClusterConfig multiClusterConfigs;

  public TopicCleanupService(
      Admin admin,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      PubSubTopicRepository pubSubTopicRepository) {
    this.admin = admin;
    this.sleepIntervalBetweenTopicListFetchMs =
        multiClusterConfigs.getTopicCleanupSleepIntervalBetweenTopicListFetchMs();
    this.delayFactor = multiClusterConfigs.getTopicCleanupDelayFactor();
    this.minNumberOfUnusedKafkaTopicsToPreserve = multiClusterConfigs.getMinNumberOfUnusedKafkaTopicsToPreserve();
    this.cleanupThread = new Thread(new TopicCleanupTask(), "TopicCleanupTask");
    this.multiClusterConfigs = multiClusterConfigs;
    this.pubSubTopicRepository = pubSubTopicRepository;
  }

  @Override
  public boolean startInner() throws Exception {
    cleanupThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stop.set(true);
    cleanupThread.interrupt();
  }

  TopicManager getTopicManager() {
    return admin.getTopicManager();
  }

  TopicManager getTopicManager(String kafkaBootstrapServerAddress) {
    return admin.getTopicManager(kafkaBootstrapServerAddress);
  }

  private class TopicCleanupTask implements Runnable {
    @Override
    public void run() {
      while (!stop.get()) {
        try {
          Thread.sleep(sleepIntervalBetweenTopicListFetchMs);
        } catch (InterruptedException e) {
          LOGGER.error("Received InterruptedException during sleep in TopicCleanup thread");
          break;
        }
        if (stop.get()) {
          break;
        }
        try {
          if (admin.isLeaderControllerOfControllerCluster()) {
            if (!isLeaderControllerOfControllerCluster) {
              /**
               * Sleep for some time when current controller firstly becomes leader controller of controller cluster.
               * This is trying to avoid concurrent Kafka topic deletion sent by both previous leader controller
               * (Kafka topic cleanup doesn't finish in a short period) and the new leader controller.
               */
              isLeaderControllerOfControllerCluster = true;
              LOGGER.info("Current controller becomes the leader controller of controller cluster");
              continue;
            }
            cleanupVeniceTopics();
          } else {
            isLeaderControllerOfControllerCluster = false;
          }
        } catch (Exception e) {
          LOGGER.error("Received exception when cleaning up topics", e);
        }
      }
      LOGGER.info("TopicCleanupTask stopped");
    }
  }

  Admin getAdmin() {
    return admin;
  }

  /**
   * The following will delete topics based on their priority. Real-time topics are given higher priority than version topics.
   * If version topic deletion takes more than certain time it refreshes the entire topic list and start deleting from RT topics again.
    */
  void cleanupVeniceTopics() {
    PriorityQueue<PubSubTopic> allTopics = new PriorityQueue<>((s1, s2) -> s1.isRealTime() ? -1 : 0);
    populateDeprecatedTopicQueue(allTopics);
    long refreshTime = System.currentTimeMillis();
    while (!allTopics.isEmpty()) {
      PubSubTopic topic = allTopics.poll();
      /**
       * Until now, we haven't figured out a good way to handle real-time topic cleanup:
       *     1. If {@link TopicCleanupService} doesn't delete real-time topic, the truncated real-time topic could cause inconsistent data problem
       *       between parent cluster and prod cluster if the deleted hybrid store gets re-created;
       *     2. If {@link TopicCleanupService} deletes the real-time topic, it might crash MM if application is still producing to the real-time topic
       *       in parent cluster;
       *
       *     Since Kafka nurse script will automatically kick in if MM crashes (which should still happen very infrequently),
       *     for the time being, we choose to delete the real-time topic.
       */
      try {
        try {
          // Best effort to clean up staled replica statuses from meta system store.
          cleanupReplicaStatusesFromMetaSystemStore(topic);
        } catch (Exception e) {
          LOGGER.error(
              "Received exception: {} while trying to clean up replica statuses from meta system store for topic: {}, but topic deletion will continue",
              e,
              topic);
        }
        getTopicManager().ensureTopicIsDeletedAndBlockWithRetry(topic);
      } catch (ExecutionException e) {
        LOGGER.warn("ExecutionException caught when trying to delete topic: {}", topic);
        // No op, will try again in the next cleanup cycle.
      }

      if (!topic.isRealTime()) {
        // If Version topic deletion took long time, skip further VT deletion and check if we have new RT topic to
        // delete
        if (System.currentTimeMillis() - refreshTime > refreshQueueCycle) {
          allTopics.clear();
          populateDeprecatedTopicQueue(allTopics);
          if (allTopics.isEmpty()) {
            break;
          }
          refreshTime = System.currentTimeMillis();
        }
      }
    }
  }

  private void populateDeprecatedTopicQueue(PriorityQueue<PubSubTopic> topics) {
    Map<String, Map<PubSubTopic, Long>> allStoreTopics = getAllVeniceStoreTopicsRetentions(getTopicManager());
    allStoreTopics.forEach((storeName, topicRetentions) -> {
      PubSubTopic realTimeTopic = pubSubTopicRepository.getTopic(Version.composeRealTimeTopic(storeName));
      if (topicRetentions.containsKey(realTimeTopic)) {
        if (admin.isTopicTruncatedBasedOnRetention(topicRetentions.get(realTimeTopic))) {
          topics.offer(realTimeTopic);
        }
        topicRetentions.remove(realTimeTopic);
      }
      List<PubSubTopic> oldTopicsToDelete =
          extractVersionTopicsToCleanup(admin, topicRetentions, minNumberOfUnusedKafkaTopicsToPreserve, delayFactor);
      if (!oldTopicsToDelete.isEmpty()) {
        topics.addAll(oldTopicsToDelete);
      }
    });
  }

  /**
   * @return a map object that maps from the store name to the Kafka topic name and its configured Kafka retention time.
   */
  public static Map<String, Map<PubSubTopic, Long>> getAllVeniceStoreTopicsRetentions(TopicManager topicManager) {
    Map<PubSubTopic, Long> topicsWithRetention = topicManager.getAllTopicRetentions();
    Map<String, Map<PubSubTopic, Long>> allStoreTopics = new HashMap<>();

    for (Map.Entry<PubSubTopic, Long> entry: topicsWithRetention.entrySet()) {
      PubSubTopic topic = entry.getKey();
      long retention = entry.getValue();
      String storeName = topic.getStoreName();
      if (storeName.isEmpty()) {
        // TODO: check whether Venice needs to cleanup topics not belonging to Venice.
        continue;
      }
      allStoreTopics.compute(storeName, (s, topics) -> {
        if (topics == null) {
          topics = new HashMap<>();
        }
        topics.put(topic, retention);
        return topics;
      });
    }
    return allStoreTopics;
  }

  /**
   * Filter Venice version topics so that the returned topics satisfying the following conditions:
   * <ol>
   *  <li> topic is truncated based on retention time. </li>
   *  <li> topic version is in the deletion range. </li>
   *  <li> current controller is the parent controller or Helix resource for this topic is already removed in child controller. </li>
   *  <li> topic is a real time topic;
   *     <p>
   *       or topic is a version topic and passes delay countdown condition. </li>
   * </ol>
   * @return a list that contains topics satisfying all the above conditions.
   */
  public static List<PubSubTopic> extractVersionTopicsToCleanup(
      Admin admin,
      Map<PubSubTopic, Long> topicRetentions,
      int minNumberOfUnusedKafkaTopicsToPreserve,
      int delayFactor) {
    if (topicRetentions.isEmpty()) {
      return Collections.emptyList();
    }
    Set<PubSubTopic> veniceTopics = topicRetentions.keySet();
    Optional<Integer> optionalMaxVersion = veniceTopics.stream()
        .filter(vt -> Version.isVersionTopic(vt.getName()))
        .map(vt -> Version.parseVersionFromKafkaTopicName(vt.getName()))
        .max(Integer::compare);

    if (!optionalMaxVersion.isPresent()) {
      return Collections.emptyList();
    }

    int maxVersion = optionalMaxVersion.get();

    String storeName = veniceTopics.iterator().next().getStoreName();
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    boolean isStoreZkShared = systemStoreType != null && systemStoreType.isStoreZkShared();
    boolean isStoreDeleted = !isStoreZkShared && !admin.getStoreConfigRepo().getStoreConfig(storeName).isPresent();
    // Do not preserve any VT for deleted user stores or zk shared system stores.
    // TODO revisit the behavior if we'd like to support rollback for zk shared system stores.
    final long maxVersionNumberToDelete =
        isStoreDeleted || isStoreZkShared ? maxVersion : maxVersion - minNumberOfUnusedKafkaTopicsToPreserve;

    return veniceTopics.stream()
        /** Consider only truncated topics */
        .filter(t -> admin.isTopicTruncatedBasedOnRetention(topicRetentions.get(t)))
        /** Always preserve the last {@link #minNumberOfUnusedKafkaTopicsToPreserve} topics, whether they are healthy or not */
        .filter(t -> Version.parseVersionFromKafkaTopicName(t.getName()) <= maxVersionNumberToDelete)
        /**
         * Filter out resources, which haven't been fully removed in child fabrics yet. This is only performed in the
         * child fabric because parent fabric don't have storage node helix resources.
         *
         * The reason to filter out still-alive resource is to avoid triggering the non-existing topic issue
         * of Kafka consumer happening in Storage Node.
         */
        .filter(t -> admin.isParent() || !admin.isResourceStillAlive(t.getName()))
        .filter(t -> {
          if (Version.isRealTimeTopic(t.getName())) {
            return true;
          }
          // delay VT topic deletion as there could be a race condition where the resource is already deleted by venice
          // but kafka still holding on to the deleted topic message in producer buffer which might cause infinite hang
          // in kafka.
          int remainingFactor =
              storeToCountdownForDeletion.merge(t.getName(), delayFactor, (oldVal, givenVal) -> oldVal - 1);
          if (remainingFactor > 0) {
            return false;
          }
          storeToCountdownForDeletion.remove(t);
          return true;
        })
        .collect(Collectors.toList());
  }

  /**
   * Clean up staled replica status from meta system store if necessary.
   * @param topic
   * @return whether the staled replica status cleanup happens or not.
   */
  protected boolean cleanupReplicaStatusesFromMetaSystemStore(PubSubTopic topic) {
    if (admin.isParent()) {
      // No op in Parent Controller
      return false;
    }
    if (!topic.isVersionTopic()) {
      // Only applicable to version topic
      return false;
    }

    String storeName = Version.parseStoreFromKafkaTopicName(topic.getName());
    int version = Version.parseVersionFromKafkaTopicName(topic.getName());
    HelixReadOnlyStoreConfigRepository storeConfigRepository = admin.getStoreConfigRepo();
    Optional<StoreConfig> storeConfig = storeConfigRepository.getStoreConfig(storeName);
    // Get cluster name for current store
    if (!storeConfig.isPresent()) {
      throw new VeniceException("Failed to get store config for store: " + storeName);
    }
    /**
     * This logic won't take care of store migration scenarios properly since it will just look at the current cluster
     * when topic deletion happens.
     * But it is fine at this stage since a minor leaking of store replica statuses in meta system store is acceptable.
     * If the size of meta system store becomes unacceptable because of unknown issue, we could always do an empty push
     * to clean it up.
     */
    String clusterName = storeConfig.get().getCluster();
    /**
     * Check whether RT topic for the meta system store exists or not to decide whether we should clean replica statuses from meta System Store or not.
     * Since {@link TopicCleanupService} will be running in the leader Controller of Controller Cluster, so
     * we won't have access to store repository for every Venice cluster, and the existence of RT topic for
     * meta System store is the way to check whether meta System store is enabled or not.
     */
    PubSubTopic rtTopicForMetaSystemStore = pubSubTopicRepository
        .getTopic(Version.composeRealTimeTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName)));
    TopicManager topicManager = getTopicManager();
    if (topicManager.containsTopic(rtTopicForMetaSystemStore)) {
      /**
       * Find out the total number of partition of version topic, and we will use this info to clean up replica statuses for each partition.
       */
      int partitionCount = topicManager.partitionsFor(topic).size();
      MetaStoreWriter metaStoreWriter = admin.getMetaStoreWriter();
      for (int i = 0; i < partitionCount; ++i) {
        metaStoreWriter.deleteStoreReplicaStatus(clusterName, storeName, version, i);
      }
      LOGGER.info(
          "Successfully removed store replica status from meta system store for store: {} , "
              + "version: {} with partition count: {} in cluster: {}",
          storeName,
          version,
          partitionCount,
          clusterName);
      return true;
    }
    return false;
  }
}
