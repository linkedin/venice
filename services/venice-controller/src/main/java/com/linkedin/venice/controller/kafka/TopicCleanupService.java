package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.stats.TopicCleanupServiceStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicType;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
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
  private final Set<String> childRegions;
  private final Map<String, Map<String, Integer>> multiDataCenterStoreToVersionTopicCount;
  private final PubSubTopicRepository pubSubTopicRepository;
  private final TopicCleanupServiceStats topicCleanupServiceStats;
  private String localDatacenter;
  private boolean isRTTopicDeletionBlocked = false;
  private boolean isLeaderControllerOfControllerCluster = false;
  private long refreshQueueCycle = Time.MS_PER_MINUTE;

  protected final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private PubSubAdminAdapter sourceOfTruthPubSubAdminAdapter;
  private long recentDanglingTopicCleanupTime = -1L;
  private final Map<PubSubTopic, Integer> danglingTopicOccurrenceCounter;
  private final int danglingTopicOccurrenceThresholdForCleanup;
  private final long danglingTopicCleanupIntervalMs;

  public TopicCleanupService(
      Admin admin,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      PubSubTopicRepository pubSubTopicRepository,
      TopicCleanupServiceStats topicCleanupServiceStats,
      PubSubClientsFactory pubSubClientsFactory) {
    this.admin = admin;
    this.sleepIntervalBetweenTopicListFetchMs =
        multiClusterConfigs.getTopicCleanupSleepIntervalBetweenTopicListFetchMs();
    this.delayFactor = multiClusterConfigs.getTopicCleanupDelayFactor();
    this.minNumberOfUnusedKafkaTopicsToPreserve = multiClusterConfigs.getMinNumberOfUnusedKafkaTopicsToPreserve();
    this.cleanupThread = new Thread(new TopicCleanupTask(), "TopicCleanupTask");
    this.multiClusterConfigs = multiClusterConfigs;
    this.pubSubTopicRepository = pubSubTopicRepository;
    this.topicCleanupServiceStats = topicCleanupServiceStats;
    this.childRegions = multiClusterConfigs.getCommonConfig().getChildDatacenters();
    if (!admin.isParent()) {
      // Only perform cross fabric VT check for RT deletion in child fabrics.
      this.multiDataCenterStoreToVersionTopicCount = new HashMap<>(childRegions.size());
      for (String datacenter: childRegions) {
        multiDataCenterStoreToVersionTopicCount.put(datacenter, new HashMap<>());
      }
    } else {
      this.multiDataCenterStoreToVersionTopicCount = Collections.emptyMap();
    }

    this.danglingTopicCleanupIntervalMs =
        Time.MS_PER_SECOND * multiClusterConfigs.getDanglingTopicCleanupIntervalSeconds();

    PubSubAdminAdapterFactory sourceOfTruthAdminAdapterFactory =
        multiClusterConfigs.getSourceOfTruthAdminAdapterFactory();
    PubSubAdminAdapterFactory pubSubAdminAdapterFactory = pubSubClientsFactory.getAdminAdapterFactory();
    this.danglingTopicOccurrenceCounter = new HashMap<>();
    this.danglingTopicOccurrenceThresholdForCleanup =
        multiClusterConfigs.getDanglingTopicOccurrenceThresholdForCleanup();
    if (!sourceOfTruthAdminAdapterFactory.getClass().equals(pubSubAdminAdapterFactory.getClass())
        && danglingTopicCleanupIntervalMs > 0) {
      this.sourceOfTruthPubSubAdminAdapter = constructSourceOfTruthPubSubAdminAdapter(sourceOfTruthAdminAdapterFactory);
    } else {
      this.sourceOfTruthPubSubAdminAdapter = null;
    }
  }

  private PubSubAdminAdapter constructSourceOfTruthPubSubAdminAdapter(
      PubSubAdminAdapterFactory sourceOfTruthAdminAdapterFactory) {
    VeniceProperties veniceProperties = admin.getPubSubSSLProperties(getTopicManager().getPubSubClusterAddress());
    return sourceOfTruthAdminAdapterFactory.create(veniceProperties, pubSubTopicRepository);
  }

  // For test purpose
  void setSourceOfTruthPubSubAdminAdapter(PubSubAdminAdapter pubSubAdminAdapter) {
    this.sourceOfTruthPubSubAdminAdapter = pubSubAdminAdapter;
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

          if (stop.get()) {
            break;
          }
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
          if (ExceptionUtils.recursiveClassEquals(e, InterruptedException.class)) {
            LOGGER.info("Received InterruptedException in TopicCleanupTask. Will stop.");
            break;
          }
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
    topicCleanupServiceStats.recordDeletableTopicsCount(allTopics.size());
    long refreshTime = System.currentTimeMillis();
    while (!allTopics.isEmpty()) {
      PubSubTopic topic = allTopics.poll();
      try {
        if (topic.isRealTime() && !multiDataCenterStoreToVersionTopicCount.isEmpty()) {
          // Only delete realtime topic in child fabrics if all version topics are deleted in all child fabrics.
          if (isRTTopicDeletionBlocked) {
            LOGGER.warn(
                "Topic deletion for topic: {} is blocked due to unable to fetch version topic info",
                topic.getName());
            topicCleanupServiceStats.recordTopicDeletionError();
            continue;
          }
          boolean canDelete = true;
          for (Map.Entry<String, Map<String, Integer>> mapEntry: multiDataCenterStoreToVersionTopicCount.entrySet()) {
            if (mapEntry.getValue().containsKey(topic.getStoreName())) {
              canDelete = false;
              LOGGER.info(
                  "Topic deletion for topic: {} is delayed due to {} version topics found in datacenter {}",
                  topic.getName(),
                  mapEntry.getValue().get(topic.getStoreName()),
                  mapEntry.getKey());
              break;
            }
          }
          if (!canDelete) {
            continue;
          }
        }
        getTopicManager().ensureTopicIsDeletedAndBlockWithRetry(topic);
        topicCleanupServiceStats.recordTopicDeleted();
      } catch (VeniceException e) {
        LOGGER.warn("Caught exception when trying to delete topic: {} - {}", topic.getName(), e.toString());
        topicCleanupServiceStats.recordTopicDeletionError();
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
    Map<PubSubTopic, Long> topicsWithRetention = getTopicManager().getAllTopicRetentions();
    Map<String, Map<PubSubTopic, Long>> allStoreTopics = getAllVeniceStoreTopicsRetentions(topicsWithRetention);
    AtomicBoolean realTimeTopicDeletionNeeded = new AtomicBoolean(false);
    allStoreTopics.forEach((storeName, topicRetentions) -> {
      int minNumOfUnusedVersionTopicsOverride = minNumberOfUnusedKafkaTopicsToPreserve;
      PubSubTopic realTimeTopic = pubSubTopicRepository.getTopic(Version.composeRealTimeTopic(storeName));
      if (topicRetentions.containsKey(realTimeTopic)) {
        if (admin.isTopicTruncatedBasedOnRetention(topicRetentions.get(realTimeTopic))) {
          topics.offer(realTimeTopic);
          minNumOfUnusedVersionTopicsOverride = 0;
          realTimeTopicDeletionNeeded.set(true);
        }
        topicRetentions.remove(realTimeTopic);
      }
      List<PubSubTopic> oldTopicsToDelete =
          extractVersionTopicsToCleanup(admin, topicRetentions, minNumOfUnusedVersionTopicsOverride, delayFactor);
      if (!oldTopicsToDelete.isEmpty()) {
        topics.addAll(oldTopicsToDelete);
      }
    });
    if (realTimeTopicDeletionNeeded.get() && !multiDataCenterStoreToVersionTopicCount.isEmpty()) {
      refreshMultiDataCenterStoreToVersionTopicCountMap(topicsWithRetention.keySet());
    }

    // Check if there are dangling topics to be deleted.
    if (sourceOfTruthPubSubAdminAdapter != null
        && System.currentTimeMillis() - danglingTopicCleanupIntervalMs > recentDanglingTopicCleanupTime) {
      List<PubSubTopic> pubSubTopics = collectDanglingTopics(topicsWithRetention);
      if (!pubSubTopics.isEmpty()) {
        LOGGER.info("Find dangling topics: {} not present among all pubsub systems.", pubSubTopics);
      }
      recentDanglingTopicCleanupTime = System.currentTimeMillis();
      topics.addAll(pubSubTopics);
    }
  }

  private void refreshMultiDataCenterStoreToVersionTopicCountMap(Set<PubSubTopic> localTopics) {
    if (localDatacenter == null) {
      String localPubSubBootstrapServer = getTopicManager().getPubSubClusterAddress();
      for (String childFabric: childRegions) {
        if (localPubSubBootstrapServer.equals(multiClusterConfigs.getChildDataCenterKafkaUrlMap().get(childFabric))) {
          localDatacenter = childFabric;
          break;
        }
      }
      if (localDatacenter == null) {
        String childFabrics = String.join(",", childRegions);
        LOGGER.error(
            "Blocking RT topic deletion. Cannot find local datacenter in child datacenter list: {}",
            childFabrics);
        isRTTopicDeletionBlocked = true;
        return;
      }
    }
    clearAndPopulateStoreToVersionTopicCountMap(
        localTopics,
        multiDataCenterStoreToVersionTopicCount.get(localDatacenter));
    if (childRegions.size() > 1) {
      for (String childFabric: childRegions) {
        try {
          if (childFabric.equals(localDatacenter)) {
            continue;
          }
          String pubSubBootstrapServer = multiClusterConfigs.getChildDataCenterKafkaUrlMap().get(childFabric);
          Set<PubSubTopic> remoteTopics = getTopicManager(pubSubBootstrapServer).listTopics();
          clearAndPopulateStoreToVersionTopicCountMap(
              remoteTopics,
              multiDataCenterStoreToVersionTopicCount.get(childFabric));
        } catch (Exception e) {
          LOGGER.error("Failed to refresh store to version topic count map for fabric {}", childFabric, e);
          isRTTopicDeletionBlocked = true;
          return;
        }
      }
    }
    isRTTopicDeletionBlocked = false;
  }

  private static void clearAndPopulateStoreToVersionTopicCountMap(
      Set<PubSubTopic> topics,
      Map<String, Integer> storeToVersionTopicCountMap) {
    storeToVersionTopicCountMap.clear();
    for (PubSubTopic topic: topics) {
      String storeName = topic.getStoreName();
      if (!storeName.isEmpty() && topic.isVersionTopic()) {
        storeToVersionTopicCountMap.merge(storeName, 1, Integer::sum);
      }
    }
  }

  /**
   * @return a map object that maps from the store name to the Kafka topic name and its configured Kafka retention time.
   */
  public static Map<String, Map<PubSubTopic, Long>> getAllVeniceStoreTopicsRetentions(
      Map<PubSubTopic, Long> topicsWithRetention) {
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
        .filter(t -> admin.isTopicTruncatedBasedOnRetention(t.getName(), topicRetentions.get(t)))
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

  private List<PubSubTopic> collectDanglingTopics(Map<PubSubTopic, Long> pubSubTopicsRetentions) {
    List<PubSubTopic> topicsToCleanup = new ArrayList<>();
    Set<PubSubTopic> kafkaTopics = sourceOfTruthPubSubAdminAdapter.listAllTopics();
    for (Map.Entry<PubSubTopic, Long> entry: pubSubTopicsRetentions.entrySet()) {
      PubSubTopic pubSubTopic = entry.getKey();
      if (!kafkaTopics.contains(pubSubTopic)) {
        try {
          String storeName = pubSubTopic.getStoreName();
          if (PubSubTopicType.isAdminTopic(pubSubTopic.getName())) {
            LOGGER.error("Skip dangling admin topic {}.", pubSubTopic);
            continue;
          }
          String clusterDiscovered = admin.discoverCluster(storeName).getFirst();
          Store store = admin.getStore(clusterDiscovered, storeName);
          LOGGER.warn("Find topic discrepancy case: {}", pubSubTopic);
          if (!isStillValidRealtimeTopic(pubSubTopic, store) || !isStillValidVersionTopic(pubSubTopic, store)) {
            if (checkIfDanglingTopicConsistentlyFound(pubSubTopic)) {
              LOGGER.warn("Will remove consistently found dangling topic {}.", pubSubTopic);
              topicsToCleanup.add(pubSubTopic);
              danglingTopicOccurrenceCounter.remove(pubSubTopic);
            }
          }
        } catch (Exception e) {
          if (e instanceof VeniceNoStoreException) {
            if (checkIfDanglingTopicConsistentlyFound(pubSubTopic)) {
              LOGGER.warn("No store is found for topic: {}", pubSubTopic);
              topicsToCleanup.add(pubSubTopic);
            }
          } else {
            LOGGER.error("Error happened during checking dangling topic: {}", pubSubTopic, e);
          }
        }
      }
    }
    return topicsToCleanup;
  }

  private boolean isStillValidRealtimeTopic(PubSubTopic pubSubTopic, Store store) {
    if (pubSubTopic.isRealTime() && !store.isHybrid()) {
      for (Version version: store.getVersions()) {
        if (version.getHybridStoreConfig() != null) {
          break;
        }
      }
      return false;
    }
    return true;
  }

  private boolean isStillValidVersionTopic(PubSubTopic pubSubTopic, Store store) {
    if (pubSubTopic.isVersionTopicOrStreamReprocessingTopic() || pubSubTopic.isViewTopic()) {
      int versionNum = Version.parseVersionFromKafkaTopicName(pubSubTopic.getName());
      if (!store.containsVersion(versionNum)) {
        return false;
      }
    }
    return true;
  }

  private boolean checkIfDanglingTopicConsistentlyFound(PubSubTopic pubSubTopic) {
    danglingTopicOccurrenceCounter.compute(pubSubTopic, (key, val) -> (val == null) ? 1 : val + 1);
    if (danglingTopicOccurrenceCounter.get(pubSubTopic) >= danglingTopicOccurrenceThresholdForCleanup) {
      return true;
    }
    return false;
  }

}
