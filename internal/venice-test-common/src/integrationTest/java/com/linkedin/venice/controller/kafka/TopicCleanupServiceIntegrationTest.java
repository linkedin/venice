package com.linkedin.venice.controller.kafka;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.pubsub.PubSubConstants.DEFAULT_TOPIC_RETENTION_POLICY_MS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.HelixVeniceClusterResources;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.stats.TopicCleanupServiceStats;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class TopicCleanupServiceIntegrationTest {
  @Test(timeOut = 180_000)
  public void testOrphanRealTimeTopicIsMarkedBeforeCleanupDeletion() {
    Properties controllerProperties = new Properties();
    controllerProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, "false");
    controllerProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, "false");
    // Drive cleanup synchronously below rather than racing the controller's background cleanup thread.
    controllerProperties.setProperty(
        TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS,
        Long.toString(TimeUnit.DAYS.toMillis(1)));
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(0)
        .numberOfRouters(0)
        .replicationFactor(1)
        .extraProperties(controllerProperties)
        .build();

    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options)) {
      VeniceHelixAdmin childAdmin = cluster.getLeaderVeniceController().getVeniceHelixAdmin();
      assertFalse(childAdmin.isParent());
      String clusterName = cluster.getClusterName();
      String storeName = Utils.getUniqueString("orphan-rt-retention");
      childAdmin.createStore(clusterName, storeName, "test-owner", "\"string\"", "\"string\"");
      HelixVeniceClusterResources resources = childAdmin.getHelixVeniceClusterResources(clusterName);
      assertNotNull(resources.getStoreMetadataRepository().getStore(storeName));
      assertNotNull(resources.getStoreConfigAccessor().getStoreConfig(storeName));
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> assertTrue(childAdmin.getStoreConfigRepo().getStoreConfig(storeName).isPresent()));
      assertEquals(childAdmin.discoverCluster(storeName), clusterName);

      PubSubTopicRepository topics = cluster.getPubSubTopicRepository();
      PubSubTopic realTimeTopic = topics.getTopic(Utils.composeRealTimeTopic(storeName));
      TopicManager topicManager = childAdmin.getTopicManager();
      topicManager.createTopic(realTimeTopic, 1, 1, false);
      assertTrue(topicManager.containsTopic(realTimeTopic));
      long ordinaryRetentionMs = topicManager.getTopicRetention(realTimeTopic);
      assertEquals(ordinaryRetentionMs, DEFAULT_TOPIC_RETENTION_POLICY_MS);
      assertFalse(childAdmin.isTopicTruncatedBasedOnRetention(realTimeTopic.getName(), ordinaryRetentionMs));

      TopicCleanupService cleanup = new TopicCleanupService(
          childAdmin,
          childAdmin.getMultiClusterConfigs(),
          topics,
          new TopicCleanupServiceStats(new MetricsRepository()),
          cluster.getPubSubBrokerWrapper().getPubSubClientsFactory());
      cleanup.cleanupVeniceTopics(topicManager);
      assertTrue(
          topicManager.containsTopic(realTimeTopic),
          "An existing store's normal-retention RT must be preserved");
      assertEquals(topicManager.getTopicRetention(realTimeTopic), ordinaryRetentionMs);

      resources.getStoreConfigAccessor().deleteConfig(storeName);
      assertNotNull(resources.getStoreMetadataRepository().getStore(storeName));
      assertNull(resources.getStoreConfigAccessor().getStoreConfig(storeName));
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        assertFalse(childAdmin.getStoreConfigRepo().getStoreConfig(storeName).isPresent());
        expectThrows(VeniceNoStoreException.class, () -> childAdmin.discoverCluster(storeName));
      });
      cleanup.cleanupVeniceTopics(topicManager);
      assertTrue(topicManager.containsTopic(realTimeTopic), "Store metadata alone must protect a normal-retention RT");
      assertEquals(topicManager.getTopicRetention(realTimeTopic), ordinaryRetentionMs);

      resources.getStoreConfigAccessor().createConfig(storeName, clusterName);
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> assertTrue(childAdmin.getStoreConfigRepo().getStoreConfig(storeName).isPresent()));
      assertEquals(childAdmin.discoverCluster(storeName), clusterName);
      // Bypass normal deletion so the RT retains its ordinary retention throughout the metadata transitions.
      resources.getStoreMetadataRepository().deleteStore(storeName);
      assertNull(resources.getStoreMetadataRepository().getStore(storeName));
      assertNotNull(resources.getStoreConfigAccessor().getStoreConfig(storeName));
      cleanup.cleanupVeniceTopics(topicManager);
      assertTrue(topicManager.containsTopic(realTimeTopic), "StoreConfig alone must protect a normal-retention RT");
      assertEquals(topicManager.getTopicRetention(realTimeTopic), ordinaryRetentionMs);

      resources.getStoreConfigAccessor().deleteConfig(storeName);
      assertNull(resources.getStoreConfigAccessor().getStoreConfig(storeName));
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        assertFalse(childAdmin.getStoreConfigRepo().getStoreConfig(storeName).isPresent());
        expectThrows(VeniceNoStoreException.class, () -> childAdmin.discoverCluster(storeName));
      });
      assertEquals(topicManager.getTopicRetention(realTimeTopic), ordinaryRetentionMs);

      cleanup.cleanupVeniceTopics(topicManager);
      assertTrue(
          topicManager.containsTopic(realTimeTopic),
          "Orphan detection must mark retention without deleting the RT");
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> assertEquals(
              topicManager.getTopicRetention(realTimeTopic),
              childAdmin.getMultiClusterConfigs().getDeprecatedRealTimeTopicRetentionMs()));
      assertTrue(
          childAdmin.isTopicTruncatedBasedOnRetention(
              realTimeTopic.getName(),
              topicManager.getTopicRetention(realTimeTopic)));

      cleanup.cleanupVeniceTopics(topicManager);
      assertFalse(topicManager.containsTopic(realTimeTopic), "A later cleanup scan must delete the marked orphan RT");
    }
  }
}
