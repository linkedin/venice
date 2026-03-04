package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_LINGER_MS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.StatefulVeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.endToEnd.TestChangelogValue;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.util.Utf8;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;


/**
 * Utility class to help with integration tests.
 *
 * N.B.: The visibility of this class and its functions is package-private on purpose.
 */
public class IntegrationTestUtils {
  static final int MAX_ASYNC_START_WAIT_TIME_MS = 30 * Time.MS_PER_SECOND;

  /**
   * N.B.: Visibility is package-private on purpose.
   */
  static VeniceProperties getClusterProps(
      String clusterName,
      String zkAddress,
      String zkBasePath,
      PubSubBrokerWrapper pubSubBrokerWrapper,
      boolean sslToKafka) {
    // TODO: Validate that these configs are all still used.
    // TODO: Centralize default config values in a single place

    VeniceProperties clusterProperties = new PropertyBuilder()

        // Helix-related config
        .put(ZOOKEEPER_ADDRESS, zkAddress + zkBasePath)

        // Kafka-related config
        .put(
            KAFKA_BOOTSTRAP_SERVERS,
            sslToKafka ? pubSubBrokerWrapper.getSSLAddress() : pubSubBrokerWrapper.getAddress())
        .put(KAFKA_LINGER_MS, 0)

        // Other configs
        .put(CLUSTER_NAME, clusterName)
        .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.toString())
        .put(CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL, false)
        .build();

    return clusterProperties;
  }

  static void ensureZkPathExists(String zkAddress, String zkBasePath) {
    ZkClient zkClient = ZkClientFactory.newZkClient(zkAddress);
    if (!zkClient.exists(zkBasePath)) {
      zkClient.create(zkBasePath, null, CreateMode.PERSISTENT);
    }
  }

  /**
   * Participant store should be set up by child controller.
   */
  public static void verifyParticipantMessageStoreSetup(
      VeniceHelixAdmin veniceAdmin,
      String clusterName,
      PubSubTopicRepository pubSubTopicRepository) {
    TopicManager topicManager = veniceAdmin.getTopicManager();
    String participantStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName);
    PubSubTopic participantStoreRt = pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(participantStoreName));
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Store store = veniceAdmin.getStore(clusterName, participantStoreName);
      assertNotNull(store);
      assertEquals(store.getVersions().size(), 1);
    });
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      assertTrue(topicManager.containsTopic(participantStoreRt));
    });
  }

  public static void pollChangeEventsFromChangeCaptureConsumer(
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEvents,
      VeniceChangelogConsumer veniceChangelogConsumer) {
    Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        pollAllMessagesForVeniceChangelogConsumer(veniceChangelogConsumer, 1000);
    for (PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
      String key = pubSubMessage.getKey().toString();
      polledChangeEvents.put(key, pubSubMessage);
    }
  }

  public static void pollChangeEventsFromSpecificStatefulChangeCaptureConsumer(
      Map<String, PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEvents,
      List<PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledMessageList,
      StatefulVeniceChangelogConsumer veniceChangelogConsumer) {
    Collection<PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> pubSubMessages =
        pollAllMessagesForStatefulVeniceChangelogConsumer(veniceChangelogConsumer, 1000);
    for (PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
      String key = pubSubMessage.getKey() == null ? null : pubSubMessage.getKey().toString();
      polledChangeEvents.put(key, pubSubMessage);
    }
    polledMessageList.addAll(pubSubMessages);
  }

  public static void pollChangeEventsFromSpecificChangeCaptureConsumer(
      Map<String, PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEvents,
      List<PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledMessageList,
      VeniceChangelogConsumer veniceChangelogConsumer) {
    Collection<PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> pubSubMessages =
        pollAllMessagesForVeniceChangelogConsumer(veniceChangelogConsumer, 1000);
    for (PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
      String key = pubSubMessage.getKey() == null ? null : pubSubMessage.getKey().toString();
      polledChangeEvents.put(key, pubSubMessage);
    }
    polledMessageList.addAll(pubSubMessages);
  }

  public static int pollAfterImageEventsFromChangeCaptureConsumer(
      Map<String, Utf8> polledChangeEvents,
      VeniceChangelogConsumer veniceChangelogConsumer) {
    int polledMessagesNum = 0;
    Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        pollAllMessagesForVeniceChangelogConsumer(veniceChangelogConsumer, 1000);
    for (PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
      Utf8 afterImageEvent = pubSubMessage.getValue().getCurrentValue();
      String key = pubSubMessage.getKey().toString();
      polledChangeEvents.put(key, afterImageEvent);
      polledMessagesNum++;
    }
    return polledMessagesNum;
  }

  public static Collection<PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> pollAllMessagesForStatefulVeniceChangelogConsumer(
      StatefulVeniceChangelogConsumer<Utf8, TestChangelogValue> viewTopicConsumer,
      int timeoutMs) {
    Collection<PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> pubSubAllMessages =
        new ArrayList<>();
    Collection<PubSubMessage<Utf8, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> pubSubMessages;

    do {
      pubSubMessages = viewTopicConsumer.poll(timeoutMs);
      pubSubAllMessages.addAll(pubSubMessages);
    } while (!pubSubMessages.isEmpty());

    return pubSubAllMessages;
  }

  public static Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pollAllMessagesForVeniceChangelogConsumer(
      VeniceChangelogConsumer<Utf8, Utf8> viewTopicConsumer,
      int timeoutMs) {
    Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubAllMessages = new ArrayList<>();
    Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages;

    do {
      pubSubMessages = viewTopicConsumer.poll(timeoutMs);
      pubSubAllMessages.addAll(pubSubMessages);
    } while (!pubSubMessages.isEmpty());

    return pubSubAllMessages;
  }
}
