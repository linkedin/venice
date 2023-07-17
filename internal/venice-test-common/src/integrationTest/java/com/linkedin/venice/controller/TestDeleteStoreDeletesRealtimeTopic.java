package com.linkedin.venice.controller;

import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_OPERATION_TIMEOUT_MS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.makeStoreHybrid;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.TimeUnit;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestDeleteStoreDeletesRealtimeTopic {
  private static final Logger LOGGER = LogManager.getLogger(TestDeleteStoreDeletesRealtimeTopic.class);

  private VeniceClusterWrapper venice = null;
  private AvroGenericStoreClient client = null;
  private ControllerClient controllerClient = null;
  private TopicManager topicManager = null;
  private String storeName = null;

  private PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeClass
  public void setUp() {
    venice = ServiceFactory.getVeniceCluster();
    controllerClient =
        ControllerClient.constructClusterControllerClient(venice.getClusterName(), venice.getRandomRouterURL());

    try (TopicManagerRepository topicManagerRepository = IntegrationTestPushUtils.getTopicManagerRepo(
        DEFAULT_KAFKA_OPERATION_TIMEOUT_MS,
        100,
        0l,
        venice.getKafka().getAddress(),
        pubSubTopicRepository)) {
      topicManager = topicManagerRepository.getTopicManager();
    }

    storeName = Utils.getUniqueString("hybrid-store");
    venice.getNewStore(storeName);
    makeStoreHybrid(venice, storeName, 100L, 5L);
    client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()));
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(topicManager);
    Utils.closeQuietlyWithErrorLogged(client);
    Utils.closeQuietlyWithErrorLogged(venice);
    Utils.closeQuietlyWithErrorLogged(controllerClient);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void deletingHybridStoreDeletesRealtimeTopic() {
    TestUtils.assertCommand(controllerClient.emptyPush(storeName, Utils.getUniqueString("push-id"), 1L));

    // write streaming records
    SystemProducer veniceProducer = null;
    try {
      veniceProducer = getSamzaProducer(venice, storeName, Version.PushType.STREAM);
      for (int i = 1; i <= 10; i++) {
        sendStreamingRecord(veniceProducer, storeName, i);
      }
    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
    }

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1, "The empty push has not activated yet...");
    });

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      try {
        assertEquals(client.get("9").get(), new Utf8("stream_9"));
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });

    // verify realtime topic exists
    PubSubTopic rtTopic = pubSubTopicRepository.getTopic(Version.composeRealTimeTopic(storeName));
    Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(rtTopic));

    // disable store
    TestUtils.assertCommand(
        controllerClient
            .updateStore(storeName, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false)));

    // wait on delete store as it blocks on deletion of RT topic
    TestUtils.waitForNonDeterministicCompletion(10, TimeUnit.SECONDS, () -> {
      return !controllerClient.deleteStore(storeName).isError(); // error because store no longer exists
    });

    TestUtils.waitForNonDeterministicCompletion(20, TimeUnit.SECONDS, () -> {
      return controllerClient.getStore(storeName).isError(); // error because store no longer exists
    });

    LOGGER.info("Delete store has completed...");

    // verify realtime topic does not exist
    PubSubTopic realTimeTopicName = pubSubTopicRepository.getTopic(Version.composeRealTimeTopic(storeName));
    try {
      boolean isTruncated = topicManager.isTopicTruncated(realTimeTopicName, 60000);
      Assert.assertTrue(
          isTruncated,
          "Real-time buffer topic should be truncated: " + realTimeTopicName + " but retention is set to: "
              + topicManager.getTopicRetention(realTimeTopicName) + ".");
      LOGGER.info("Confirmed truncation of real-time topic: {}", realTimeTopicName);
    } catch (TopicDoesNotExistException e) {
      LOGGER.info("Caught a TopicDoesNotExistException for real-time topic: {}, which is fine.", realTimeTopicName);
    } catch (Exception e) {
      LOGGER.error(e);
    }
  }
}
