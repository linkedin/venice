package com.linkedin.venice.controller;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.kafka.TopicManager.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


public class TestDeleteStoreDeletesRealtimeTopic {

  private static final Logger LOGGER = Logger.getLogger(TestDeleteStoreDeletesRealtimeTopic.class);

  private VeniceClusterWrapper venice = null;
  private AvroGenericStoreClient client = null;
  private ControllerClient controllerClient = null;
  private TopicManager topicManager = null;
  private String storeName = null;

  @BeforeClass
  public void setUp() {
    venice = ServiceFactory.getVeniceCluster();
    controllerClient = new ControllerClient(venice.getClusterName(), venice.getRandomRouterURL());
    topicManager = new TopicManager(DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, TestUtils.getVeniceConsumerFactory(venice.getKafka()));
    storeName = TestUtils.getUniqueString("hybrid-store");
    venice.getNewStore(storeName);
    makeStoreHybrid(venice, storeName, 100L, 5L);
    client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(venice.getRandomRouterURL()));
  }

  @AfterClass
  public void cleanUp() {
    IOUtils.closeQuietly(topicManager);
    IOUtils.closeQuietly(client);
    IOUtils.closeQuietly(venice);
    IOUtils.closeQuietly(controllerClient);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void deletingHybridStoreDeletesRealtimeTopic() {
    TestUtils.assertCommand(controllerClient.emptyPush(storeName, TestUtils.getUniqueString("push-id"), 1L));

    //write streaming records
    SystemProducer veniceProducer = null;
    try {
      veniceProducer = getSamzaProducer(venice, storeName, Version.PushType.STREAM);
      for (int i=1; i<=10; i++) {
        sendStreamingRecord(veniceProducer, storeName, i);
      }
    } finally {
      if (null != veniceProducer) {
        veniceProducer.stop();
      }
    }

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1, "The empty push has not activated yet...");
    });

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      try {
        assertEquals(client.get("9").get(),new Utf8("stream_9"));
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });

    //verify realtime topic exists
    Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(Version.composeRealTimeTopic(storeName)));

    //disable store
    TestUtils.assertCommand(controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
        .setEnableReads(false)
        .setEnableWrites(false)));

    // wait on delete store as it blocks on deletion of RT topic
    TestUtils.waitForNonDeterministicCompletion(10, TimeUnit.SECONDS, () -> {
      return !controllerClient.deleteStore(storeName).isError(); //error because store no longer exists
    });

    TestUtils.waitForNonDeterministicCompletion(20, TimeUnit.SECONDS, () -> {
      return controllerClient.getStore(storeName).isError(); //error because store no longer exists
    });

    LOGGER.info("Delete store has completed...");

    //verify realtime topic does not exist
    String realTimeTopicName = Version.composeRealTimeTopic(storeName);
    try {
      boolean isTruncated = topicManager.isTopicTruncated(realTimeTopicName, 60000);
      Assert.assertTrue(isTruncated,"Real-time buffer topic should be truncated: " + realTimeTopicName
          + " but retention is set to: " + topicManager.getTopicRetention(realTimeTopicName) + ".");
      LOGGER.info("Confirmed truncation of real-time topic: " + realTimeTopicName);
    } catch (TopicDoesNotExistException e) {
      LOGGER.info("Caught a TopicDoesNotExistException for real-time topic: " + realTimeTopicName + ", which is fine.");
    } catch (Exception e) {
      LOGGER.error(e);
    }
  }
}
