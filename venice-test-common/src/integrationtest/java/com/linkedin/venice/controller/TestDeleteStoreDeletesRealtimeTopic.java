package com.linkedin.venice.controller;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.TrackableControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.FlakyTestRetryAnalyzer;
import com.linkedin.venice.utils.TestUtils;
import java.util.concurrent.TimeUnit;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
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

  @AfterMethod
  public void cleanUp() {
    IOUtils.closeQuietly(topicManager);
    IOUtils.closeQuietly(client);
    IOUtils.closeQuietly(venice);
    IOUtils.closeQuietly(controllerClient);
  }

  @Test
  public void deletingHybridStoreDeletesRealtimeTopic(){
    venice = ServiceFactory.getVeniceCluster();
    controllerClient = new ControllerClient(venice.getClusterName(), venice.getRandomRouterURL());

    String storeName = TestUtils.getUniqueString("hybrid-store");
    venice.getNewStore(storeName);
    makeStoreHybrid(venice, storeName, 100L, 5L);
    controllerClient.emptyPush(storeName, TestUtils.getUniqueString("push-id"), 1L);

    //write streaming records
    SystemProducer veniceProducer = getSamzaProducer(venice, storeName, ControllerApiConstants.PushType.STREAM);
    for (int i=1; i<=10; i++) {
      sendStreamingRecord(veniceProducer, storeName, i);
    }
    veniceProducer.stop();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1, "The empty push has not activated yet...");
    });

    AvroGenericStoreClient client =
        ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName)
                .setVeniceURL(venice.getRandomRouterURL()));

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      try {
        assertEquals(client.get("9").get(),new Utf8("stream_9"));
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });

    //verify realtime topic exists
    topicManager = new TopicManager(venice.getKafka().getZkAddress(), DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS,
        DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, TestUtils.getVeniceConsumerFactory(venice.getKafka().getAddress()));
    Assert.assertTrue(topicManager.containsTopic(Version.composeRealTimeTopic(storeName)));

    //disable store
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
        .setEnableReads(false)
        .setEnableWrites(false));

    //delete store
    TrackableControllerResponse response = controllerClient.deleteStore(storeName);
    Assert.assertFalse(response.isError(), "Received an error on the delete store command: " + response.getError() + ".");
    TestUtils.waitForNonDeterministicCompletion(10, TimeUnit.SECONDS, () -> {
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
