package com.linkedin.venice.controller;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.FlakyTestRetryAnalyzer;
import com.linkedin.venice.utils.TestUtils;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


public class TestDeleteStoreDeletesRealtimeTopic {

  @Test(retryAnalyzer = FlakyTestRetryAnalyzer.class)
  public void deletingHybridStoreDeletesRealtimeTopic(){
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster();
    ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), venice.getRandomRouterURL());

    String storeName = TestUtils.getUniqueString("hybrid-store");
    venice.getNewStore(storeName);
    makeStoreHybrid(venice, storeName, 100L, 5L);
    controllerClient.emptyPush(storeName, TestUtils.getUniqueString("push-id"), 1L);

    //write streaming records
    SystemProducer veniceProducer = getSamzaProducer(venice);
    for (int i=1; i<=10; i++) {
      sendStreamingRecord(veniceProducer, storeName, i);
    }
    veniceProducer.stop();

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
    TopicManager topicManager = new TopicManager(venice.getKafka().getZkAddress(), TestUtils.getVeniceConsumerFactory(venice.getKafka().getAddress()));
    Assert.assertTrue(topicManager.containsTopic(Version.composeRealTimeTopic(storeName)));

    //disable store
    controllerClient.updateStore(storeName, Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(false),
        Optional.of(false), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty());
    //delete store
    controllerClient.deleteStore(storeName);
    TestUtils.waitForNonDeterministicCompletion(10, TimeUnit.SECONDS, () -> {
      return controllerClient.getStore(storeName).isError(); //error because store no longer exists
    });

    //verify realtime topic does not exist
    Assert.assertFalse(topicManager.containsTopic(Version.composeRealTimeTopic(storeName)));

    IOUtils.closeQuietly(topicManager);
    client.close();
    venice.close();
  }
}
