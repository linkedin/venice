package com.linkedin.venice.controller;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.avro.util.Utf8;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


public class TestTopicRequestOnHybridDelete {


  @Test
  public void serverRestartOnHybridStoreKeepsVersionOnline(){
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

    System.out.println(venice.clusterConnectionInformation());

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

    controllerClient.emptyPush(storeName, TestUtils.getUniqueString("push-id"), 1L);
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 2);
    });

    try {
      venice.getNewStore(storeName);
      Assert.fail("Must not be able to create a store that already exists");
    } catch (VeniceException e){
      //expected
    }

    //disable store
    controllerClient.updateStore(storeName, Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(false),
        Optional.of(false), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty());
    //delete store
    controllerClient.deleteStore(storeName);
    TestUtils.waitForNonDeterministicCompletion(10, TimeUnit.SECONDS, () -> {
      return controllerClient.getStore(storeName).isError(); //error because store no longer exists
    });

    //recreate store
    venice.getNewStore(storeName);
    Assert.assertEquals(controllerClient.getStore(storeName).getStore().getVersions().size(), 0);
    makeStoreHybrid(venice, storeName, 100L, 5L);
    controllerClient.emptyPush(storeName, TestUtils.getUniqueString("push-id3"), 1L);

    int expectedCurrentVersion = 3;

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), expectedCurrentVersion);
    });


    //write more streaming records
    veniceProducer = getSamzaProducer(venice);
    for (int i=11; i<=20; i++) {
      sendStreamingRecord(veniceProducer, storeName, i);
    }
    veniceProducer.stop();

    //verify new records appear
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      try {
        assertEquals(client.get("19").get(),new Utf8("stream_19"));
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });

    StoreResponse storeResponseBeforeRestart = controllerClient.getStore(storeName);
    List<Version> beforeRestartVersions = storeResponseBeforeRestart.getStore().getVersions();

    boolean foundCurrent = false;
    for (Version version : beforeRestartVersions){
      if (version.getNumber() == expectedCurrentVersion) {
        Assert.assertEquals(version.getStatus(), VersionStatus.ONLINE);
        foundCurrent = true;
      }
    }
    Assert.assertTrue(foundCurrent, "Store's versions must contain the current version " + expectedCurrentVersion);

    //TODO restart a storage node, and verify version is still online.

    client.close();
    venice.close();
  }

  //TODO this test passes, but the same workflow should be tested in a multi-colo simulation
  @Test
  public void deleteStoreAfterStartedPushAllowsNewPush(){
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster();
    ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), venice.getRandomRouterURL());
    TopicManager topicManager = new TopicManager(venice.getKafka().getZkAddress(), TestUtils.getVeniceConsumerFactory(venice.getKafka().getAddress()));

    String storeName = TestUtils.getUniqueString("hybrid-store");
    venice.getNewStore(storeName);
    makeStoreHybrid(venice, storeName, 100L, 5L);

    //new version, but don't write records
    VersionCreationResponse startedVersion = controllerClient.requestTopicForWrites(storeName, 1L, ControllerApiConstants.PushType.BATCH, TestUtils.getUniqueString("pushId"));
    Assert.assertEquals(controllerClient.queryJobStatus(startedVersion.getKafkaTopic()).getStatus(), ExecutionStatus.STARTED.toString());
    Assert.assertTrue(topicManager.containsTopic(startedVersion.getKafkaTopic()));

    //disable store
    controllerClient.updateStore(storeName, Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(false),
        Optional.of(false), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty());
    //delete versions
    controllerClient.deleteAllVersions(storeName);
    //enable store
    controllerClient.updateStore(storeName, Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(true),
        Optional.of(true), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty());

    controllerClient.emptyPush(storeName, TestUtils.getUniqueString("push-id3"), 1L);

    int expectedCurrentVersion = 2;

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), expectedCurrentVersion);
    });

    venice.close();
  }
}
